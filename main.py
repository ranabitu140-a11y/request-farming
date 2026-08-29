import os
import random
import string
import logging
import asyncio
from dotenv import load_dotenv
from keep_alive import keep_alive
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ChatJoinRequestHandler, ContextTypes, filters, CallbackQueryHandler
)
from telegram.request import HTTPXRequest

load_dotenv()

# ==========================================
# CONFIG
# ==========================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")
DB_NAME = os.environ.get("DB_NAME", "dynamic_force_db")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
DELETE_TIMER = int(os.environ.get("DELETE_TIMER", "30")) # in minutes

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# Silence spammy HTTP request logs from python-telegram-bot
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# ==========================================
# DATABASE (Motor - Async MongoDB)
# ==========================================
mongo_client = AsyncIOMotorClient(MONGO_URI, maxPoolSize=100)
db = mongo_client[DB_NAME]
channels_pool = db["channels_pool"]
files_col = db["files"]
users_col = db["users"]
pending_col = db["pending_requests"]
admins_col = db["admins"]
settings_col = db["settings"]
source_channels_col = db["source_channels"]
db_channels_col = db["db_channels"]
captions_col = db["captions"]

async def get_delete_timer():
    doc = await settings_col.find_one({"_id": "config"})
    return doc.get("delete_timer", 30) if doc else 30

async def set_delete_timer(minutes: int):
    await settings_col.update_one({"_id": "config"}, {"$set": {"delete_timer": minutes}}, upsert=True)

async def get_protect_content():
    doc = await settings_col.find_one({"_id": "config"})
    return doc.get("protect_content", True) if doc else True

async def set_protect_content(status: bool):
    await settings_col.update_one({"_id": "config"}, {"$set": {"protect_content": status}}, upsert=True)


def generate_payload(length=32):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


async def save_file(file_id, file_type):
    payload = generate_payload()
    await files_col.insert_one({"payload_string": payload, "files": [{"file_id": file_id, "type": file_type}]})
    return payload


async def save_batch(files_list):
    payload = generate_payload()
    await files_col.insert_one({"payload_string": payload, "files": files_list})
    return payload


async def get_file(payload):
    doc = await files_col.find_one({"payload_string": payload})
    return doc.get("files", []) if doc else None


async def add_admin(user_id):
    await admins_col.update_one({"user_id": user_id}, {"$set": {"user_id": user_id}}, upsert=True)


async def is_admin(user_id):
    return bool(await admins_col.find_one({"user_id": user_id}))


async def add_channel_db(channel_id, invite_link):
    await channels_pool.update_one({"channel_id": channel_id}, {"$set": {"invite_link": invite_link}}, upsert=True)


async def get_all_channels():
    return await channels_pool.find({}).to_list(length=None)


async def get_user_channels(user_id):
    user = await users_col.find_one({"user_id": user_id})
    return user.get("requested_channels", []) if user else []


async def add_user_channel(user_id, channel_id):
    await users_col.update_one({"user_id": user_id}, {"$addToSet": {"requested_channels": channel_id}}, upsert=True)


async def set_pending(user_id, payload, message_id, channel_id=None):
    await pending_col.update_one({"user_id": user_id}, {"$set": {"payload_string": payload, "message_id": message_id, "channel_id": channel_id}}, upsert=True)


async def get_pending(user_id):
    doc = await pending_col.find_one({"user_id": user_id})
    return (doc["payload_string"], doc.get("message_id"), doc.get("channel_id")) if doc else (None, None, None)


async def clear_pending(user_id):
    await pending_col.delete_one({"user_id": user_id})


# ==========================================
# In-memory batch state tracker
# ==========================================
batch_state = {}

async def delete_messages_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.data["chat_id"]
    message_ids = job.data["message_ids"]
    
    for msg_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:
            pass # Ignore if message was already deleted or chat not found

async def _deliver_files_background(bot, job_queue, user_id, files_list, protect_status, msg_id_to_delete=None):
    if msg_id_to_delete:
        try:
            await bot.delete_message(chat_id=user_id, message_id=msg_id_to_delete)
        except Exception:
            pass

    sent_messages = []
    for f in files_list:
        try:
            s_msg = None
            if f["type"] == "document":
                s_msg = await bot.send_document(chat_id=user_id, document=f["file_id"], protect_content=protect_status)
            elif f["type"] == "video":
                s_msg = await bot.send_video(chat_id=user_id, video=f["file_id"], protect_content=protect_status)
            elif f["type"] == "photo":
                s_msg = await bot.send_photo(chat_id=user_id, photo=f["file_id"], protect_content=protect_status)
            elif f["type"] == "text":
                s_msg = await bot.send_message(chat_id=user_id, text=f["file_id"], protect_content=protect_status)
            if s_msg:
                sent_messages.append(s_msg.message_id)
        except Exception as e:
            log.error(f"Send error in background: {e}")
            
    if sent_messages:
        timer = await get_delete_timer()
        warning_msg = await bot.send_message(
            chat_id=user_id, 
            text=f"⚠️ *Important:*\n\nAll Messages will be deleted after {timer} minutes. Please save or forward these messages to your personal saved messages to avoid losing them!",
            parse_mode="Markdown"
        )
        sent_messages.append(warning_msg.message_id)
        if job_queue:
            job_queue.run_once(delete_messages_job, timer * 60, data={"chat_id": user_id, "message_ids": sent_messages})
        else:
            log.warning("No JobQueue available. Messages will not be auto-deleted.")

# ==========================================
# HANDLERS
# ==========================================

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command and deep links."""
    user_id = update.effective_user.id
    # log.info(f"/start from {user_id}")

    if not context.args:
        await update.message.reply_text("🔥 I am file store bot")
        return

    payload = context.args[0]

    files_list = await get_file(payload)
    if not files_list:
        await update.message.reply_text("❌ Invalid or expired link.")
        return

    all_channels = await get_all_channels()
    requested = await get_user_channels(user_id)

    # ---- Randomized Channel Selection Algorithm ----
    # Filter out any channel IDs that are already in the user's requested list.
    # This ensures the user is only ever asked to join channels they haven't requested yet.
    available = [c for c in all_channels if c["channel_id"] not in requested]
    log.info(f"User {user_id} requested: {requested}, available: {[c['channel_id'] for c in available]}")

    # Edge Case Bypass: If the user has already requested ALL channels in the pool,
    # skip the force request completely and deliver files directly.
    if not available:
        log.info(f"No available channels for {user_id}. Delivering directly.")
        protect_status = await get_protect_content()
        asyncio.create_task(_deliver_files_background(context.bot, context.job_queue, user_id, files_list, protect_status))
        return

    # Check membership concurrently to avoid sequential delays
    async def check_membership(selected_channel):
        channel_id = selected_channel["channel_id"]
        try:
            member = await context.bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status in ['member', 'creator', 'administrator', 'kicked']:
                return selected_channel, True
        except Exception:
            pass
        return selected_channel, False

    membership_results = await asyncio.gather(*(check_membership(c) for c in available))
    
    actually_available = []
    channels_to_add = []
    for selected_channel, in_chat in membership_results:
        if in_chat:
            channels_to_add.append(selected_channel["channel_id"])
        else:
            actually_available.append(selected_channel)
            
    if channels_to_add:
        await asyncio.gather(*(add_user_channel(user_id, cid) for cid in channels_to_add))
        
    available = actually_available
    
    selected = None
    if available:
        random.shuffle(available)
        for s in available:
            try:
                await context.bot.decline_chat_join_request(chat_id=s["channel_id"], user_id=user_id)
                selected = s
                break
            except Exception as e:
                if "already a participant" in str(e).lower():
                    await add_user_channel(user_id, s["channel_id"])
                else:
                    selected = s
                    break

    # If all channels were skipped because user is already in them
    if not selected:
        log.info(f"No available channels for {user_id} after checking membership. Delivering directly.")
        protect_status = await get_protect_content()
        asyncio.create_task(_deliver_files_background(context.bot, context.job_queue, user_id, files_list, protect_status))
        return

    # Use stored invite link instead of generating a fresh one
    invite_url = selected["invite_link"]

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Join Channel to Access", url=invite_url)]
    ])
    msg = await update.message.reply_text(
        "🔐 To access this file, please send a *join request* to the BACKUP channel below.\n\n"
        "⚠️ Just tap the button and send a join request — your file will arrive automatically!",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await set_pending(user_id, payload, msg.message_id, selected["channel_id"])


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help to admins."""
    if not await is_admin(update.effective_user.id):
        return
    log.info(f"/help from {update.effective_user.id}")
    await update.message.reply_text(
        "**🤖 Bot Commands & Help**\n\n"
        "**Upload Files:**\n"
        "Send any Document or Video to create a single file link.\n\n"
        "**Commands:**\n"
        "`/start` — Start the bot\n"
        "`/batch` — Create a batch link for multiple files\n"
        "`/add_channel <channel_id>` — Add a force-join channel\n"
        "`/channels` — List and remove channels\n"
        "`/set_timer <minutes>` — Set the auto-delete timer\n"
        "`/protect on/off` — Toggle forward protection\n"
        "`/add_admin <id>` — Add admin (Owner)\n"
        "`/rem_admin <id>` — Remove admin (Owner)\n"
        "`/admins` — List admins (Owner)\n"
        "`/broadcast` — Broadcast message (Reply to msg)\n"
        "`/add_source <id>` — Add image source channel\n"
        "`/rem_source <id>` — Remove image source channel\n"
        "`/list_sources` — List source channels\n"
        "`/add_db <id>` — Add DB processing channel\n"
        "`/rem_db <id>` — Remove DB processing channel\n"
        "`/list_db` — List DB channels\n"
        "`/add_caption <text>` — Add custom caption\n"
        "`/rem_caption <id>` — Remove custom caption\n"
        "`/list_captions` — List all custom captions\n"
        "`/run <src_id> <start_id> [end_id]` — Process videos\n"
        "`/help` — Show this message",
        parse_mode="Markdown"
    )


async def batch_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start batch creation."""
    if not await is_admin(update.effective_user.id):
        return
    log.info(f"/batch from {update.effective_user.id}")
    batch_state[update.effective_user.id] = {"state": "WAITING_FIRST_MSG"}
    await update.message.reply_text("📦 Forward the *FIRST* message from your content channel.", parse_mode="Markdown")


async def add_channel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a channel to force-join pool. Bot must be admin in that channel."""
    if not await is_admin(update.effective_user.id):
        return
    log.info(f"/add_channel from {update.effective_user.id}")

    if not context.args:
        await update.message.reply_text("Usage: `/add_channel <channel_id>`\nExample: `/add_channel -1001234567890`", parse_mode="Markdown")
        return

    try:
        channel_id = int(context.args[0])
        # Bot dynamically creates an invite link with join requests enabled
        invite = await context.bot.create_chat_invite_link(
            chat_id=channel_id,
            creates_join_request=True
        )
        await add_channel_db(channel_id, invite.invite_link)
        await update.message.reply_text(
            f"✅ Channel `{channel_id}` added!\n\n🔗 Invite: {invite.invite_link}",
            parse_mode="Markdown"
        )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Error: {e}\n\nMake sure bot is admin with 'Invite Users' rights."
        )


async def set_timer_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the auto-delete timer."""
    if not await is_admin(update.effective_user.id):
        return
        
    if not context.args:
        current = await get_delete_timer()
        await update.message.reply_text(f"Current timer is {current} minutes.\nUsage: `/set_timer <minutes>`", parse_mode="Markdown")
        return
        
    try:
        minutes = int(context.args[0])
        if minutes < 1:
            await update.message.reply_text("❌ Timer must be at least 1 minute.")
            return
        await set_delete_timer(minutes)
        await update.message.reply_text(f"✅ Auto-delete timer set to {minutes} minutes.")
    except ValueError:
        await update.message.reply_text("❌ Please provide a valid number of minutes.")


async def protect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle forward protection."""
    if not await is_admin(update.effective_user.id):
        return
        
    if not context.args:
        current = await get_protect_content()
        status = "ON" if current else "OFF"
        await update.message.reply_text(f"Current Forward Protection is {status}.\nUsage: `/protect on` or `/protect off`", parse_mode="Markdown")
        return
        
    arg = context.args[0].lower()
    if arg == "on":
        await set_protect_content(True)
        await update.message.reply_text("✅ Forward Protection is now ON. Users cannot forward or save the delivered files.")
    elif arg == "off":
        await set_protect_content(False)
        await update.message.reply_text("✅ Forward Protection is now OFF. Users can forward and save the delivered files.")
    else:
        await update.message.reply_text("❌ Invalid argument. Use `on` or `off`.")


async def add_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add an admin (Owner only)."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        return
        
    if not context.args:
        await update.message.reply_text("Usage: `/add_admin <user_id>`", parse_mode="Markdown")
        return
        
    try:
        new_admin = int(context.args[0])
        await admins_col.update_one({"user_id": new_admin}, {"$set": {"user_id": new_admin}}, upsert=True)
        await update.message.reply_text(f"✅ User `{new_admin}` added as admin.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid User ID.")

async def rem_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove an admin (Owner only)."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        return
        
    if not context.args:
        await update.message.reply_text("Usage: `/rem_admin <user_id>`", parse_mode="Markdown")
        return
        
    try:
        old_admin = int(context.args[0])
        await admins_col.delete_one({"user_id": old_admin})
        await update.message.reply_text(f"✅ User `{old_admin}` removed from admins.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid User ID.")

async def list_admins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all admins (Owner only)."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        return
        
    cursor = admins_col.find({})
    admins = await cursor.to_list(length=100)
    
    if not admins:
        await update.message.reply_text("📭 No additional admins.")
        return
        
    msg = "📋 **Admin List:**\n\n"
    for a in admins:
        msg += f"👤 `{a['user_id']}`\n"
        
    await update.message.reply_text(msg, parse_mode="Markdown")

async def _run_broadcast_background(bot, chat_id, msg_to_broadcast, users, status_msg):
    total = len(users)
    success = 0
    failed = 0
    deleted = 0
    
    for i, u in enumerate(users):
        user_id = u["user_id"]
        try:
            if msg_to_broadcast.forward_origin:
                await bot.forward_message(
                    chat_id=user_id,
                    from_chat_id=chat_id,
                    message_id=msg_to_broadcast.message_id
                )
            else:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=chat_id,
                    message_id=msg_to_broadcast.message_id
                )
            success += 1
        except Exception as e:
            if "Forbidden" in str(e) or "bot was blocked" in str(e) or "user is deactivated" in str(e):
                await users_col.delete_one({"user_id": user_id})
                deleted += 1
            failed += 1
            
        # Update progress every 20 users
        if (i + 1) % 20 == 0:
            try:
                await status_msg.edit_text(
                    f"🚀 **Broadcast In Progress...**\n\n"
                    f"🎯 Total Users: {total}\n"
                    f"⏳ Processed: {i + 1}\n"
                    f"⏳ Remaining: {total - (i + 1)}\n"
                    f"✅ Success: {success}\n"
                    f"❌ Failed: {failed}\n"
                    f"🗑️ Removed (Blocked): {deleted}",
                    parse_mode="Markdown"
                )
            except Exception:
                pass  # Ignore rate limits or identical text errors
                
        await asyncio.sleep(0.05)
        
    try:
        await status_msg.edit_text(
            f"📢 **Broadcast Completed!**\n\n"
            f"🎯 Total Users: {total}\n"
            f"✅ Success: {success}\n"
            f"❌ Failed: {failed}\n"
            f"🗑️ Removed (Blocked Bot): {deleted}",
            parse_mode="Markdown"
        )
    except Exception:
        pass


async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast a message to all users (Admins only)."""
    if not await is_admin(update.effective_user.id):
        return
        
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Please reply to the message you want to broadcast.")
        return
        
    msg_to_broadcast = update.message.reply_to_message
    status_msg = await update.message.reply_text("🚀 Starting broadcast... This may take a while.")
    
    cursor = users_col.find({})
    users = await cursor.to_list(length=None)
    
    # Run the actual broadcast in the background so it doesn't block the bot for other users
    asyncio.create_task(_run_broadcast_background(
        bot=context.bot,
        chat_id=update.message.chat_id,
        msg_to_broadcast=msg_to_broadcast,
        users=users,
        status_msg=status_msg
    ))

async def channels_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all channels and provide remove buttons."""
    if not await is_admin(update.effective_user.id):
        return
        
    channels = await get_all_channels()
    if not channels:
        await update.message.reply_text("📭 No channels currently in the pool.")
        return
        
    keyboard = []
    for c in channels:
        channel_id = c["channel_id"]
        keyboard.append([InlineKeyboardButton(f"❌ Remove {channel_id}", callback_data=f"rem_{channel_id}")])
        
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("📋 **Force-Join Channels:**\nTap to remove a channel from the pool.", reply_markup=reply_markup, parse_mode="Markdown")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    if data.startswith("rem_"):
        channel_id_str = data.split("_")[1]
        try:
            channel_id = int(channel_id_str)
            await channels_pool.delete_one({"channel_id": channel_id})
            
            # Refresh list
            channels = await get_all_channels()
            if not channels:
                await query.edit_message_text("📭 No channels left in the pool.")
            else:
                keyboard = []
                for c in channels:
                    cid = c["channel_id"]
                    keyboard.append([InlineKeyboardButton(f"❌ Remove {cid}", callback_data=f"rem_{cid}")])
                await query.edit_message_text("📋 **Force-Join Channels:**\nTap to remove a channel from the pool.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
                
            # Send notification
            await context.bot.send_message(chat_id=query.from_user.id, text=f"✅ Removed channel {channel_id} from the pool.")
        except Exception as e:
            await context.bot.send_message(chat_id=query.from_user.id, text=f"❌ Error removing channel: {e}")


async def handle_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle file uploads and batch forwarding flow."""
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        return

    message = update.message
    log.info(f"Upload/forward from {user_id}, batch_state={batch_state.get(user_id)}")

    # ---- Handle Batch Flow ----
    if user_id in batch_state:
        state = batch_state[user_id]

        if state["state"] == "WAITING_FIRST_MSG":
            if not message.forward_origin or getattr(message.forward_origin, 'type', None) != 'channel':
                await message.reply_text("❌ Please *forward* a message from your channel.", parse_mode="Markdown")
                return
            batch_state[user_id] = {
                "state": "WAITING_LAST_MSG",
                "chat_id": message.forward_origin.chat.id,
                "first_msg_id": message.forward_origin.message_id
            }
            await message.reply_text("✅ Got it! Now forward the *LAST* message from the same channel.", parse_mode="Markdown")
            return

        elif state["state"] == "WAITING_LAST_MSG":
            if not message.forward_origin or getattr(message.forward_origin, 'type', None) != 'channel' or message.forward_origin.chat.id != state["chat_id"]:
                await message.reply_text("❌ Forward the last message from the *same* channel.", parse_mode="Markdown")
                return

            first_msg = state["first_msg_id"]
            last_msg = message.forward_origin.message_id
            chat_id = state["chat_id"]
            del batch_state[user_id]

            if first_msg > last_msg:
                first_msg, last_msg = last_msg, first_msg

            status_msg = await message.reply_text("⏳ Fetching messages from channel...")

            try:
                # Use pyrofork to fetch messages silently without flooding the admin chat
                from pyrogram import Client as PyroClient
                API_ID = int(os.environ.get("API_ID", "0"))
                API_HASH = os.environ.get("API_HASH", "")

                async with PyroClient(
                    "batch_fetcher",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    bot_token=BOT_TOKEN,
                    in_memory=True
                ) as pyro_client:
                    # Force resolve the channel peer to prevent PeerIdInvalid
                    try:
                        from pyrogram.raw.functions.channels import GetChannels
                        from pyrogram.raw.types import InputChannel
                        channel_id_raw = abs(chat_id) - 1000000000000
                        await pyro_client.invoke(GetChannels(id=[InputChannel(channel_id=channel_id_raw, access_hash=0)]))
                    except Exception as e:
                        log.error(f"Failed to resolve channel peer: {e}")

                    msg_ids = list(range(first_msg, last_msg + 1))
                    all_msgs = []
                    for i in range(0, len(msg_ids), 200):
                        chunk = msg_ids[i:i + 200]
                        msgs = await pyro_client.get_messages(chat_id, chunk)
                        if isinstance(msgs, list):
                            all_msgs.extend(msgs)
                        else:
                            all_msgs.append(msgs)

                    file_ids = []
                    for msg in all_msgs:
                        if msg.empty:
                            continue
                        if msg.document:
                            file_ids.append({"file_id": msg.document.file_id, "type": "document"})
                        elif msg.video:
                            file_ids.append({"file_id": msg.video.file_id, "type": "video"})
                        elif msg.photo:
                            file_ids.append({"file_id": msg.photo.file_id, "type": "photo"})
                        elif msg.text:
                            file_ids.append({"file_id": msg.text, "type": "text"})

                if not file_ids:
                    await status_msg.edit_text("❌ No files found in that range.")
                    return

                payload = await save_batch(file_ids)
                bot_me = await context.bot.get_me()
                deep_link = f"https://t.me/{bot_me.username}?start={payload}"
                await status_msg.edit_text(
                    f"✅ Batch created! ({len(file_ids)} files)\n\n🔗 *Link:* {deep_link}",
                    parse_mode="Markdown"
                )
            except Exception as e:
                log.error(f"Batch error: {e}", exc_info=True)
                await status_msg.edit_text(f"❌ Error: {e}")
            return

    # ---- Normal Single File Upload ----
    if message.document:
        file_id = message.document.file_id
        file_type = "document"
    elif message.video:
        file_id = message.video.file_id
        file_type = "video"
    elif message.photo:
        file_id = message.photo[-1].file_id  # Get the highest resolution photo
        file_type = "photo"
    elif message.text:
        bot_me = await context.bot.get_me()
        if f"t.me/{bot_me.username}" in message.text:
            return
        file_id = message.text
        file_type = "text"
    else:
        return

    payload = await save_file(file_id, file_type)
    bot_me = await context.bot.get_me()
    deep_link = f"https://t.me/{bot_me.username}?start={payload}"
    await message.reply_text(f"✅ File saved!\n\n🔗 *Link:* {deep_link}", parse_mode="Markdown")


# ==========================================
# JOIN REQUEST HANDLER (The Delivery)
# ==========================================
async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle join requests — deliver file WITHOUT approving.
    CRUCIAL: We intentionally DO NOT approve the request. It stays pending.
    """
    request = update.chat_join_request
    user_id = request.from_user.id
    channel_id = request.chat.id
    log.info(f"Join request from {user_id} for channel {channel_id}")

    # Record that the user has requested this channel
    await add_user_channel(user_id, channel_id)

    # Check if there's a pending file delivery for this user
    pending_payload, msg_id, pending_channel = await get_pending(user_id)
    log.info(f"Pending payload for {user_id}: {pending_payload}")
    
    if pending_payload:
        files_list = await get_file(pending_payload)
        log.info(f"Files found in DB: {len(files_list) if files_list else 0}")
        if files_list:
            protect_status = await get_protect_content()
            asyncio.create_task(_deliver_files_background(context.bot, context.job_queue, user_id, files_list, protect_status, msg_id))
            
        await clear_pending(user_id)

    # NOTE: We do NOT call request.approve(). The join request stays pending.


async def add_source_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a source channel for image/video extraction."""
    if not await is_admin(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("Usage: `/add_source <channel_id>`", parse_mode="Markdown")
        return
    try:
        channel_id = int(context.args[0])
        await source_channels_col.update_one({"channel_id": channel_id}, {"$set": {"channel_id": channel_id}}, upsert=True)
        await update.message.reply_text(f"✅ Source channel `{channel_id}` added.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid channel ID.")

async def rem_source_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a source channel."""
    if not await is_admin(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("Usage: `/rem_source <channel_id>`", parse_mode="Markdown")
        return
    try:
        channel_id = int(context.args[0])
        await source_channels_col.delete_one({"channel_id": channel_id})
        await update.message.reply_text(f"✅ Source channel `{channel_id}` removed.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid channel ID.")

async def list_sources_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all source channels."""
    if not await is_admin(update.effective_user.id): return
    channels = await source_channels_col.find({}).to_list(length=None)
    if not channels:
        await update.message.reply_text("📭 No source channels.")
        return
    msg = "📋 **Source Channels:**\n\n"
    for c in channels:
        msg += f"🔹 `{c['channel_id']}`\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def add_db_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a DB processing channel."""
    if not await is_admin(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("Usage: `/add_db <channel_id>`", parse_mode="Markdown")
        return
    try:
        channel_id = int(context.args[0])
        await db_channels_col.update_one({"channel_id": channel_id}, {"$set": {"channel_id": channel_id}}, upsert=True)
        await update.message.reply_text(f"✅ DB Processing channel `{channel_id}` added.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid channel ID.")

async def rem_db_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a DB processing channel."""
    if not await is_admin(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("Usage: `/rem_db <channel_id>`", parse_mode="Markdown")
        return
    try:
        channel_id = int(context.args[0])
        await db_channels_col.delete_one({"channel_id": channel_id})
        await update.message.reply_text(f"✅ DB Processing channel `{channel_id}` removed.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ Invalid channel ID.")

async def list_db_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all DB processing channels."""
    if not await is_admin(update.effective_user.id): return
    channels = await db_channels_col.find({}).to_list(length=None)
    if not channels:
        await update.message.reply_text("📭 No DB channels.")
        return
    msg = "📋 **DB Processing Channels:**\n\n"
    for c in channels:
        msg += f"🔹 `{c['channel_id']}`\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def add_caption_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("Usage: `/add_caption <your caption text>`", parse_mode="Markdown")
        return
    text = " ".join(context.args)
    cid = generate_payload(6)
    await captions_col.insert_one({"_id": cid, "text": text})
    await update.message.reply_text(f"✅ Caption added with ID: `{cid}`\n\nPreview:\n{text}", parse_mode="Markdown")

async def rem_caption_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("Usage: `/rem_caption <id>`", parse_mode="Markdown")
        return
    cid = context.args[0]
    res = await captions_col.delete_one({"_id": cid})
    if res.deleted_count > 0:
        await update.message.reply_text(f"✅ Caption `{cid}` removed.", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Caption not found.", parse_mode="Markdown")

async def list_captions_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return
    captions = await captions_col.find({}).to_list(length=None)
    if not captions:
        await update.message.reply_text("📭 No custom captions. Default is used.")
        return
    msg = "📋 **Custom Captions:**\n\n"
    for c in captions:
        msg += f"🔹 **ID:** `{c['_id']}`\n**Text:** {c['text']}\n\n"
    await update.message.reply_text(msg, parse_mode="Markdown")


async def run_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Extract thumbnails from source channel and post to DB channels."""
    if not await is_admin(update.effective_user.id): return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: `/run <source_channel_id> <start_msg_id> [end_msg_id]`", parse_mode="Markdown")
        return
        
    try:
        source_channel_id = int(context.args[0])
        start_msg_id = int(context.args[1])
        end_msg_id = int(context.args[2]) if len(context.args) > 2 else start_msg_id
    except ValueError:
        await update.message.reply_text("❌ Channel ID and Message IDs must be integers.")
        return

    # Warn if source channel is not in db
    src_doc = await source_channels_col.find_one({"channel_id": source_channel_id})
    if not src_doc:
        await update.message.reply_text("⚠️ Warning: Source channel not found in /list_sources, but continuing...")

    db_docs = await db_channels_col.find({}).to_list(length=None)
    if not db_docs:
        await update.message.reply_text("❌ No DB Processing channels set! Add with /add_db")
        return
    db_channels = [d["channel_id"] for d in db_docs]

    status_msg = await update.message.reply_text("⏳ Initializing Pyrogram to fetch messages...")

    try:
        from pyrogram import Client as PyroClient
        API_ID = int(os.environ.get("API_ID", "0"))
        API_HASH = os.environ.get("API_HASH", "")

        async with PyroClient(
            "run_fetcher",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            in_memory=True
        ) as pyro_client:
            try:
                from pyrogram.raw.functions.channels import GetChannels
                from pyrogram.raw.types import InputChannel
                channel_id_raw = abs(source_channel_id) - 1000000000000
                await pyro_client.invoke(GetChannels(id=[InputChannel(channel_id=channel_id_raw, access_hash=0)]))
            except Exception as e:
                log.error(f"Failed to resolve source channel peer: {e}")

            start, end = min(start_msg_id, end_msg_id), max(start_msg_id, end_msg_id)
            msg_ids = list(range(start, end + 1))
            processed_count = 0
            
            for i in range(0, len(msg_ids), 200):
                chunk = msg_ids[i:i + 200]
                msgs = await pyro_client.get_messages(source_channel_id, chunk)
                if not isinstance(msgs, list):
                    msgs = [msgs]
                    
                bot_me = await context.bot.get_me()
                
                for msg in msgs:
                    if msg.empty or not msg.video:
                        continue
                        
                    # Save video for sharing
                    payload = await save_file(msg.video.file_id, "video")
                    deep_link = f"https://t.me/{bot_me.username}?start={payload}"
                    
                    # Get Thumbnail without downloading the full video
                    thumb_bytes = None
                    if msg.video.thumbs:
                        try:
                            # Fetch only the small thumbnail bytes
                            thumb_io = await pyro_client.download_media(msg.video.thumbs[0].file_id, in_memory=True)
                            if thumb_io:
                                thumb_bytes = thumb_io.getvalue()
                        except Exception as e:
                            log.error(f"Error downloading thumbnail: {e}")
                    
                    orig_caption = msg.caption or ''
                    captions = await captions_col.find({}).to_list(length=None)
                    if captions:
                        chosen_text = random.choice(captions)["text"]
                    else:
                        chosen_text = "🎥 **Vip Stuff 🥵🥵🔞**"
                        
                    caption = f"{chosen_text}\n\n🔗 [Click here to Watch/Download]({deep_link})\n\n{orig_caption}"
                    
                    # Post to DB processing channels
                    for db_id in db_channels:
                        try:
                            if thumb_bytes:
                                await context.bot.send_photo(chat_id=db_id, photo=thumb_bytes, caption=caption, parse_mode="Markdown")
                            else:
                                await context.bot.send_message(chat_id=db_id, text=f"No thumbnail available.\n\n{caption}", parse_mode="Markdown")
                        except Exception as e:
                            log.error(f"Failed to send poster to db channel {db_id}: {e}")
                            
                    processed_count += 1
                    
            await status_msg.edit_text(f"✅ Run completed! Processed {processed_count} videos.")
            
    except Exception as e:
        log.error(f"/run error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Error during run: {e}")

async def live_channel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle new videos posted live to source channels."""
    msg = update.channel_post
    if not msg or not msg.video:
        return
        
    channel_id = update.effective_chat.id
    
    # Check if channel is a source channel
    src = await source_channels_col.find_one({"channel_id": channel_id})
    if not src:
        return
        
    db_docs = await db_channels_col.find({}).to_list(length=None)
    if not db_docs:
        return
    db_channels = [d["channel_id"] for d in db_docs]
    
    bot_me = await context.bot.get_me()
    payload = await save_file(msg.video.file_id, "video")
    deep_link = f"https://t.me/{bot_me.username}?start={payload}"
    
    thumb_bytes = None
    if msg.video.thumbnail:
        try:
            # We can download it using PTB! Because this is live, the bot sees the message.
            thumb_file = await context.bot.get_file(msg.video.thumbnail.file_id)
            tb = await thumb_file.download_as_bytearray()
            thumb_bytes = bytes(tb)
        except Exception as e:
            log.error(f"Error downloading live thumbnail: {e}")
            
    # get caption
    captions = await captions_col.find({}).to_list(length=None)
    if captions:
        chosen_text = random.choice(captions)["text"]
    else:
        chosen_text = "🎥 **Vip Stuff 🥵🥵🔞**"
        
    orig_caption = msg.caption or ''
    caption = f"{chosen_text}\n\n🔗 [Click here to Watch/Download]({deep_link})\n\n{orig_caption}"
        
    for db_id in db_channels:
        try:
            if thumb_bytes:
                await context.bot.send_photo(chat_id=db_id, photo=thumb_bytes, caption=caption, parse_mode="Markdown")
            else:
                await context.bot.send_message(chat_id=db_id, text=f"No thumbnail available.\n\n{caption}", parse_mode="Markdown")
        except Exception as e:
            log.error(f"Live broadcast send error to {db_id}: {e}")

# ==========================================
# MAIN
# ==========================================
async def post_init(application: Application):
    """Run after the bot starts."""
    log.info("Creating database indexes for maximum performance...")
    try:
        await files_col.create_index("payload_string", unique=True)
        await users_col.create_index("user_id", unique=True)
        await pending_col.create_index("user_id", unique=True)
        await channels_pool.create_index("channel_id", unique=True)
        await admins_col.create_index("user_id", unique=True)
        await source_channels_col.create_index("channel_id", unique=True)
        await db_channels_col.create_index("channel_id", unique=True)
    except Exception as e:
        log.error(f"Error creating indexes: {e}")

    if OWNER_ID != 0:
        await add_admin(OWNER_ID)
        log.info(f"Owner initialized: {OWNER_ID}")
    me = await application.bot.get_me()
    log.info(f"Bot is running as @{me.username} (ID: {me.id})")


def main():
    log.info("Starting bot...")
    keep_alive()

    req = HTTPXRequest(connection_pool_size=500, read_timeout=30.0)
    app = Application.builder().token(BOT_TOKEN).request(req).post_init(post_init).build()

    # Register handlers
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("batch", batch_cmd))
    app.add_handler(CommandHandler("add_channel", add_channel_cmd))
    app.add_handler(CommandHandler("channels", channels_cmd))
    app.add_handler(CommandHandler("set_timer", set_timer_cmd))
    app.add_handler(CommandHandler("protect", protect_cmd))
    app.add_handler(CommandHandler("add_admin", add_admin_cmd))
    app.add_handler(CommandHandler("rem_admin", rem_admin_cmd))
    app.add_handler(CommandHandler("admins", list_admins_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    app.add_handler(CommandHandler("add_source", add_source_cmd))
    app.add_handler(CommandHandler("rem_source", rem_source_cmd))
    app.add_handler(CommandHandler("list_sources", list_sources_cmd))
    app.add_handler(CommandHandler("add_db", add_db_cmd))
    app.add_handler(CommandHandler("rem_db", rem_db_cmd))
    app.add_handler(CommandHandler("list_db", list_db_cmd))
    app.add_handler(CommandHandler("add_caption", add_caption_cmd))
    app.add_handler(CommandHandler("rem_caption", rem_caption_cmd))
    app.add_handler(CommandHandler("list_captions", list_captions_cmd))
    app.add_handler(CommandHandler("run", run_cmd))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    # Live channel handler for auto processing
    app.add_handler(MessageHandler(
        filters.ChatType.CHANNEL & filters.VIDEO,
        live_channel_handler
    ))
    
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & (filters.Document.ALL | filters.VIDEO | filters.PHOTO | filters.TEXT | filters.FORWARDED) & ~filters.COMMAND,
        handle_upload
    ))
    app.add_handler(ChatJoinRequestHandler(join_request_handler))

    # Run the bot using polling (proven to work with this token)
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
