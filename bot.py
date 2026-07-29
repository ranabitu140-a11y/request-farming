import os
import random
import string
import logging
import asyncio
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ChatJoinRequestHandler, ContextTypes, filters, CallbackQueryHandler
)

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
log = logging.getLogger(__name__)

# ==========================================
# DATABASE (Motor - Async MongoDB)
# ==========================================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]
channels_pool = db["channels_pool"]
files_col = db["files"]
users_col = db["users"]
pending_col = db["pending_requests"]
admins_col = db["admins"]
settings_col = db["settings"]

async def get_delete_timer():
    doc = await settings_col.find_one({"_id": "config"})
    return doc.get("delete_timer", 30) if doc else 30

async def set_delete_timer(minutes: int):
    await settings_col.update_one({"_id": "config"}, {"$set": {"delete_timer": minutes}}, upsert=True)


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


async def set_pending(user_id, payload, message_id):
    await pending_col.update_one({"user_id": user_id}, {"$set": {"payload_string": payload, "message_id": message_id}}, upsert=True)


async def get_pending(user_id):
    doc = await pending_col.find_one({"user_id": user_id})
    return (doc["payload_string"], doc.get("message_id")) if doc else (None, None)


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

# ==========================================
# HANDLERS
# ==========================================

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command and deep links."""
    user_id = update.effective_user.id
    log.info(f"/start from {user_id}")

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

    # Edge Case Bypass: If the user has already requested ALL channels in the pool,
    # skip the force request completely and deliver files directly.
    if not available:
        msg = await update.message.reply_text("⏳ Sending your files...")
        sent_messages = []
        for f in files_list:
            try:
                if f["type"] == "document":
                    s_msg = await context.bot.send_document(chat_id=user_id, document=f["file_id"])
                elif f["type"] == "video":
                    s_msg = await context.bot.send_video(chat_id=user_id, video=f["file_id"])
                elif f["type"] == "photo":
                    s_msg = await context.bot.send_photo(chat_id=user_id, photo=f["file_id"])
                elif f["type"] == "text":
                    s_msg = await context.bot.send_message(chat_id=user_id, text=f["file_id"])
                sent_messages.append(s_msg.message_id)
            except Exception as e:
                log.error(f"Send error: {e}")
        await msg.delete()
        
        if sent_messages:
            timer = await get_delete_timer()
            warning_msg = await context.bot.send_message(
                chat_id=user_id, 
                text=f"⚠️ *Important:*\n\nAll Messages will be deleted after {timer} minutes. Please save or forward these messages to your personal saved messages to avoid losing them!",
                parse_mode="Markdown"
            )
            sent_messages.append(warning_msg.message_id)
            context.job_queue.run_once(delete_messages_job, timer * 60, data={"chat_id": user_id, "message_ids": sent_messages})
        
        return

    # Pick one channel completely at random from the remaining available pool
    selected = random.choice(available)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Join Channel to Access", url=selected["invite_link"])]
    ])
    msg = await update.message.reply_text(
        "🔐 To access this file, please send a *join request* to the BACKUP channel below.\n\n"
        "⚠️ Just tap the button and send a join request — your file will arrive automatically!",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await set_pending(user_id, payload, msg.message_id)


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
        "`/add_admin <id>` — Add admin (Owner)\n"
        "`/rem_admin <id>` — Remove admin (Owner)\n"
        "`/admins` — List admins (Owner)\n"
        "`/broadcast` — Broadcast message (Reply to msg)\n"
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
    
    total = len(users)
    success = 0
    failed = 0
    deleted = 0
    
    for u in users:
        user_id = u["user_id"]
        try:
            if msg_to_broadcast.forward_origin:
                await context.bot.forward_message(
                    chat_id=user_id,
                    from_chat_id=update.message.chat_id,
                    message_id=msg_to_broadcast.message_id
                )
            else:
                await context.bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=update.message.chat_id,
                    message_id=msg_to_broadcast.message_id
                )
            success += 1
        except Exception as e:
            if "Forbidden" in str(e) or "bot was blocked" in str(e) or "user is deactivated" in str(e):
                await users_col.delete_one({"user_id": user_id})
                deleted += 1
            failed += 1
            
        await asyncio.sleep(0.05)
        
    await status_msg.edit_text(
        f"📢 **Broadcast Completed!**\n\n"
        f"🎯 Total Users: {total}\n"
        f"✅ Success: {success}\n"
        f"❌ Failed: {failed}\n"
        f"🗑️ Removed (Blocked Bot): {deleted}",
        parse_mode="Markdown"
    )

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
                    f"✅ Batch created! ({len(file_ids)} files)\n\n🔗 *Link:* `{deep_link}`",
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
    await message.reply_text(f"✅ File saved!\n\n🔗 *Link:* `{deep_link}`", parse_mode="Markdown")


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
    pending_payload, msg_id = await get_pending(user_id)
    if pending_payload:
        files_list = await get_file(pending_payload)
        sent_messages = []
        if files_list:
            for f in files_list:
                try:
                    if f["type"] == "document":
                        s_msg = await context.bot.send_document(chat_id=user_id, document=f["file_id"])
                    elif f["type"] == "video":
                        s_msg = await context.bot.send_video(chat_id=user_id, video=f["file_id"])
                    elif f["type"] == "photo":
                        s_msg = await context.bot.send_photo(chat_id=user_id, photo=f["file_id"])
                    elif f["type"] == "text":
                        s_msg = await context.bot.send_message(chat_id=user_id, text=f["file_id"])
                    sent_messages.append(s_msg.message_id)
                except Exception as e:
                    log.error(f"Failed to send to {user_id}: {e}")
        await clear_pending(user_id)
        
        if msg_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=msg_id)
            except Exception as e:
                log.error(f"Failed to delete join request message: {e}")

        if sent_messages:
            timer = await get_delete_timer()
            warning_msg = await context.bot.send_message(
                chat_id=user_id, 
                text=f"⚠️ *Important:*\n\nAll Messages will be deleted after {timer} minutes. Please save or forward these messages to your personal saved messages to avoid losing them!",
                parse_mode="Markdown"
            )
            sent_messages.append(warning_msg.message_id)
            context.job_queue.run_once(delete_messages_job, timer * 60, data={"chat_id": user_id, "message_ids": sent_messages})

    # NOTE: We do NOT call request.approve(). The join request stays pending.


# ==========================================
# MAIN
# ==========================================
async def post_init(application: Application):
    """Run after the bot starts."""
    if OWNER_ID != 0:
        await add_admin(OWNER_ID)
        log.info(f"Owner initialized: {OWNER_ID}")
    me = await application.bot.get_me()
    log.info(f"Bot is running as @{me.username} (ID: {me.id})")


def main():
    log.info("Starting bot...")

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # Register handlers
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("batch", batch_cmd))
    app.add_handler(CommandHandler("add_channel", add_channel_cmd))
    app.add_handler(CommandHandler("channels", channels_cmd))
    app.add_handler(CommandHandler("set_timer", set_timer_cmd))
    app.add_handler(CommandHandler("add_admin", add_admin_cmd))
    app.add_handler(CommandHandler("rem_admin", rem_admin_cmd))
    app.add_handler(CommandHandler("admins", list_admins_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & (filters.Document.ALL | filters.VIDEO | filters.PHOTO | filters.TEXT | filters.FORWARDED) & ~filters.COMMAND,
        handle_upload
    ))
    app.add_handler(ChatJoinRequestHandler(join_request_handler))

    # Run the bot using polling (proven to work with this token)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
