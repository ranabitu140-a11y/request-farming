<div align="center">
  <h1>🚀 Dynamic Force Request Telegram Bot</h1>
  <p><i>A high-traffic, asynchronous Telegram File Sharing Bot with a rotating Dynamic Force-Join System and Auto-Deletion Features.</i></p>
</div>

---

## 🌟 Overview
This Telegram bot is a **high-performance file-sharing solution** built on Pyrogram and Motor (Async MongoDB). It strictly manages a randomized pool of private channels and delivers media seamlessly when users submit a join request, **without actually approving the request** (farming pending join requests). 

It is designed for speed, security, and high concurrency. The bot never stores physical files; it relies entirely on Telegram's highly efficient `file_id` caching.

## ✨ Features
- **Dynamic Channel Rotation:** Automatically randomizes the force-join channel from a pool.
- **Join-Request Delivery:** Triggers instantaneous file delivery the moment a user requests to join the channel.
- **Batch Processing:** Create a single start link containing multiple files (`/batch` command).
- **Auto-Delete Timer:** Automatically deletes delivered files and warnings after a dynamic, customizable timeframe to prevent leaks.
- **Media Support:** Seamlessly handles Documents, Videos, Photos, and Text Messages.
- **Clean UI:** Automatically deletes the "Join Channel" buttons from user chats once files are delivered.
- **Admin Management:** Owners can dynamically promote/demote admins via Telegram commands.
- **Channel Management:** Interactive UI to add and remove force-join channels directly within Telegram.

---

## 🛠 Prerequisites
- **Python 3.10+**
- **MongoDB Database** (e.g., MongoDB Atlas)
- **Telegram API ID and Hash** (from my.telegram.org)
- **Bot Token** (from @BotFather)

## 📦 Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/ranabitu140-a11y/request-farming.git
   cd request-farming
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the Environment Variables**
   Create a `.env` file in the root directory and add the following:
   ```ini
   API_ID=your_api_id
   API_HASH=your_api_hash
   BOT_TOKEN=your_bot_token
   MONGO_URI=mongodb+srv://...
   DB_NAME=dynamic_force_db
   OWNER_ID=your_telegram_user_id
   DELETE_TIMER=30
   ```

4. **Run the Bot**
   ```bash
   python bot.py
   ```

---

## ⚙️ Commands

### 👤 User Commands
- `/start` — Start the bot (or access a file link)

### 👑 Owner/Admin Commands
- `/batch` — Create a batch link for multiple files (forward first & last message)
- `/add_channel <channel_id>` — Add a new force-join channel (Bot must be an admin with invite rights)
- `/channels` — List all active channels with interactive Remove buttons
- `/set_timer <minutes>` — Change the auto-delete timer dynamically
- `/add_admin <user_id>` — Promote a user to Admin *(Owner Only)*
- `/rem_admin <user_id>` — Demote an Admin *(Owner Only)*
- `/admins` — View list of current Admins *(Owner Only)*

---

## 🛡 Security & Architecture
- **Motor (AsyncIOMotorClient):** Utilizes async I/O operations for non-blocking, fast read/write speeds under heavy user load.
- **Zero Content Stored:** Protects copyright and storage limits by exclusively saving and forwarding Telegram `file_id` strings.
- **Silent Forwarding:** The batch creation is fully silent and does not flood the admin chat with copied media.
