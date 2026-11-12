import asyncio
import contextlib
import functools
import html
import json
import logging
import os
import re
import secrets
import signal
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from telegram import Bot, BotCommand, CallbackQuery, Message, Update
from telegram.constants import ChatType, ParseMode
from telegram.error import BadRequest, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from pyrogram import Client
from pyrogram.enums import ChatType as PyroChatType, SentCodeType
from pyrogram.errors import (
    FloodWait,
    PasswordHashInvalid,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    SessionPasswordNeeded,
)

import config
import downloader
import texts
import uploader
from cache import Store
from config import (
    API_HASH,
    API_ID,
    BOT_TOKEN,
    CACHE_PATH,
    FILE_CACHE_TTL,
    MAX_CACHED_FILES,
    PROGRESS_INTERVAL,
    RELAY_CHANNEL_ID,
    SESSION_PATH,
    USER_DOWNLOADS_PER_HOUR,
    USER_LOOKUPS_PER_HOUR,
    USER_MAX_ACTIVE_DOWNLOADS,
    VIDEO_CACHE_TTL,
    config_error,
)
from downloader import (
    Choice,
    DownloadError,
    Progress,
    VideoInfo,
    check_pot_provider,
    check_proxy,
    cleanup_job_files,
    cleanup_temp_dir,
    download_video,
    get_info,
    guess_key,
)
from keyboards import build_quality_keyboard, cancel_keyboard, link_id
from limits import BUSY, UserLimits
from uploader import UploadCancelled

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

URL_RE = re.compile(r"https?://[^\s]+", re.IGNORECASE)

MAX_LINKS_PER_CHAT = 20
MAX_EDIT_RETRY_WAIT = 30

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
SETUP_TIMEOUT_SECONDS = 300


def get_admin_ids() -> set[int]:
    admin_set: set[int] = set()
    for attr in ("ADMIN_IDS", "ADMIN_ID", "OWNER_ID"):
        val = getattr(config, attr, None)
        if val is not None:
            if isinstance(val, (int, float)):
                admin_set.add(int(val))
            elif isinstance(val, (list, tuple, set, frozenset)):
                for item in val:
                    try:
                        admin_set.add(int(item))
                    except (ValueError, TypeError):
                        pass
            elif isinstance(val, str):
                for part in re.split(r"[,\s;]+", val.strip().strip('"').strip("'")):
                    if part.isdigit():
                        admin_set.add(int(part))

    for env_key in ("ADMIN_IDS", "ADMIN_ID", "OWNER_ID"):
        raw = os.environ.get(env_key, "").strip().strip('"').strip("'")
        if raw:
            for part in re.split(r"[,\s;]+", raw):
                if part.isdigit():
                    admin_set.add(int(part))
    return admin_set


store = Store(CACHE_PATH, RELAY_CHANNEL_ID, MAX_CACHED_FILES, VIDEO_CACHE_TTL, FILE_CACHE_TTL)
limits = UserLimits(frozenset(get_admin_ids()), USER_MAX_ACTIVE_DOWNLOADS, USER_DOWNLOADS_PER_HOUR, USER_LOOKUPS_PER_HOUR)

_active_tasks: set[asyncio.Task] = set()
_shutting_down = False
_uploader_ready = False


# ---------------------------------------------------------------------------
# Helpers & Environment Config
# ---------------------------------------------------------------------------

def _seconds(value) -> float:
    return value.total_seconds() if hasattr(value, "total_seconds") else float(value)


def set_env(key: str, value: str) -> None:
    os.environ[key] = str(value)
    lines = ENV_PATH.read_text(encoding="utf-8").splitlines() if ENV_PATH.exists() else []
    pattern = re.compile(rf"^\s*{re.escape(key)}\s*=")
    for i, line in enumerate(lines):
        if pattern.match(line):
            lines[i] = f"{key}={value}"
            break
    else:
        lines.append(f"{key}={value}")
    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with contextlib.suppress(OSError):
        os.chmod(ENV_PATH, 0o600)


def _session_logged_in() -> bool:
    if os.environ.get("SESSION_STRING"):
        return True
    if os.environ.get("UPLOADER_MODE", "").strip().upper() == "BOT":
        return True
    if not SESSION_PATH.exists():
        return False
    try:
        with contextlib.closing(sqlite3.connect(SESSION_PATH.as_uri() + "?mode=ro", uri=True)) as db:
            row = db.execute("SELECT user_id FROM sessions").fetchone()
    except sqlite3.Error:
        return False
    return bool(row and row[0])


def is_configured() -> bool:
    has_creds = bool(
        BOT_TOKEN
        and (API_ID or os.environ.get("API_ID"))
        and (API_HASH or os.environ.get("API_HASH"))
        and (RELAY_CHANNEL_ID or os.environ.get("RELAY_CHANNEL_ID"))
    )
    return has_creds and _session_logged_in()


async def _stop_client(client: Optional[Client]) -> None:
    """Safely stop a Pyrogram client whether initialized or uninitialized."""
    if client is None:
        return
    with contextlib.suppress(Exception):
        if getattr(client, "is_connected", False):
            if getattr(client, "is_initialized", False):
                await client.stop()
            else:
                await client.disconnect()


async def _edit(target: Message | CallbackQuery, text: str, reply_markup=None, retry: bool = True) -> bool:
    edit = target.edit_message_text if isinstance(target, CallbackQuery) else target.edit_text
    try:
        await edit(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        return True
    except RetryAfter as e:
        wait = _seconds(e.retry_after)
        if retry and wait <= MAX_EDIT_RETRY_WAIT:
            await asyncio.sleep(wait)
            return await _edit(target, text, reply_markup, retry=False)
        logger.warning("Could not edit status message: %s", e)
        return False
    except (TelegramError, TypeError) as e:
        if isinstance(e, TypeError) and "inaccessible" not in str(e).lower():
            raise
        if "not modified" in str(e).lower():
            return True
        logger.warning("Could not edit status message: %s", e)
        return False


async def _answer(query: CallbackQuery, text: str | None = None, show_alert: bool = False) -> None:
    try:
        await query.answer(text, show_alert=show_alert)
    except TelegramError as e:
        logger.warning("Could not answer callback query: %s", e)


def _remember_link(chat_data: dict, link: str, entry: dict) -> None:
    links: dict = chat_data.setdefault("links", {})
    links.pop(link, None)
    links[link] = entry
    while len(links) > MAX_LINKS_PER_CHAT:
        links.pop(next(iter(links)))


def _thread_id(message) -> int | None:
    return message.message_thread_id if getattr(message, "is_topic_message", False) else None


async def _copy_to_user(
    bot: Bot, chat_id: int, thread_id: int | None, relay_message_id: int, title: str, is_audio: bool,
) -> None:
    target_relay = uploader.current_relay_channel_id or RELAY_CHANNEL_ID or int(os.environ.get("RELAY_CHANNEL_ID", 0))
    await bot.copy_message(
        chat_id=chat_id,
        from_chat_id=target_relay,
        message_id=relay_message_id,
        message_thread_id=thread_id,
        caption=texts.caption(title, is_audio),
        parse_mode=ParseMode.HTML,
    )


def _relay_message_missing(error: BadRequest) -> bool:
    text = str(error).lower()
    return any(s in text for s in ("message to copy not found", "message_id_invalid", "message not found"))


async def _delete_relay_message(bot: Bot, message_id: int) -> None:
    target_relay = uploader.current_relay_channel_id or RELAY_CHANNEL_ID or int(os.environ.get("RELAY_CHANNEL_ID", 0))
    try:
        await bot.delete_message(chat_id=target_relay, message_id=message_id)
    except TelegramError as e:
        logger.warning(
            "Could not delete relay message %s (does the bot have delete rights?): %s",
            message_id, e,
        )


def _tracked(handler):
    @functools.wraps(handler)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        task = asyncio.current_task()
        _active_tasks.add(task)
        try:
            await handler(update, context)
        finally:
            _active_tasks.discard(task)
    return wrapper


# ---------------------------------------------------------------------------
# Interactive Setup Wizard
# ---------------------------------------------------------------------------

class SetupState:
    CONFIRM_OVERWRITE = "CONFIRM_OVERWRITE"
    ASK_API_ID = "ASK_API_ID"
    ASK_API_HASH = "ASK_API_HASH"
    CHOOSE_MODE = "CHOOSE_MODE"
    ASK_SESSION_STRING = "ASK_SESSION_STRING"
    ASK_PHONE = "ASK_PHONE"
    ASK_OTP = "ASK_OTP"
    ASK_2FA = "ASK_2FA"
    ASK_RELAY = "ASK_RELAY"


class SetupWizard:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.admin_id: Optional[int] = None
        self.chat_id: Optional[int] = None
        self.state: Optional[str] = None
        self.api_id: Optional[int] = None
        self.api_hash: Optional[str] = None
        self.mode: str = "BOT"
        self.session_string: Optional[str] = None
        self.phone_number: Optional[str] = None
        self.phone_code_hash: Optional[str] = None
        self.temp_client: Optional[Client] = None
        self.channels: list = []
        self.timeout_task: Optional[asyncio.Task] = None

    def is_active(self) -> bool:
        return self.admin_id is not None and self.state is not None

    def is_active_for(self, user_id: int, chat_id: int) -> bool:
        return self.is_active() and self.admin_id == user_id and self.chat_id == chat_id

    def reset_timeout(self, bot: Bot) -> None:
        if self.timeout_task and not self.timeout_task.done():
            self.timeout_task.cancel()
        self.timeout_task = asyncio.create_task(self._timeout_worker(bot))

    async def _timeout_worker(self, bot: Bot) -> None:
        try:
            await asyncio.sleep(SETUP_TIMEOUT_SECONDS)
            if self.is_active():
                chat_id = self.chat_id
                await self.cleanup()
                if chat_id:
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text="⌛ <b>Setup timed out due to inactivity.</b> Run /setup_session when you are ready.",
                            parse_mode=ParseMode.HTML,
                        )
                    except TelegramError:
                        pass
        except asyncio.CancelledError:
            pass

    async def cleanup(self) -> None:
        if self.timeout_task and not self.timeout_task.done():
            self.timeout_task.cancel()
            self.timeout_task = None
        if self.temp_client:
            await _stop_client(self.temp_client)
            self.temp_client = None
        self.admin_id = None
        self.chat_id = None
        self.state = None
        self.api_id = None
        self.api_hash = None
        self.mode = "BOT"
        self.session_string = None
        self.phone_number = None
        self.phone_code_hash = None
        self.channels = []


_setup = SetupWizard()


async def init_and_start_uploader(
    api_id: int,
    api_hash: str,
    relay_channel_id: int,
    bot_token: Optional[str] = None,
    session_string: Optional[str] = None,
) -> None:
    """Reconstruct and start the Pyrogram uploader client."""
    global _uploader_ready, API_ID, API_HASH, RELAY_CHANNEL_ID

    API_ID = api_id
    API_HASH = api_hash
    RELAY_CHANNEL_ID = relay_channel_id
    store.relay_channel_id = relay_channel_id
    limits._admins = frozenset(get_admin_ids())

    if uploader.pyro is not None and getattr(uploader.pyro, "is_connected", False):
        await _stop_client(uploader.pyro)

    uploader.init_client(
        api_id=api_id,
        api_hash=api_hash,
        relay_channel_id=relay_channel_id,
        bot_token=bot_token,
        session_string=session_string,
    )
    await uploader.pyro.start()
    logger.info("Uploader client connected.")
    await uploader.prime_peer_cache()
    await uploader.check_relay_channel()
    downloader.set_upload_limit(uploader.upload_limit())
    logger.info("Upload limit set to %d MiB", downloader.upload_limit // 2**20)
    _uploader_ready = True


async def cmd_setup_session(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat

    if not user:
        return

    admins = get_admin_ids()
    if not admins:
        await message.reply_text(
            f"⚠️ <b>No Administrator Configured</b>\n\n"
            f"Your Telegram User ID is: <code>{user.id}</code>\n\n"
            f"Add this line to <code>.env</code>:\n"
            f"<code>ADMIN_IDS={user.id}</code>\n\n"
            f"Then restart the bot and send /setup_session.",
            parse_mode=ParseMode.HTML,
        )
        return

    if user.id not in admins:
        await message.reply_text(
            f"⛔ <b>Access Denied: Unauthorized</b>\n\n"
            f"Your Telegram User ID is: <code>{user.id}</code>\n"
            f"Add this ID to <code>ADMIN_IDS</code> in <code>.env</code> to gain access.",
            parse_mode=ParseMode.HTML,
        )
        return

    if chat.type != ChatType.PRIVATE:
        await message.reply_text("⚠️ Setup can only be run in a private chat with the bot.")
        return

    async with _setup.lock:
        if _setup.is_active():
            if _setup.admin_id == user.id:
                await message.reply_text("You already have an active setup session. Send /cancel_setup to restart.")
            else:
                await message.reply_text("Another administrator is currently running the setup wizard. Please wait.")
            return

        _setup.admin_id = user.id
        _setup.chat_id = chat.id
        _setup.reset_timeout(context.bot)

        if _session_logged_in() or _uploader_ready:
            _setup.state = SetupState.CONFIRM_OVERWRITE
            await message.reply_text(
                "⚠️ <b>An active uploader session already exists.</b>\n\n"
                "Reconfiguring will overwrite the current session.\n\n"
                "Type <code>YES</code> to confirm, or /cancel_setup to abort.",
                parse_mode=ParseMode.HTML,
            )
            return

        await _start_credentials_step(message)


async def _start_credentials_step(message: Message) -> None:
    existing_id = str(API_ID or os.environ.get("API_ID", "")).strip()
    existing_hash = str(API_HASH or os.environ.get("API_HASH", "")).strip()

    if existing_id.isdigit() and len(existing_hash) == 32:
        _setup.state = SetupState.ASK_API_ID
        masked_hash = existing_hash[:4] + ("*" * 24) + existing_hash[-4:]
        await message.reply_text(
            f"<b>Step 1/3: Telegram API Credentials</b>\n\n"
            f"Found credentials in <code>.env</code>:\n"
            f"• API ID: <code>{existing_id}</code>\n"
            f"• API HASH: <code>{masked_hash}</code>\n\n"
            f"Reply <code>YES</code> to keep them, or enter a new numeric <b>API ID</b>:",
            parse_mode=ParseMode.HTML,
        )
    else:
        _setup.state = SetupState.ASK_API_ID
        await message.reply_text(
            "<b>Step 1/3: Telegram API Credentials</b>\n\n"
            "Get your API credentials at https://my.telegram.org under 'API development tools'.\n\n"
            "Please enter your numeric <b>API ID</b>:",
            parse_mode=ParseMode.HTML,
        )


async def cmd_cancel_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or user.id not in get_admin_ids():
        return

    async with _setup.lock:
        if not _setup.is_active():
            await update.effective_message.reply_text("There is no active setup session to cancel.")
            return

        await _setup.cleanup()
        await update.effective_message.reply_text("✅ Setup session cancelled.")


async def _show_mode_selection(message: Message) -> None:
    _setup.state = SetupState.CHOOSE_MODE
    await message.reply_text(
        "<b>Step 2/3: Choose Uploader Mode</b>\n\n"
        "1️⃣ <b>Automatic Bot Mode (Instant & Recommended)</b>\n"
        "⚡ Connects directly using your Bot Token over MTProto.\n"
        "❌ <b>No phone number, no OTP, zero expiration errors!</b>\n"
        "📦 Uploads files up to <b>2,000 MiB (2 GB)</b>.\n\n"
        "2️⃣ <b>Pyrogram Session String</b>\n"
        "Paste an existing Pyrogram session string (for Telegram Premium 4GB accounts).\n\n"
        "3️⃣ <b>Phone Number & OTP Login</b>\n"
        "Log in interactively via phone number.\n\n"
        "👉 <b>Reply <code>1</code> for Instant Automatic Mode!</b>",
        parse_mode=ParseMode.HTML,
    )


async def handle_setup_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    text = (message.text or "").strip()
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if not _setup.is_active_for(user_id, chat_id):
        return

    _setup.reset_timeout(context.bot)

    # 1. Overwrite Confirmation
    if _setup.state == SetupState.CONFIRM_OVERWRITE:
        if text.upper() == "YES":
            if SESSION_PATH.exists():
                with contextlib.suppress(OSError):
                    SESSION_PATH.unlink()
            await _start_credentials_step(message)
        else:
            await message.reply_text("Setup aborted.")
            await _setup.cleanup()
        return

    # 2. API ID
    if _setup.state == SetupState.ASK_API_ID:
        existing_id = str(API_ID or os.environ.get("API_ID", "")).strip()
        existing_hash = str(API_HASH or os.environ.get("API_HASH", "")).strip()

        if text.upper() == "YES" and existing_id.isdigit() and len(existing_hash) == 32:
            _setup.api_id = int(existing_id)
            _setup.api_hash = existing_hash
            await _show_mode_selection(message)
            return

        if not text.isdigit():
            await message.reply_text("❌ API ID must be numeric digits. Please try again:")
            return

        _setup.api_id = int(text)
        _setup.state = SetupState.ASK_API_HASH
        await message.reply_text("Now enter your 32-character hexadecimal <b>API HASH</b>:\n<i>(Deleted immediately for security)</i>", parse_mode=ParseMode.HTML)
        return

    # 3. API HASH
    if _setup.state == SetupState.ASK_API_HASH:
        with contextlib.suppress(TelegramError):
            await message.delete()

        if not re.fullmatch(r"[0-9a-fA-F]{32}", text):
            await message.reply_text("❌ API HASH must be a 32-character hexadecimal string. Please try again:")
            return

        _setup.api_hash = text
        set_env("API_ID", str(_setup.api_id))
        set_env("API_HASH", _setup.api_hash)
        await _show_mode_selection(message)
        return

    # 4. Mode Selection
    if _setup.state == SetupState.CHOOSE_MODE:
        if text in ("1", "1️⃣", "AUTO", "AUTOMATIC", "BOT"):
            _setup.mode = "BOT"
            status_msg = await message.reply_text("Connecting via MTProto using Bot Token…")
            try:
                _setup.temp_client = Client(
                    "uploader",
                    api_id=_setup.api_id,
                    api_hash=_setup.api_hash,
                    bot_token=BOT_TOKEN,
                    workdir=str(BASE_DIR),
                )
                await _setup.temp_client.start()
                me = await _setup.temp_client.get_me()
                _setup.state = SetupState.ASK_RELAY

                current_relay = os.environ.get("RELAY_CHANNEL_ID", "").strip()
                keep_text = f"\n\nReply <code>YES</code> to keep current channel ID <code>{current_relay}</code>" if current_relay else ""

                await _edit(
                    status_msg,
                    f"✅ <b>Connected via MTProto as @{me.username}!</b>\n\n"
                    f"<b>Step 3/3: Relay Channel</b>\n"
                    f"Enter the Channel ID of your private relay channel (e.g. <code>-100xxxxxxxxxx</code>):{keep_text}",
                )
            except Exception as e:
                logger.exception("Failed to initialize bot MTProto")
                await _edit(status_msg, f"❌ Failed to connect: {e}\nTry /cancel_setup and restart.")
            return

        if text in ("2", "2️⃣", "STRING"):
            _setup.state = SetupState.ASK_SESSION_STRING
            await message.reply_text("Please paste your Pyrogram <code>SESSION_STRING</code>:\n<i>(Deleted immediately for security)</i>", parse_mode=ParseMode.HTML)
            return

        if text in ("3", "3️⃣", "PHONE", "OTP"):
            _setup.state = SetupState.ASK_PHONE
            await message.reply_text("Enter the uploader account phone number with country code (e.g. <code>+8801712345678</code>):", parse_mode=ParseMode.HTML)
            return

        await message.reply_text("Reply <code>1</code> for Automatic Bot Mode, <code>2</code> for Session String, or <code>3</code> for Phone/OTP:")
        return

    # 5. Session String Input
    if _setup.state == SetupState.ASK_SESSION_STRING:
        with contextlib.suppress(TelegramError):
            await message.delete()

        status_msg = await message.reply_text("Verifying session string…")
        try:
            _setup.session_string = text
            _setup.temp_client = Client(
                "uploader",
                api_id=_setup.api_id,
                api_hash=_setup.api_hash,
                session_string=_setup.session_string,
            )
            await _setup.temp_client.start()
            set_env("SESSION_STRING", _setup.session_string)
            await _on_auth_success(message, context)
        except Exception as e:
            logger.exception("Invalid session string")
            await _edit(status_msg, f"❌ Invalid session string: {e}\nPlease paste a valid Pyrogram session string:")
        return

    # 6. Phone Login (Fallback)
    if _setup.state == SetupState.ASK_PHONE:
        phone = text.replace(" ", "").replace("-", "")
        if not re.fullmatch(r"^\+[1-9]\d{6,14}$", phone):
            await message.reply_text("❌ Invalid format. Must start with '+' followed by country code and digits:")
            return

        _setup.phone_number = phone
        status_msg = await message.reply_text("Requesting login code from Telegram…")
        try:
            _setup.temp_client = Client(
                "uploader",
                api_id=_setup.api_id,
                api_hash=_setup.api_hash,
                workdir=str(BASE_DIR),
            )
            await _setup.temp_client.connect()
            sent_code = await _setup.temp_client.send_code(_setup.phone_number)
            _setup.phone_code_hash = sent_code.phone_code_hash
            _setup.state = SetupState.ASK_OTP

            code_type = getattr(sent_code, "type", None)
            method = "Telegram app" if code_type == SentCodeType.APP else "SMS"
            await _edit(status_msg, f"📩 Code sent via <b>{method}</b>. Enter the numeric code below:")
        except Exception as e:
            logger.exception("Failed to send code")
            await _edit(status_msg, f"❌ Failed: {e}\nConsider replying <code>/setup_session</code> and picking <b>Option 1 (Automatic Mode)</b>.")
        return

    # 7. OTP Code
    if _setup.state == SetupState.ASK_OTP:
        with contextlib.suppress(TelegramError):
            await message.delete()

        match = re.search(r"\b\d{5,6}\b", text)
        code = match.group(0) if match else re.sub(r"\D", "", text)

        try:
            await _setup.temp_client.sign_in(_setup.phone_number, _setup.phone_code_hash, code)
            await _on_auth_success(message, context)
        except SessionPasswordNeeded:
            _setup.state = SetupState.ASK_2FA
            await message.reply_text("🔐 Enter your Telegram 2FA cloud password:", parse_mode=ParseMode.HTML)
        except Exception as e:
            await message.reply_text(
                f"❌ Login failed: {e}\n\n"
                "💡 <i>Tip: Run /setup_session and choose <b>Option 1 (Automatic Bot Mode)</b> to connect instantly without OTPs!</i>",
                parse_mode=ParseMode.HTML,
            )
        return

    # 8. 2FA Password
    if _setup.state == SetupState.ASK_2FA:
        with contextlib.suppress(TelegramError):
            await message.delete()
        try:
            await _setup.temp_client.check_password(text)
            await _on_auth_success(message, context)
        except Exception as e:
            await message.reply_text(f"❌ 2FA error: {e}. Try again or /cancel_setup.")
        return

    # 9. Relay Channel Selection
    if _setup.state == SetupState.ASK_RELAY:
        picked_channel_id: Optional[int] = None
        current_relay = os.environ.get("RELAY_CHANNEL_ID", "").strip()

        if text.upper() == "YES" and current_relay:
            picked_channel_id = int(current_relay)
        elif text.isdigit() and 1 <= int(text) <= len(_setup.channels):
            picked_channel_id = _setup.channels[int(text) - 1].id
        else:
            try:
                picked_channel_id = int(text)
            except ValueError:
                await message.reply_text("❌ Please enter a valid Channel ID (e.g. <code>-100xxxxxxxxxx</code>):")
                return

        status_msg = await message.reply_text("Verifying channel access…")
        uploader_chat = None
        try:
            uploader_chat = await _setup.temp_client.get_chat(picked_channel_id)
        except Exception as first_err:
            # Bot accounts may not have this peer cached yet — try the bare channel ID
            err_str = str(first_err).lower()
            if "peer" in err_str or "invalid" in err_str:
                raw_str = str(picked_channel_id).lstrip("-")
                if raw_str.startswith("100"):
                    raw_str = raw_str[3:]
                try:
                    uploader_chat = await _setup.temp_client.get_chat(int(raw_str))
                except Exception:
                    pass
            if uploader_chat is None:
                await _edit(
                    status_msg,
                    f"❌ Cannot access channel <code>{picked_channel_id}</code>: {first_err}\n\n"
                    "Make sure:\n"
                    "• The bot is added to the channel as an <b>Administrator</b>\n"
                    "• The Channel ID is correct (copy it from channel info or a bot like @userinfobot)\n"
                    "• Try the ID without the <code>-100</code> prefix if it still fails",
                )
                return

        set_env("RELAY_CHANNEL_ID", str(picked_channel_id))
        set_env("UPLOADER_MODE", _setup.mode)

        # Safely stop temp_client without triggering ConnectionError
        await _stop_client(_setup.temp_client)
        _setup.temp_client = None

        bot_token_arg = BOT_TOKEN if _setup.mode == "BOT" else None
        try:
            await init_and_start_uploader(
                api_id=_setup.api_id,
                api_hash=_setup.api_hash,
                relay_channel_id=picked_channel_id,
                bot_token=bot_token_arg,
                session_string=_setup.session_string,
            )
        except Exception as e:
            logger.exception("Failed to start uploader")
            await _edit(status_msg, f"⚠️ Error starting uploader: {e}")
            await _setup.cleanup()
            return

        mode_name = "Automatic Bot Mode (2 GB limit)" if _setup.mode == "BOT" else "Account Session"
        await _edit(
            status_msg,
            f"🎉 <b>Setup completed successfully!</b>\n\n"
            f"• Mode: <b>{mode_name}</b>\n"
            f"• Relay Channel: <b>{html.escape(uploader_chat.title or 'Relay')}</b> [<code>{picked_channel_id}</code>]\n"
            f"• Upload Limit: <b>{downloader.upload_limit // (1024 * 1024)} MiB</b>\n"
            f"• Status: <b>Online and Ready</b>\n\n"
            f"Users can now send video links to download.",
        )
        await _setup.cleanup()


async def _on_auth_success(message: Message, context: ContextTypes.DEFAULT_TYPE) -> None:
    me = await _setup.temp_client.get_me()
    premium_text = "Yes (4000 MiB limit)" if getattr(me, "is_premium", False) else "No (2000 MiB limit)"

    channels = []
    if not getattr(me, "is_bot", False):
        with contextlib.suppress(Exception):
            async for dialog in _setup.temp_client.get_dialogs():
                if dialog.chat.type == PyroChatType.CHANNEL:
                    channels.append(dialog.chat)

    _setup.channels = channels
    _setup.state = SetupState.ASK_RELAY
    current_relay = os.environ.get("RELAY_CHANNEL_ID", "").strip()
    keep_msg = f" (or reply <code>YES</code> to keep ID <code>{current_relay}</code>)" if current_relay else ""

    if channels:
        ch_list = "\n".join(f"<b>{i}.</b> {html.escape(c.title or 'Channel')} [<code>{c.id}</code>]" for i, c in enumerate(channels[:25], 1))
        await message.reply_text(
            f"✅ <b>Authenticated as {html.escape(me.first_name)}!</b>\n"
            f"• Premium: <b>{premium_text}</b>\n\n"
            f"<b>Step 3/3: Relay Channel</b>\n\n{ch_list}\n\n"
            f"Reply with the channel number or enter a Channel ID{keep_msg}:",
            parse_mode=ParseMode.HTML,
        )
    else:
        await message.reply_text(
            f"✅ <b>Authenticated!</b>\n\n"
            f"<b>Step 3/3: Relay Channel</b>\n"
            f"Enter your relay Channel ID (e.g. <code>-100xxxxxxxxxx</code>){keep_msg}:",
            parse_mode=ParseMode.HTML,
        )


# ---------------------------------------------------------------------------
# Download Progress and Cancelling
# ---------------------------------------------------------------------------

class _StatusUpdater:
    def __init__(self, query: CallbackQuery, title: str, label: str, token: str):
        self._query = query
        self._title = title
        self._label = label
        self._keyboard = cancel_keyboard(token)
        self._loop = asyncio.get_running_loop()
        self._pending: Optional[Progress] = None
        self._flusher: Optional[asyncio.Task] = None
        self._next_edit = 0.0
        self._last_text: Optional[str] = None
        self.closed = False

    def report(self, progress: Progress) -> None:
        self._loop.call_soon_threadsafe(self._schedule, progress)

    def _schedule(self, progress: Progress) -> None:
        if self.closed:
            return
        self._pending = progress
        if self._flusher is None or self._flusher.done():
            self._flusher = self._loop.create_task(self._flush())

    async def _flush(self) -> None:
        delay = self._next_edit - self._loop.time()
        if delay > 0:
            await asyncio.sleep(delay)
        progress, self._pending = self._pending, None
        if progress is not None:
            await self.show(texts.progress(self._title, self._label, progress))

    async def show(self, text: str) -> None:
        if self.closed or text == self._last_text:
            return
        try:
            await self._query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=self._keyboard)
            self._last_text = text
        except RetryAfter as e:
            self._next_edit = self._loop.time() + _seconds(e.retry_after)
            return
        except (TelegramError, TypeError) as e:
            if "not modified" not in str(e).lower():
                logger.debug("Progress edit failed: %s", e)
        self._next_edit = self._loop.time() + PROGRESS_INTERVAL

    def close(self) -> None:
        self.closed = True
        if self._flusher is not None and not self._flusher.done():
            self._flusher.cancel()


@dataclass
class _Job:
    user_id: int
    task: asyncio.Task
    updater: _StatusUpdater
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    uploading: bool = False
    finished: bool = False

    @property
    def cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def cancel(self) -> bool:
        if self.finished or self.cancelled:
            return False
        self.cancel_event.set()
        self.updater.close()
        if not self.uploading:
            self.task.cancel()
        return True


_jobs: dict[str, _Job] = {}


async def handle_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    try:
        token = json.loads(query.data)["c"]
    except (json.JSONDecodeError, KeyError, TypeError):
        token = None
    job = _jobs.get(token)
    if job is None or job.finished:
        await _answer(query, texts.nothing_to_cancel())
        return
    user_id = update.effective_user.id
    if user_id != job.user_id and user_id not in get_admin_ids():
        await _answer(query, texts.not_your_download(), show_alert=True)
        return
    cancelled = job.cancel()
    await _answer(query, texts.cancelling() if cancelled else texts.nothing_to_cancel())


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def _begin_shutdown(app: Application) -> None:
    global _shutting_down
    if _shutting_down:
        logger.warning("Second stop signal: exiting immediately.")
        os._exit(1)
    _shutting_down = True
    logger.info("Stopping: cancelling %d running job(s)…", len(_active_tasks))
    for task in list(_active_tasks):
        task.cancel()
    if app.running:
        app.stop_running()
    else:
        raise SystemExit(0)


def _install_signal_handlers(app: Application) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _begin_shutdown, app)
        except (NotImplementedError, RuntimeError):
            pass


async def post_init(app: Application) -> None:
    global _uploader_ready
    _install_signal_handlers(app)
    cleanup_temp_dir()
    await asyncio.to_thread(store.load)
    await downloader.warm_up()

    if is_configured():
        try:
            await uploader.pyro.start()
            logger.info("Pyrogram uploader client started.")
            await uploader.prime_peer_cache()
            await uploader.check_relay_channel()
            target_relay = uploader.current_relay_channel_id or RELAY_CHANNEL_ID or int(os.environ.get("RELAY_CHANNEL_ID", 0))
            try:
                await app.bot.get_chat(target_relay)
            except TelegramError as e:
                logger.warning(
                    "The bot cannot open relay channel %s (%s). Ensure the bot is added as an admin.",
                    target_relay, e,
                )
            downloader.set_upload_limit(uploader.upload_limit())
            logger.info("Upload limit: %d MiB", downloader.upload_limit // 2**20)
            _uploader_ready = True
        except Exception as e:
            logger.error("Could not start uploader client: %s. Run /setup_session to configure.", e)
            _uploader_ready = False
    else:
        logger.warning("Uploader account is not authenticated yet. Bot started in setup mode.")
        _uploader_ready = False

    try:
        commands = [BotCommand(name, desc) for name, desc in texts.commands()]
        commands.append(BotCommand("setup_session", "Setup uploader account (Admin)"))
        commands.append(BotCommand("cancel_setup", "Cancel session setup (Admin)"))
        await app.bot.set_my_commands(commands)
    except TelegramError as e:
        logger.warning("Could not set the bot's command menu: %s", e)

    await check_pot_provider()
    await check_proxy()


async def post_shutdown(app: Application) -> None:
    await _setup.cleanup()
    if uploader.pyro and getattr(uploader.pyro, "is_connected", False):
        await _stop_client(uploader.pyro)
    downloader.shutdown()
    logger.info("Stopped.")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled error while processing an update", exc_info=context.error)


# ---------------------------------------------------------------------------
# Command Handlers
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    admins = get_admin_ids()

    if not _uploader_ready:
        if user_id in admins:
            await update.effective_message.reply_text(
                "👋 <b>Welcome, Administrator!</b>\n\n"
                "The bot is running in <b>Setup Mode</b>.\n"
                "Send /setup_session to configure the uploader instantly.",
                parse_mode=ParseMode.HTML,
            )
            return

        await update.effective_message.reply_text(
            f"👋 Welcome! The bot is currently under maintenance. Please check back later.\n\n"
            f"<i>(Your Telegram ID: <code>{user_id}</code>)</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    await update.effective_message.reply_text(texts.start(), parse_mode=ParseMode.HTML)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(texts.help_text(), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# Link Handler
# ---------------------------------------------------------------------------

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if _setup.is_active_for(user_id, chat_id):
        await handle_setup_step(update, context)
        return

    message = update.effective_message

    if not _uploader_ready:
        if user_id in get_admin_ids():
            await message.reply_text("⚠️ <b>Uploader not ready.</b> Run /setup_session to configure.", parse_mode=ParseMode.HTML)
        else:
            await message.reply_text("The downloader service is temporarily unavailable. Please try again later.", parse_mode=ParseMode.HTML)
        return

    match = URL_RE.search(message.text or "")
    if not match:
        if update.effective_chat.type == ChatType.PRIVATE:
            await message.reply_text(texts.no_link(), parse_mode=ParseMode.HTML)
        return

    url = match.group(0)
    status = await message.reply_text(texts.fetching(), parse_mode=ParseMode.HTML)

    try:
        guess = await guess_key(url)
        cached = store.video(guess) if guess else None
        if cached is not None:
            info = VideoInfo.from_dict(cached)
        else:
            wait = limits.take_lookup(user_id)
            if wait:
                await _edit(status, texts.lookup_limit(wait))
                return

            async def on_wait(queue: str, position: int) -> None:
                await _edit(status, texts.queued(queue, position) if position else texts.fetching())

            try:
                info = await get_info(url, key_hint=guess, on_wait=on_wait)
            except DownloadError as e:
                await _edit(status, texts.error(str(e)))
                return
            await asyncio.to_thread(store.put_video, info.to_dict(), guess)

        link = link_id(url)
        keyboard, choices = build_quality_keyboard(info, link)
        _remember_link(context.chat_data, link, {
            "url": url, "key": info.key, "title": info.title, "choices": choices,
        })
        if not await _edit(status, texts.choose(info), reply_markup=keyboard):
            await message.reply_text(texts.choose(info), parse_mode=ParseMode.HTML, reply_markup=keyboard)

    except asyncio.CancelledError:
        await _edit(status, texts.restarting())
        raise
    except Exception:
        logger.exception("Unexpected error while handling a link")
        await _edit(status, texts.error(texts.GENERIC_ERROR))


# ---------------------------------------------------------------------------
# Choice Handler
# ---------------------------------------------------------------------------

async def handle_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not _uploader_ready:
        await _answer(query, "Service is temporarily unavailable while maintenance is being completed.", show_alert=True)
        return

    try:
        payload = json.loads(query.data)
        label: str = payload["l"]
        link: str = payload["h"]
    except (json.JSONDecodeError, KeyError, TypeError):
        payload = None

    entry = (context.chat_data or {}).get("links", {}).get(link) if payload else None
    choice: Choice | None = entry["choices"].get(label) if entry else None
    if choice is None:
        await _answer(query)
        await _edit(query, texts.menu_expired())
        return

    chat_id = update.effective_chat.id
    thread_id = _thread_id(query.message)
    user_id = update.effective_user.id
    title: str = entry["title"]
    keyboard = getattr(query.message, "reply_markup", None)
    answered = False

    cached_message_id = store.file(entry["key"], choice.label)
    if cached_message_id is not None:
        await _answer(query)
        answered = True
        try:
            await _copy_to_user(context.bot, chat_id, thread_id, cached_message_id, title, choice.is_audio)
            await _edit(query, texts.done_instantly(title))
            return
        except BadRequest as e:
            if not _relay_message_missing(e):
                logger.warning("Could not send cached file %s to chat %s: %s", cached_message_id, chat_id, e)
                await _edit(query, texts.error(texts.DELIVERY_FAILED), reply_markup=keyboard)
                return
            logger.info("Cached relay message %s is missing; re-downloading", cached_message_id)
            await asyncio.to_thread(store.drop_file, entry["key"], choice.label, cached_message_id)
        except TelegramError as e:
            logger.warning("Could not send cached file %s to chat %s: %s", cached_message_id, chat_id, e)
            await _edit(query, texts.error(texts.DELIVERY_FAILED), reply_markup=keyboard)
            return

    verdict = limits.start_download(user_id)
    if verdict:
        alert = texts.busy_alert() if verdict == BUSY else texts.download_limit_alert(verdict)
        if answered:
            await _edit(query, texts.error(alert), reply_markup=keyboard)
        else:
            await _answer(query, alert, show_alert=True)
        return

    token = secrets.token_hex(4)
    updater = _StatusUpdater(query, title, choice.label, token)
    job = _Job(user_id, asyncio.current_task(), updater)
    _jobs[token] = job

    def finish() -> None:
        job.finished = True
        _jobs.pop(token, None)
        updater.close()

    result = None
    pending = None
    try:
        if not answered:
            await _answer(query)
        await updater.show(texts.downloading(title, choice.label))

        async def on_wait(queue: str, position: int) -> None:
            await updater.show(texts.queued(queue, position) if position else texts.downloading(title, choice.label))

        result = await download_video(
            entry["url"], choice, key=entry["key"], on_wait=on_wait, on_progress=updater.report,
        )
        logger.info("Download complete: %s", result.path)

        job.uploading = True
        if job.cancelled:
            raise UploadCancelled()
        await updater.show(texts.uploading(title))
        relay_msg = await uploader.send_to_relay(
            result, on_progress=updater.report, cancel_event=job.cancel_event,
        )
        evicted = await asyncio.to_thread(store.put_file, result.key, choice.label, relay_msg.id)
        for old_id in evicted:
            context.application.create_task(_delete_relay_message(context.bot, old_id))
        if job.cancelled:
            raise UploadCancelled()
        finish()

        await _copy_to_user(context.bot, chat_id, thread_id, relay_msg.id, title, choice.is_audio)
        logger.info("Delivered to chat_id=%s", chat_id)
        await _edit(query, texts.done(title))

    except asyncio.CancelledError as e:
        finish()
        pending = getattr(e, "pending", None)
        if job.cancelled and not _shutting_down:
            asyncio.current_task().uncancel()
            await _edit(query, texts.cancelled(title), reply_markup=keyboard)
        else:
            await _edit(query, texts.restarting())
            raise
    except UploadCancelled:
        finish()
        await _edit(query, texts.cancelled(title), reply_markup=keyboard)
    except (DownloadError, RuntimeError) as e:
        finish()
        pending = getattr(e, "pending", None)
        await _edit(query, texts.error(str(e)), reply_markup=keyboard)
    except TelegramError as e:
        finish()
        logger.warning("Could not deliver to chat %s: %s", chat_id, e)
        await _edit(query, texts.error(texts.DELIVERY_FAILED), reply_markup=keyboard)
    except Exception:
        finish()
        logger.exception("Unexpected error in handle_choice")
        await _edit(query, texts.error(texts.GENERIC_ERROR), reply_markup=keyboard)
    finally:
        finish()
        if pending is not None:
            pending.add_done_callback(lambda _: limits.finish_download(user_id))
        else:
            limits.finish_download(user_id)
        if result is not None:
            cleanup_job_files(result.job_id)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def _check_settings() -> None:
    if not BOT_TOKEN:
        config_error("Missing BOT_TOKEN in .env. The bot cannot start without a bot token.")
    if not get_admin_ids():
        logger.warning("No ADMIN_IDS configured in .env! Admins will not be able to run /setup_session.")
    if not is_configured():
        logger.warning(
            "Uploader credentials or session not found. Bot is starting in setup mode. "
            "Authorized admins can configure it by sending /setup_session in private chat."
        )


def main() -> None:
    _check_settings()
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("setup_session", cmd_setup_session))
    app.add_handler(CommandHandler("cancel_setup", cmd_cancel_setup))
    app.add_handler(
        MessageHandler(
            filters.UpdateType.MESSAGE & filters.TEXT & ~filters.COMMAND,
            _tracked(handle_text),
        )
    )
    app.add_handler(CallbackQueryHandler(handle_cancel, pattern=r'^\{"c":'))
    app.add_handler(CallbackQueryHandler(_tracked(handle_choice)))
    app.add_error_handler(on_error)

    logger.info("Bot is running…")
    app.run_polling(allowed_updates=[Update.MESSAGE, Update.CALLBACK_QUERY], stop_signals=None)
    logging.shutdown()
    os._exit(0)


if __name__ == "__main__":
    main()