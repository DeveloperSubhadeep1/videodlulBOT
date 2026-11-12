import asyncio
import contextlib
import functools
import json
import logging
import os
import re
import secrets
import signal
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from telegram import BotCommand, CallbackQuery, Message, Update
from telegram.constants import ChatType, ParseMode
from telegram.error import BadRequest, RetryAfter, TelegramError
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
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


# Initialize cache store without relay channels
store = Store(CACHE_PATH, MAX_CACHED_FILES, VIDEO_CACHE_TTL, FILE_CACHE_TTL)
limits = UserLimits(frozenset(get_admin_ids()), USER_MAX_ACTIVE_DOWNLOADS, USER_DOWNLOADS_PER_HOUR, USER_LOOKUPS_PER_HOUR)

_active_tasks: set[asyncio.Task] = set()
_shutting_down = False
_uploader_ready = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seconds(value) -> float:
    return value.total_seconds() if hasattr(value, "total_seconds") else float(value)


def is_configured() -> bool:
    return bool(BOT_TOKEN and API_ID and API_HASH)


async def _safe_reply(message: Message, text: str, **kwargs) -> Optional[Message]:
    try:
        return await message.reply_text(text, quote=False, **kwargs)
    except BadRequest:
        with contextlib.suppress(Exception):
            return await message.get_bot().send_message(chat_id=message.chat_id, text=text, **kwargs)
    except Exception:
        return None


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
        return False
    except (TelegramError, TypeError) as e:
        if "not modified" in str(e).lower():
            return True
        return False


async def _answer(query: CallbackQuery, text: str | None = None, show_alert: bool = False) -> None:
    with contextlib.suppress(TelegramError):
        await query.answer(text, show_alert=show_alert)


def _remember_link(chat_data: dict, link: str, entry: dict) -> None:
    links: dict = chat_data.setdefault("links", {})
    links.pop(link, None)
    links[link] = entry
    while len(links) > MAX_LINKS_PER_CHAT:
        links.pop(next(iter(links)))


def _thread_id(message) -> int | None:
    return message.message_thread_id if getattr(message, "is_topic_message", False) else None


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
# Status Updater with Dashboard
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

    def report(self, progress_obj: Progress) -> None:
        self._loop.call_soon_threadsafe(self._schedule, progress_obj)

    def _schedule(self, progress_obj: Progress) -> None:
        if self.closed:
            return
        self._pending = progress_obj
        if self._flusher is None or self._flusher.done():
            self._flusher = self._loop.create_task(self._flush())

    async def _flush(self) -> None:
        delay = self._next_edit - self._loop.time()
        if delay > 0:
            await asyncio.sleep(delay)
        p, self._pending = self._pending, None
        if p is not None:
            await self.show(texts.progress(self._title, self._label, p))

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
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, _begin_shutdown, app)


async def post_init(app: Application) -> None:
    global _uploader_ready
    _install_signal_handlers(app)
    cleanup_temp_dir()
    await asyncio.to_thread(store.load)
    await downloader.warm_up()

    if is_configured():
        try:
            # Start Pyrogram MTProto bot client in RAM
            await uploader.pyro.start()
            downloader.set_upload_limit(uploader.upload_limit())
            logger.info("Direct MTProto engine started (Limit: %d MiB). Ready!", downloader.upload_limit // (1024 * 1024))
            _uploader_ready = True
        except Exception as e:
            logger.error("Could not start Pyrogram MTProto engine: %s", e)
            _uploader_ready = False
    else:
        logger.error("Missing credentials in .env: BOT_TOKEN, API_ID, and API_HASH are required.")
        _uploader_ready = False

    with contextlib.suppress(TelegramError):
        await app.bot.set_my_commands([BotCommand(name, desc) for name, desc in texts.commands()])

    await check_pot_provider()
    await check_proxy()


async def post_shutdown(app: Application) -> None:
    if uploader.pyro and getattr(uploader.pyro, "is_connected", False):
        with contextlib.suppress(Exception):
            await uploader.pyro.stop()
    downloader.shutdown()
    logger.info("Stopped.")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled error while processing an update", exc_info=context.error)


# ---------------------------------------------------------------------------
# Command Handlers
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _safe_reply(update.effective_message, texts.start(), parse_mode=ParseMode.HTML)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _safe_reply(update.effective_message, texts.help_text(), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# Link Handler
# ---------------------------------------------------------------------------

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not _uploader_ready:
        await _safe_reply(message, "The service is temporarily initializing. Please try again shortly.", parse_mode=ParseMode.HTML)
        return

    match = URL_RE.search(message.text or "")
    if not match:
        if update.effective_chat.type == ChatType.PRIVATE:
            await _safe_reply(message, texts.no_link(), parse_mode=ParseMode.HTML)
        return

    url = match.group(0)
    user_id = update.effective_user.id
    status = await _safe_reply(message, texts.fetching(), parse_mode=ParseMode.HTML)
    if not status:
        return

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
            await _safe_reply(message, texts.choose(info), parse_mode=ParseMode.HTML, reply_markup=keyboard)

    except asyncio.CancelledError:
        await _edit(status, texts.restarting())
        raise
    except Exception:
        logger.exception("Unexpected error while handling link")
        await _edit(status, texts.error(texts.GENERIC_ERROR))


# ---------------------------------------------------------------------------
# Choice Handler
# ---------------------------------------------------------------------------

async def handle_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not _uploader_ready:
        await _answer(query, "Service is initializing. Please try again in a moment.", show_alert=True)
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

    # 1. Instant Cache Check (Deliver via cached file_id in 0.1s!)
    cached_entry = store.file(entry["key"], choice.label)
    if cached_entry is not None:
        cached_file_id, cached_type = cached_entry
        await _answer(query)
        answered = True
        try:
            caption_text = texts.caption(title, choice.is_audio)
            if cached_type == "audio":
                await context.bot.send_audio(chat_id, audio=cached_file_id, caption=caption_text, parse_mode=ParseMode.HTML, message_thread_id=thread_id)
            elif cached_type == "document":
                await context.bot.send_document(chat_id, document=cached_file_id, caption=caption_text, parse_mode=ParseMode.HTML, message_thread_id=thread_id)
            else:
                await context.bot.send_video(chat_id, video=cached_file_id, caption=caption_text, parse_mode=ParseMode.HTML, message_thread_id=thread_id)
            await _edit(query, texts.done_instantly(title))
            return
        except Exception as e:
            logger.info("Cached file_id expired or invalid (%s); re-downloading", e)
            await asyncio.to_thread(store.drop_file, entry["key"], choice.label)

    # 2. Rate Limits
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

        # Step 1: Download Media
        result = await download_video(
            entry["url"], choice, key=entry["key"], on_wait=on_wait, on_progress=updater.report,
        )
        logger.info("Download complete: %s", result.path)

        # Step 2: Stream Directly to User via MTProto
        job.uploading = True
        if job.cancelled:
            raise UploadCancelled()
        await updater.show(texts.uploading(title))

        caption_text = texts.caption(title, choice.is_audio)
        _, file_id, file_type = await uploader.send_to_user(
            chat_id=chat_id,
            thread_id=thread_id,
            result=result,
            caption=caption_text,
            on_progress=updater.report,
            cancel_event=job.cancel_event,
        )

        # Cache file_id for future instant delivery
        await asyncio.to_thread(store.put_file, result.key, choice.label, file_id, file_type)
        finish()
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
        logger.warning("Delivery error to chat %s: %s", chat_id, e)
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
        config_error("Missing BOT_TOKEN in .env.")
    if not API_ID or not API_HASH:
        config_error("Missing API_ID or API_HASH in .env.")


def main() -> None:
    _check_settings()

    request = HTTPXRequest(
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
        pool_timeout=30.0,
    )

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(HTTPXRequest(connect_timeout=30.0, read_timeout=30.0))
        .concurrent_updates(True)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
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
    app.run_polling(
        allowed_updates=[Update.MESSAGE, Update.CALLBACK_QUERY],
        bootstrap_retries=5,
        stop_signals=None,
    )
    logging.shutdown()
    os._exit(0)


if __name__ == "__main__":
    main()