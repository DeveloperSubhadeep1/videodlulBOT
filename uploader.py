import asyncio
import contextlib
import logging
import os
import re
import time
from collections import deque
from pathlib import Path
from typing import Awaitable, Callable, Optional, Tuple

from pyrogram import Client
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import Message

from config import (
    API_HASH,
    API_ID,
    BOT_TOKEN,
    MAX_CONCURRENT_UPLOADS,
    MAX_UPLOAD_BYTES,
)
from downloader import DownloadResult, OnProgress, Progress

logger = logging.getLogger(__name__)

_MAX_FLOOD_WAIT = 600


def _build_client() -> Client:
    """Build Pyrogram MTProto bot client in memory."""
    return Client(
        "uploader_bot",
        api_id=API_ID or 1,
        api_hash=API_HASH or "00000000000000000000000000000000",
        bot_token=BOT_TOKEN,
        in_memory=True,
        max_concurrent_transmissions=MAX_CONCURRENT_UPLOADS,
        sleep_threshold=180,
    )


pyro: Client = _build_client()


class UploadCancelled(Exception):
    """The user cancelled while the file was uploading."""


def upload_limit() -> int:
    """Bots uploading over MTProto support files up to 2,000 MiB (2 GB)."""
    return MAX_UPLOAD_BYTES  # 2000 MiB


def _file_name(title: str, suffix: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', " ", title)
    name = re.sub(r"\s+", " ", name).strip()[:100] or "video"
    return f"{name}{suffix}"


class _RateMeter:
    def __init__(self, window: float = 5.0):
        self._window = window
        self._samples: deque[tuple[float, int]] = deque()

    def update(self, done: int, total: int) -> tuple[Optional[float], Optional[float]]:
        now = time.monotonic()
        self._samples.append((now, done))
        while len(self._samples) > 2 and now - self._samples[0][0] > self._window:
            self._samples.popleft()
        started, done_then = self._samples[0]
        if now <= started or done <= done_then:
            return None, None
        speed = (done - done_then) / (now - started)
        return speed, (total - done) / speed if total else None


async def _send_direct(
    chat_id: int,
    thread_id: Optional[int],
    result: DownloadResult,
    progress: Callable,
    caption: str,
) -> Message:
    """Send directly to the user via MTProto."""
    path = str(result.path)

    if result.is_audio:
        return await pyro.send_audio(
            chat_id=chat_id,
            audio=path,
            caption=caption,
            duration=result.duration,
            title=result.title[:128],
            performer=result.uploader,
            file_name=_file_name(result.title, result.path.suffix),
            parse_mode=ParseMode.HTML,
            message_thread_id=thread_id,
            progress=progress,
        )

    return await pyro.send_video(
        chat_id=chat_id,
        video=path,
        caption=caption,
        duration=result.duration,
        width=result.width,
        height=result.height,
        thumb=str(result.thumb) if result.thumb else None,
        file_name=_file_name(result.title, result.path.suffix),
        parse_mode=ParseMode.HTML,
        supports_streaming=True,
        message_thread_id=thread_id,
        progress=progress,
    )


async def _unless_cancelled(work: Awaitable, cancel_event: asyncio.Event):
    task = asyncio.ensure_future(work)
    waiter = asyncio.ensure_future(cancel_event.wait())
    try:
        await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        task.cancel()
        waiter.cancel()
        raise
    waiter.cancel()
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        raise UploadCancelled()
    return task.result()


async def send_to_user(
    chat_id: int,
    thread_id: Optional[int],
    result: DownloadResult,
    caption: str,
    on_progress: Optional[OnProgress] = None,
    cancel_event: Optional[asyncio.Event] = None,
) -> Tuple[Message, str, str]:
    """
    Directly upload the file to the user over MTProto.
    Returns (message, file_id, file_type).
    """
    logger.info("Direct MTProto upload to user chat=%s path=%s", chat_id, result.path)
    cancel_event = cancel_event or asyncio.Event()
    meter = _RateMeter()

    async def progress(current: int, total: int) -> None:
        if cancel_event.is_set():
            pyro.stop_transmission()
        if on_progress is not None:
            speed, eta = meter.update(current, total)
            on_progress(Progress("upload", done=current, total=total, speed=speed, eta=eta))

    for attempt in (1, 2):
        if cancel_event.is_set():
            raise UploadCancelled()
        try:
            os.utime(result.path)
            msg = await _unless_cancelled(
                _send_direct(chat_id, thread_id, result, progress, caption),
                cancel_event,
            )
        except FloodWait as e:
            wait = int(e.value)
            if attempt == 2 or wait > _MAX_FLOOD_WAIT:
                logger.warning("Upload flood-wait %ss; giving up", wait)
                raise RuntimeError("Telegram is limiting uploads right now. Please try again shortly.") from e
            logger.warning("Upload rate-limited; retrying in %ss", wait)
            try:
                await asyncio.wait_for(cancel_event.wait(), timeout=wait)
            except asyncio.TimeoutError:
                continue
            raise UploadCancelled() from e
        except UploadCancelled:
            logger.info("Upload cancelled by user")
            raise
        except Exception as e:
            logger.exception("Direct upload failed")
            raise RuntimeError(f"Failed to send file directly to chat: {e}") from e

        if msg is None:
            raise UploadCancelled()

        # Extract file_id for instant future caching
        if msg.video:
            file_id = msg.video.file_id
            file_type = "video"
        elif msg.audio:
            file_id = msg.audio.file_id
            file_type = "audio"
        else:
            file_id = msg.document.file_id
            file_type = "document"

        return msg, file_id, file_type

    raise AssertionError("unreachable")