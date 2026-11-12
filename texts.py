"""Every message the bot sends, as Telegram HTML."""

import math
from html import escape
from typing import Optional

import downloader
from config import BOT_NAME, USER_DOWNLOADS_PER_HOUR
from downloader import Progress, VideoInfo
from emojis import tg_emoji as e

TITLE_LIMIT = 150
CAPTION_TITLE_LIMIT = 900
_BAR_LENGTH = 12

GENERIC_ERROR = "Something went wrong. Please try again."
DELIVERY_FAILED = "I couldn't send the file here. Please try again."


def _shorten(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _title(title: str, limit: int = TITLE_LIMIT) -> str:
    return escape(_shorten(title, limit))


def _duration(seconds: float) -> str:
    hours, rest = divmod(max(0, round(seconds)), 3600)
    mins, secs = divmod(rest, 60)
    return f"{hours:02d}:{mins:02d}:{secs:02d}" if hours else f"{mins:02d}:{secs:02d}"


def _minutes(seconds: int) -> str:
    minutes = max(1, math.ceil(seconds / 60))
    return f"{minutes} minute{'s' if minutes != 1 else ''}"


def human_size(num_bytes: Optional[float]) -> str:
    """Readable file size."""
    if num_bytes is None:
        return "?"
    size = float(num_bytes)
    for unit in ("B", "KB", "MB"):
        if size < 1024:
            return f"{size:.1f} {unit}" if unit == "MB" and size < 10 else f"{size:.0f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def _draw_bar(done: float, total: Optional[float], length: int = _BAR_LENGTH) -> str:
    if not total or total <= 0:
        return "▱" * length
    fraction = min(max(done / total, 0.0), 1.0)
    filled = round(fraction * length)
    return "▰" * filled + "▱" * (length - filled)


# -- Commands -----------------------------------------------------------------

def start() -> str:
    return (
        f"{e('WELCOME')} <b>Welcome to {escape(BOT_NAME)}!</b>\n\n"
        f"{e('LINK')} Send me any video link from YouTube, TikTok, Instagram, Twitter, etc., "
        "and I will send it back to you in high quality video or audio (MP3/FLAC)!\n\n"
        f"Just paste your link below {e('POINT_DOWN')}"
    )


def commands() -> list[tuple[str, str]]:
    return [
        ("start", "Start the bot"),
        ("help", "Help and features"),
    ]


def help_text() -> str:
    limit_line = (
        f"• Limit: Up to {USER_DOWNLOADS_PER_HOUR} downloads per hour\n"
        if USER_DOWNLOADS_PER_HOUR > 0 else ""
    )
    return (
        f"{e('QUESTION')} <b>How to use</b>\n\n"
        "1. Send or paste any video link.\n"
        "2. Choose your quality, or select MP3/FLAC for audio.\n"
        "3. Watch live progress and receive your file directly in this chat!\n\n"
        f"{e('INFO')} <b>Features</b>\n"
        f"• Direct delivery of files up to <b>{round(downloader.upload_limit / 1024**3)} GB</b>\n"
        "• Previously downloaded videos arrive instantly via smart cache\n"
        f"{limit_line}\n"
        f"{e('KEYBOARD')} <b>Commands</b>\n"
        "/start — Start the bot\n"
        "/help — How to use"
    )


def no_link() -> str:
    return f"{e('LINK')} Send me a valid video link and I'll download it for you."


def fetching() -> str:
    return f"{e('SEARCH')} <b>Scanning media info…</b>"


_QUEUE_WAIT = {
    "youtube": "YouTube request queue",
    "info": "a lookup slot",
    "download": "a download slot",
}


def queued(queue: str, position: int) -> str:
    return f"{e('HOURGLASS')} <b>Queued for {_QUEUE_WAIT.get(queue, 'processing')}</b> (Position: #{position})…"


def choose(info: VideoInfo) -> str:
    duration = f" | ⏱️ {_duration(info.duration)}" if info.duration else ""
    return (
        f"🎬 <b>{_title(info.title)}</b>{duration}\n\n"
        f"Select your download format {e('POINT_DOWN')}"
    )


def downloading(title: str, label: str) -> str:
    return (
        f"📥 <b>DOWNLOADING MEDIA</b>\n"
        f"🎬 <b>Title:</b> <i>{_title(title)}</i>\n"
        f"🎯 <b>Format:</b> <code>{escape(label)}</code>\n\n"
        f"<code>[{_draw_bar(0, 100)}] 0%</code>\n\n"
        f"⚡ <i>Initializing connection…</i>"
    )


def uploading(title: str) -> str:
    return (
        f"🚀 <b>UPLOADING TO TELEGRAM</b>\n"
        f"🎬 <b>Title:</b> <i>{_title(title)}</i>\n\n"
        f"<code>[{_draw_bar(0, 100)}] 0%</code>\n\n"
        f"⚡ <i>Preparing MTProto stream…</i>"
    )


# -- Creative Progress Dashboard ---------------------------------------------

def progress(title: str, label: str, report: Progress) -> str:
    """Creative, dynamic visual dashboard for download/processing/upload."""
    if report.stage == "upload":
        header = "🚀 <b>UPLOADING TO TELEGRAM</b>"
        icon_type = "📦"
    elif report.stage == "process":
        header = f"⚙️ <b>{escape(report.detail or 'PROCESSING MEDIA')}</b>"
        icon_type = "⏱️"
    else:
        part_str = f" (Part {report.part}/{report.parts})" if report.parts > 1 else ""
        header = f"📥 <b>DOWNLOADING MEDIA{part_str}</b>"
        icon_type = "📦"

    bar = _draw_bar(report.done, report.total)

    if report.total and report.total > 0:
        percent = min(max(report.done / report.total, 0.0), 1.0) * 100
        progress_line = f"<code>[{bar}] {percent:.1f}%</code>"
        if report.stage == "process":
            size_str = f"{_duration(report.done)} / {_duration(report.total)}"
        else:
            size_str = f"{human_size(report.done)} / {human_size(report.total)}"
    elif report.done:
        progress_line = f"<code>[{bar}]</code>"
        size_str = f"{human_size(report.done)} processed"
    else:
        progress_line = f"<code>[{bar}] Starting...</code>"
        size_str = "Calculating..."

    stats = []
    if report.speed and report.stage != "process":
        stats.append(f"⚡ <b>Speed:</b> {human_size(report.speed)}/s")
    if report.eta is not None:
        stats.append(f"⏱️ <b>ETA:</b> {_duration(report.eta)}")

    stats_block = "\n".join(stats) if stats else "⚡ <b>Status:</b> Streaming data…"

    return (
        f"{header}\n"
        f"🎬 <b>Title:</b> <i>{_title(title, 80)}</i>\n"
        f"🎯 <b>Format:</b> <code>{escape(label)}</code>\n\n"
        f"{progress_line}\n\n"
        f"{icon_type} <b>Size:</b> {size_str}\n"
        f"{stats_block}"
    )


def done(title: str) -> str:
    return f"{e('CHECK')} <b>Complete!</b> Enjoy <b>{_title(title)}</b> {e('ENJOY')}"


def done_instantly(title: str) -> str:
    return f"{e('ZAP')} <b>Instant Delivery!</b> <b>{_title(title)}</b> was cached in high speed."


def caption(title: str, is_audio: bool) -> str:
    return f"{e('MP3') if is_audio else e('VIDEO')} {_title(title, CAPTION_TITLE_LIMIT)}"


def cancelling() -> str:
    return "Cancelling download…"


def cancelled(title: str) -> str:
    return f"{e('CROSS')} Cancelled <b>{_title(title)}</b>. Choose another format or send a new link."


# -- Errors ------------------------------------------------------------------

def error(message: str) -> str:
    return f"{e('CROSS')} {escape(message)}"


def menu_expired() -> str:
    return f"{e('WARNING')} This menu has expired. Please send the link again."


def restarting() -> str:
    return f"{e('WARNING')} The service is refreshing. Please resend the link in a moment."


def lookup_limit(wait_seconds: int) -> str:
    return f"{e('HOURGLASS')} Lookup limit reached. Please wait {_minutes(wait_seconds)}."


def busy_alert() -> str:
    return "⏳ You already have an active download in progress."


def download_limit_alert(wait_seconds: Optional[int]) -> str:
    return f"⏳ Download limit reached. Try again in {_minutes(wait_seconds or 60)}."


def nothing_to_cancel() -> str:
    return "This job has already finished."


def not_your_download() -> str:
    return "Only the person who initiated this download can cancel it."