"""
Persistent cache of:
- video metadata (formats, titles, sizes)
- file_ids of previously uploaded videos for instant zero-download re-sending
"""

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class Store:
    def __init__(self, path: Path, max_files: int, video_ttl: int, file_ttl: int):
        self._path = path
        self._max_files = max_files
        self._video_ttl = video_ttl
        self._file_ttl = file_ttl
        self._lock = threading.RLock()
        self._videos: dict[str, dict] = {}
        self._aliases: dict[str, str] = {}
        self._files: dict[str, dict] = {}
        self._write_lock = threading.Lock()
        self._version = 0
        self._written_version = 0

    def load(self) -> None:
        try:
            content = self._path.read_text(encoding="utf-8").strip()
            if not content:
                return
            data = json.loads(content)
        except FileNotFoundError:
            return
        except (OSError, ValueError) as e:
            logger.warning("Resetting unreadable cache file %s: %s", self._path, e)
            return

        with self._lock:
            self._videos = data.get("videos") or {}
            self._aliases = data.get("aliases") or {}
            self._files = data.get("files") or {}
            self._prune_videos()
            logger.info("Cache loaded: %d video metadata, %d cached file_ids", len(self._videos), len(self._files))

    # -- Video Details --------------------------------------------------------

    def video(self, key: str) -> Optional[dict]:
        with self._lock:
            entry = self._videos.get(self._aliases.get(key, key))
            if entry and time.time() - entry["saved"] < self._video_ttl:
                return entry["info"]
            return None

    def put_video(self, info: dict, alias: Optional[str] = None) -> None:
        with self._lock:
            key = info["key"]
            self._videos[key] = {"info": info, "saved": time.time()}
            if alias and alias != key:
                self._aliases[alias] = key
            self._prune_videos()
            snapshot = self._snapshot()
        self._write(snapshot)

    # -- Cached File IDs ------------------------------------------------------

    def file(self, key: str, label: str) -> Optional[tuple[str, str]]:
        """
        Return (file_id, file_type) if cached and valid.
        file_type is 'video', 'audio', or 'document'.
        """
        with self._lock:
            name = f"{key}|{label}"
            entry = self._files.get(name)
            if entry is None or time.time() - entry["saved"] >= self._file_ttl:
                return None
            self._files[name] = self._files.pop(name)
            return entry.get("file_id"), entry.get("file_type", "video")

    def put_file(self, key: str, label: str, file_id: str, file_type: str = "video") -> None:
        """Cache a sent file's Telegram file_id for instant redelivery."""
        with self._lock:
            name = f"{key}|{label}"
            self._files[name] = {
                "file_id": file_id,
                "file_type": file_type,
                "saved": time.time(),
            }
            while len(self._files) > self._max_files:
                self._files.pop(next(iter(self._files)))
            snapshot = self._snapshot()
        self._write(snapshot)

    def drop_file(self, key: str, label: str) -> None:
        with self._lock:
            name = f"{key}|{label}"
            if name in self._files:
                del self._files[name]
                snapshot = self._snapshot()
                self._write(snapshot)

    # -- Internals ------------------------------------------------------------

    def _prune_videos(self) -> None:
        cutoff = time.time() - self._video_ttl
        self._videos = {k: v for k, v in self._videos.items() if v["saved"] >= cutoff}
        self._aliases = {a: k for a, k in self._aliases.items() if k in self._videos}

    def _snapshot(self) -> tuple[int, str]:
        self._version += 1
        data = {
            "videos": self._videos,
            "aliases": self._aliases,
            "files": self._files,
        }
        return self._version, json.dumps(data, ensure_ascii=False)

    def _write(self, snapshot: tuple[int, str]) -> None:
        version, payload = snapshot
        with self._write_lock:
            if version <= self._written_version:
                return
            tmp_path = self._path.with_name(self._path.name + ".tmp")
            try:
                tmp_path.write_text(payload, encoding="utf-8")
                try:
                    os.chmod(tmp_path, 0o600)
                except OSError:
                    pass
                os.replace(tmp_path, self._path)
                self._written_version = version
            except OSError as e:
                logger.warning("Could not save cache file %s: %s", self._path, e)