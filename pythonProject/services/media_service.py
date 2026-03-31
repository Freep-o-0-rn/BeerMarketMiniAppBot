from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Optional

from aiogram import Bot

_SAFE_EXT = re.compile(r"^[a-zA-Z0-9]{1,8}$")


class MediaService:
    def __init__(self, media_root: Path):
        self.media_root = media_root
        self.media_root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _safe_suffix(filename: Optional[str], fallback: str = "bin") -> str:
        suffix = (Path(filename).suffix or "").lstrip(".").lower()
        if suffix and _SAFE_EXT.match(suffix):
            return suffix
        return fallback

    async def save_telegram_file(self, bot: Bot, file_id: str, original_name: Optional[str], *, subdir: str) -> Path:
        tg_file = await bot.get_file(file_id)
        suffix = self._safe_suffix(original_name or tg_file.file_path, fallback="dat")
        folder = self.media_root / subdir
        folder.mkdir(parents=True, exist_ok=True)
        filename = f"{uuid.uuid4().hex}.{suffix}"
        out_path = folder / filename
        await bot.download_file(tg_file.file_path, destination=out_path)
        return out_path

    def delete_file_safe(self, path_str: str) -> bool:
        path = Path(path_str)
        try:
            path_resolved = path.resolve()
            root_resolved = self.media_root.resolve()
        except FileNotFoundError:
            return False
        if root_resolved not in path_resolved.parents and path_resolved != root_resolved:
            return False
        if path_resolved.exists() and path_resolved.is_file():
            path_resolved.unlink(missing_ok=True)
            return True
        return False
