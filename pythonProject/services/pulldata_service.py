from __future__ import annotations

import json
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, List, Tuple


DEFAULT_PULLDATA_MANIFEST: Dict[str, List[str]] = {
    "include": [
        "logs",
        "Price",
        "settings",
        "data",
        "promos",
        ".env",
        "news.json",
        "webapp/news.json",
    ],
    "exclude": [
        "__pycache__",
        "*.pyc",
        "*.tmp",
    ],
}


@dataclass(frozen=True)
class PullDataArchiveResult:
    archive_path: Path
    files_count: int
    include_count: int
    exclude_count: int
    missing_paths: List[str]


def load_manifest(manifest_path: Path, logger) -> Dict[str, List[str]]:
    if not manifest_path.exists():
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(DEFAULT_PULLDATA_MANIFEST, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return dict(DEFAULT_PULLDATA_MANIFEST)
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("pulldata: cannot read manifest, fallback to defaults")
        return dict(DEFAULT_PULLDATA_MANIFEST)
    include = [str(x).strip() for x in data.get("include", []) if str(x).strip()]
    exclude = [str(x).strip() for x in data.get("exclude", []) if str(x).strip()]
    if not include:
        include = list(DEFAULT_PULLDATA_MANIFEST["include"])
    return {"include": include, "exclude": exclude}


def _match_any(rel_path: str, patterns: List[str]) -> bool:
    normalized = rel_path.replace("\\", "/")
    name = Path(normalized).name
    for pattern in patterns:
        p = (pattern or "").strip().replace("\\", "/")
        if not p:
            continue
        if "/" in p:
            if fnmatch(normalized, p):
                return True
        else:
            if fnmatch(name, p) or fnmatch(normalized, p):
                return True
    return False


def build_archive(root_dir: Path, manifest_path: Path, tz, logger) -> PullDataArchiveResult:
    root = root_dir.resolve()
    manifest = load_manifest(manifest_path, logger)
    include = manifest.get("include", [])
    exclude = manifest.get("exclude", [])
    missing: List[str] = []
    files: Dict[str, Path] = {}

    for item in include:
        rel = item.strip().replace("\\", "/")
        target = (root / rel).resolve()
        if not str(target).startswith(str(root)):
            logger.warning("pulldata: skipped path outside root: %s", rel)
            continue
        if not target.exists():
            missing.append(rel)
            continue
        if target.is_file():
            rel_file = target.relative_to(root).as_posix()
            if not _match_any(rel_file, exclude):
                files[rel_file] = target
            continue
        for file_path in target.rglob("*"):
            if not file_path.is_file():
                continue
            rel_file = file_path.relative_to(root).as_posix()
            if not _match_any(rel_file, exclude):
                files[rel_file] = file_path

    stamp = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
    tmp_dir = Path(tempfile.mkdtemp(prefix="pulldata_"))
    archive_path = tmp_dir / f"pulldata_{stamp}.zip"
    with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for rel, src in sorted(files.items()):
            zf.write(src, arcname=rel)
    return PullDataArchiveResult(
        archive_path=archive_path,
        files_count=len(files),
        include_count=len(include),
        exclude_count=len(exclude),
        missing_paths=missing,
    )
