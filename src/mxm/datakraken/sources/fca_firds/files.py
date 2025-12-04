from __future__ import annotations

import dataclasses as dc
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Callable, Iterable, TypedDict

import requests

from .file_index import FirdsFile

UA: str = "mxm-datakraken/0.1 (+https://moneyexmachina.com)"
DEFAULT_CACHE_DIR: Path = Path.home() / ".mxm" / "cache" / "fca" / "firds"
MANIFEST_FILE: str = "manifest.json"


# ------------------------------
# Data model for manifest
# ------------------------------
class ManifestFileInfo(TypedDict):
    file_type: str
    publication_date: str
    download_link: str
    path: str
    sha256: str
    downloaded_at: str


class Manifest(TypedDict):
    files: dict[str, ManifestFileInfo]


@dc.dataclass
class CacheInfo:
    root: Path
    manifest_path: Path
    manifest: Manifest


def _load_cache(cache_dir: Path = DEFAULT_CACHE_DIR) -> CacheInfo:
    """Load or initialize cache manifest."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = cache_dir / MANIFEST_FILE
    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest: Manifest = json.load(f)
    else:
        manifest = {"files": {}}  # {file_name: metadata}
    return CacheInfo(root=cache_dir, manifest_path=manifest_path, manifest=manifest)


def _save_cache(info: CacheInfo) -> None:
    tmp = info.manifest_path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(info.manifest, f, indent=2, sort_keys=True)
    os.replace(tmp, info.manifest_path)


def _dest_path_for(f: FirdsFile, cache_root: Path) -> Path:
    """Map FirdsFile -> local cache path."""
    d = cache_root / f.file_type / f.publication_date
    d.mkdir(parents=True, exist_ok=True)
    return d / f.file_name


def is_cached(f: FirdsFile, cache_dir: Path = DEFAULT_CACHE_DIR) -> bool:
    """Check if a file is already in cache."""
    return _dest_path_for(f, cache_dir).exists()


def download_and_cache(
    f: FirdsFile,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    overwrite: bool = False,
    timeout: int = 120,
) -> Path:
    """
    Download a FIRDS file and cache it locally.
    Skips download if already cached (unless overwrite=True).
    Returns the local path.
    """
    info = _load_cache(cache_dir)
    dest = _dest_path_for(f, info.root)

    if dest.exists() and not overwrite:
        return dest

    headers = {"User-Agent": UA}
    r = requests.get(f.download_link, headers=headers, timeout=timeout, stream=True)
    r.raise_for_status()

    tmp = dest.with_suffix(".part")
    h = hashlib.sha256()
    with open(tmp, "wb") as out:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            out.write(chunk)
            h.update(chunk)
    os.replace(tmp, dest)

    digest = h.hexdigest()
    with open(dest.with_suffix(".sha256"), "w", encoding="utf-8") as sf:
        sf.write(digest + "\n")

    # Update manifest
    info.manifest["files"][f.file_name] = {
        "file_type": f.file_type,
        "publication_date": f.publication_date,
        "download_link": f.download_link,
        "path": str(dest),
        "sha256": digest,
        "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    _save_cache(info)
    return dest


def download_subset(
    files: Iterable[FirdsFile],
    cache_dir: Path = DEFAULT_CACHE_DIR,
    predicate: Callable[..., FirdsFile] | None = None,
) -> list[Path]:
    """
    Download a subset of FIRDS files into cache.
    - files: iterable of FirdsFile objects
    - predicate: optional function(FirdsFile) -> bool to filter which ones to download
    Returns list of local paths.
    """
    paths: list[Path] = []
    for f in files:
        if predicate and not predicate(f):
            continue
        p = download_and_cache(f, cache_dir=cache_dir, overwrite=False)
        paths.append(p)
    return paths
