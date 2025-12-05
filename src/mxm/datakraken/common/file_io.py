"""
Lightweight JSON file I/O helpers.

- All files are read/written as UTF-8.
- `write_json` pretty-prints with 2-space indentation and does not escape non-ASCII.
- Parent directories are created as needed.
- Functions surface underlying I/O and JSON errors (no silent swallowing).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from mxm.types import JSONLike

__all__ = ["write_json", "read_json"]


def write_json(path: Path, data: JSONLike) -> Path:
    """Write JSON to disk, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, ensure_ascii=False, indent=2)
    # newline ensures consistent line endings across platforms
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


def read_json(path: Path) -> JSONLike:
    """Read JSON from disk."""
    return cast(JSONLike, json.loads(path.read_text(encoding="utf-8")))
