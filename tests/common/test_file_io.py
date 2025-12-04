from __future__ import annotations

import json
from pathlib import Path

import pytest

from mxm.datakraken.common.file_io import read_json, write_json
from mxm.datakraken.common.types import JSONLike


def test_write_then_read_roundtrip(tmp_path: Path) -> None:
    data: JSONLike = {
        "name": "München",  # non-ASCII to check ensure_ascii=False
        "nums": [1, 2.5, None],
        "nested": {"ok": True, "more": ["α", "β"]},
    }
    out = tmp_path / "a" / "b" / "data.json"
    write_json(out, data)
    assert out.exists()

    loaded = read_json(out)
    assert loaded == data  # structural equality


def test_parent_directories_are_created(tmp_path: Path) -> None:
    out = tmp_path / "deep" / "nest" / "file.json"
    write_json(out, {"x": 1})
    assert out.exists()
    assert out.parent.is_dir()


def test_read_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "nope.json"
    with pytest.raises(FileNotFoundError):
        read_json(missing)


def test_write_unsupported_type_raises(tmp_path: Path) -> None:
    out = tmp_path / "bad.json"
    # sets are not JSON-serializable
    with pytest.raises(TypeError):
        write_json(out, {"bad": {1, 2, 3}})  # type: ignore[arg-type]


def test_pretty_print_and_utf8(tmp_path: Path) -> None:
    data: JSONLike = {"k": "ñ"}
    out = tmp_path / "pp.json"
    write_json(out, data)

    # Read raw text to inspect formatting/encoding
    raw = out.read_text(encoding="utf-8")
    # 2-space indent means the value line starts with exactly two spaces
    assert '\n  "k": "ñ"\n' in raw
    # ensure_ascii=False: non-ASCII should appear as-is (not \u00f1)
    assert "ñ" in raw
    assert "\\u00f1" not in raw

    # Also confirm it parses as JSON
    assert json.loads(raw) == data
