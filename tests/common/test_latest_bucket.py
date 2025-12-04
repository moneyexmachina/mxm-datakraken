from __future__ import annotations

import os
from pathlib import Path

import pytest

from mxm_datakraken.common.latest_bucket import (
    resolve_latest_bucket,
    update_latest_pointer,
)


def test_symlink_happy_path(tmp_path: Path) -> None:
    root = tmp_path
    (root / "2025-10-29").mkdir()
    (root / "2025-10-30").mkdir()

    # create latest -> 2025-10-29
    update_latest_pointer(root, "2025-10-29")
    latest = root / "latest"
    assert latest.is_symlink()
    assert os.readlink(latest) == "2025-10-29"
    assert resolve_latest_bucket(root) == "2025-10-29"

    # update to -> 2025-10-30 (replace symlink)
    update_latest_pointer(root, "2025-10-30")
    assert latest.is_symlink()
    assert os.readlink(latest) == "2025-10-30"
    assert resolve_latest_bucket(root) == "2025-10-30"


def test_existing_real_directory_latest_raises(tmp_path: Path) -> None:
    root = tmp_path
    (root / "bucketA").mkdir()
    (root / "latest").mkdir()  # real dir, not a symlink

    with pytest.raises(RuntimeError):
        update_latest_pointer(root, "bucketA")


def test_fallback_marker_when_symlink_not_supported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path
    (root / "bucketA").mkdir()

    # Force symlink creation to fail
    def _boom(_self: Path, _target: str) -> None:  # noqa: ARG001
        raise OSError("no symlink perms")

    monkeypatch.setattr(Path, "symlink_to", _boom, raising=True)

    update_latest_pointer(root, "bucketA")

    latest = root / "latest"
    assert not latest.exists()
    marker = root / "LATEST_BUCKET"
    assert marker.exists()
    assert marker.read_text(encoding="utf-8").strip() == "bucketA"
    assert resolve_latest_bucket(root) == "bucketA"

    # Change bucket; marker should be overwritten
    update_latest_pointer(root, "bucketB")
    assert marker.read_text(encoding="utf-8").strip() == "bucketB"
    assert resolve_latest_bucket(root) == "bucketB"


def test_broken_symlink_and_marker_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path

    # Create a symlink pointing to a non-existent target
    (root / "latest").symlink_to("ghost-bucket")
    # os.readlink() still returns "ghost-bucket" (no error), so we should get that
    assert resolve_latest_bucket(root) == "ghost-bucket"

    # If os.readlink() itself raises, we should fall back to marker (if present)
    (root / "LATEST_BUCKET").write_text("marker-bucket", encoding="utf-8")

    def _readlink_raises(_path: str) -> str:  # noqa: ARG001
        raise OSError("simulated readlink failure")

    monkeypatch.setattr(os, "readlink", _readlink_raises, raising=True)
    assert resolve_latest_bucket(root) == "marker-bucket"


def test_no_pointers_returns_none(tmp_path: Path) -> None:
    root = tmp_path
    assert resolve_latest_bucket(root) is None
