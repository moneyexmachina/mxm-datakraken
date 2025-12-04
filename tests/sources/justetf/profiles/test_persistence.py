from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from mxm.datakraken.common.file_io import read_json
from mxm.datakraken.common.latest_bucket import resolve_latest_bucket
from mxm.datakraken.common.types import JSONLike
from mxm.datakraken.sources.justetf.common.models import JustETFProfile
from mxm.datakraken.sources.justetf.profiles.persistence import (
    save_profile,
    save_profiles_snapshot,
)


@pytest.fixture
def sample_profile() -> JustETFProfile:
    return {
        "isin": "TEST123",
        "source_url": "https://example.com/en/etf-profile.html?isin=TEST123",
        "name": "Test ETF",
        "data": {"Index": "Test Index"},
        "description": "A sample ETF profile.",
    }


def test_save_profile_writes_file_bucketed(
    tmp_path: Path, sample_profile: JustETFProfile
) -> None:
    """Ensure save_profile writes to bucketed layout:
    <bucket>/<ISIN>/profile.parsed.json"""
    bucket = "2025-10-30"
    path: Path = save_profile(
        sample_profile, tmp_path, as_of_bucket=bucket, write_latest=False
    )

    # File exists
    assert path.exists()

    # Check bucketed path structure and filenames
    assert path.name == "profile.parsed.json"
    assert path.parent.name == "TEST123"
    assert path.parent.parent.name == bucket
    assert path.parent.parent.parent.name == "profiles"

    loaded = read_json(path)
    assert loaded["isin"] == "TEST123"
    assert loaded["name"] == "Test ETF"


def test_save_profile_missing_isin(tmp_path: Path) -> None:
    """Ensure save_profile raises ValueError if no ISIN."""
    bad_profile: dict[str, Any] = {
        "name": "No ISIN",
        "source_url": "https://example.com/en/etf-profile.html?isin=NOPE",
    }
    with pytest.raises(ValueError):
        _ = save_profile(
            cast(JustETFProfile, bad_profile), tmp_path, as_of_bucket="2025-10-30"
        )


def test_save_profiles_snapshot_bucketed(
    tmp_path: Path, sample_profile: JustETFProfile
) -> None:
    """
    Ensure snapshot writes aggregate to <profiles>/<bucket>/profiles.parsed.json
    and updates the 'latest' pointer.
    """
    bucket = "2025-10-01"
    profiles: list[JustETFProfile] = [sample_profile]

    filepath: Path = save_profiles_snapshot(
        cast(JSONLike, profiles),
        tmp_path,
        as_of_bucket=bucket,
        write_latest=True,
    )

    # Aggregate file exists in bucket
    assert filepath.exists()
    assert filepath.name == "profiles.parsed.json"
    assert filepath.parent.name == bucket
    assert filepath.parent.parent.name == "profiles"

    # Latest pointer resolves to the same bucket (symlink or file fallback)
    profiles_root = tmp_path / "profiles"
    resolved = resolve_latest_bucket(profiles_root)
    assert resolved == bucket

    # Content sanity check
    loaded_latest = json.loads(filepath.read_text(encoding="utf-8"))
    assert loaded_latest[0]["isin"] == "TEST123"
