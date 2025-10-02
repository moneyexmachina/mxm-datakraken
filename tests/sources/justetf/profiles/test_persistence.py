from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from mxm_datakraken.sources.justetf.profiles.model import JustETFProfile
from mxm_datakraken.sources.justetf.profiles.persistence import (
    save_profile,
    save_profiles_snapshot,
)


@pytest.fixture
def sample_profile() -> JustETFProfile:
    return {
        "isin": "TEST123",
        "name": "Test ETF",
        "data": {"Index": "Test Index"},
        "description": "A sample ETF profile.",
    }


def test_save_profile_writes_file(
    tmp_path: Path, sample_profile: dict[str, str]
) -> None:
    """Ensure save_profile writes a file named after ISIN."""
    path: Path = save_profile(sample_profile, tmp_path)
    assert path.exists()
    assert path.name == "TEST123.json"

    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["isin"] == "TEST123"
    assert loaded["name"] == "Test ETF"


def test_save_profile_missing_isin(tmp_path: Path) -> None:
    """Ensure save_profile raises ValueError if no ISIN."""
    bad_profile: dict[str, str] = {"name": "No ISIN"}
    with pytest.raises(ValueError):
        save_profile(bad_profile, tmp_path)


def test_save_profiles_snapshot(tmp_path: Path, sample_profile: dict[str, str]) -> None:
    """Ensure snapshot writes dated and latest files."""
    snapshot_date: date = date(2025, 10, 1)
    profiles = [sample_profile]

    filepath: Path = save_profiles_snapshot(profiles, tmp_path, as_of=snapshot_date)

    # Dated file exists
    assert filepath.exists()
    assert "profiles_2025-10-01.json" in str(filepath)

    # Latest file exists
    latest_path: Path = tmp_path / "profiles" / "latest.json"
    assert latest_path.exists()

    loaded_latest = json.loads(latest_path.read_text(encoding="utf-8"))
    assert loaded_latest[0]["isin"] == "TEST123"
