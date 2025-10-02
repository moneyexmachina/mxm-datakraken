"""
Tests for persistence of ETF Profile Index snapshots.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from mxm_datakraken.sources.justetf.profile_index.discover import ETFProfileIndexEntry
from mxm_datakraken.sources.justetf.profile_index.persistence import save_profile_index


@pytest.fixture
def tmpdir_path(tmp_path: Path) -> Path:
    """Fixture to provide a temporary directory for file operations."""
    return tmp_path


def test_save_profile_index(tmpdir_path: Path) -> None:
    """Ensure profile index snapshots are saved correctly to JSON."""

    index: list[ETFProfileIndexEntry] = [
        {
            "isin": "TEST123",
            "url": "https://example.com/en/etf-profile.html?isin=TEST123",
            "lastmod": "2025-10-01",
        },
        {
            "isin": "TEST456",
            "url": "https://example.com/en/etf-profile.html?isin=TEST456",
        },
    ]

    snapshot_date: date = date(2025, 10, 1)
    outpath: Path = save_profile_index(index, tmpdir_path, as_of=snapshot_date)

    # Check that snapshot file exists
    assert outpath.exists()
    data: list[ETFProfileIndexEntry] = json.loads(outpath.read_text(encoding="utf-8"))
    assert len(data) == 2
    assert data[0]["isin"] == "TEST123"

    # Check that latest.json also exists and matches
    latest: Path = tmpdir_path / "profile_index" / "latest.json"
    assert latest.exists()
    data_latest: list[ETFProfileIndexEntry] = json.loads(
        latest.read_text(encoding="utf-8")
    )
    assert data_latest == data
