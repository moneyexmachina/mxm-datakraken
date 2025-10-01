from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from mxm_datakraken.sources.justetf.discover import ETFProfile
from mxm_datakraken.sources.justetf.persistence import save_discovery_results


@pytest.fixture
def tmpdir_path(tmp_path: Path) -> Path:
    """Fixture to provide a temporary directory for file operations."""
    return tmp_path


def test_save_discovery_results(tmpdir_path: Path) -> None:
    """Ensure discovery results are saved correctly to JSON."""

    profiles: list[ETFProfile] = [
        {
            "isin": "TEST123",
            "url": "https://example.com/en/etf-profile.html?isin=TEST123",
            "lastmod": "2025-10-01",
        },
        {
            "isin": "TEST456",
            "url": "https://example.com/en/etf-profile.html?isin=TEST456",
            "lastmod": None,
        },
    ]

    snapshot_date: date = date(2025, 10, 1)
    outpath: Path = save_discovery_results(profiles, tmpdir_path, as_of=snapshot_date)

    # Check that snapshot file exists
    assert outpath.exists()
    data: list[ETFProfile] = json.loads(outpath.read_text(encoding="utf-8"))
    assert len(data) == 2
    assert data[0]["isin"] == "TEST123"

    # Check that latest.json also exists and matches
    latest: Path = tmpdir_path / "discovery" / "latest.json"
    assert latest.exists()
    data_latest: list[ETFProfile] = json.loads(latest.read_text(encoding="utf-8"))
    assert data_latest == data
