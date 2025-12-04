"""
Tests for persistence of ETF Profile Index snapshots (bucketed layout).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mxm.datakraken.common.latest_bucket import resolve_latest_bucket
from mxm.datakraken.sources.justetf.profile_index.discover import ETFProfileIndexEntry
from mxm.datakraken.sources.justetf.profile_index.persistence import save_profile_index


@pytest.fixture
def tmpdir_path(tmp_path: Path) -> Path:
    """Fixture to provide a temporary directory for file operations."""
    return tmp_path


def test_save_profile_index_bucketed(tmpdir_path: Path) -> None:
    """Ensure profile index snapshots are saved correctly under
    <bucket>/profile_index.parsed.json and 'latest' is updated."""
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

    bucket = "2025-10-01"
    outpath: Path = save_profile_index(index, tmpdir_path, as_of_bucket=bucket)

    # Check returned path and layout
    assert outpath.exists()
    assert outpath.name == "profile_index.parsed.json"
    assert outpath.parent.name == bucket
    assert outpath.parent.parent.name == "profile_index"

    # Content check
    data: list[ETFProfileIndexEntry] = json.loads(outpath.read_text(encoding="utf-8"))
    assert len(data) == 2
    assert data[0]["isin"] == "TEST123"

    # Latest pointer resolves to this bucket
    pi_root = tmpdir_path / "profile_index"
    latest_bucket = resolve_latest_bucket(pi_root)
    assert latest_bucket == bucket

    # Parsed file exists at the resolved latest bucket
    latest_parsed = pi_root / latest_bucket / "profile_index.parsed.json"
    assert latest_parsed.exists()


def test_save_profile_index_no_latest(tmpdir_path: Path) -> None:
    index: list[ETFProfileIndexEntry] = [{"isin": "ABC", "url": "https://x/abc"}]
    _ = save_profile_index(
        index, tmpdir_path, as_of_bucket="2025-10-02", write_latest=False
    )

    # No latest pointer should be resolvable when none exists
    pi_root = tmpdir_path / "profile_index"
    assert resolve_latest_bucket(pi_root) is None
