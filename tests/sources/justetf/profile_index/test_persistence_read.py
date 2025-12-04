"""
Tests for loading ETF Profile Index snapshots from disk (bucketed layout).
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pytest

from mxm_datakraken.sources.justetf.profile_index.discover import ETFProfileIndexEntry
from mxm_datakraken.sources.justetf.profile_index.persistence import (
    load_profile_index,
    save_profile_index,
)


@pytest.fixture
def index() -> list[ETFProfileIndexEntry]:
    return [
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


def test_load_latest(tmp_path: Path, index: List[ETFProfileIndexEntry]) -> None:
    """
    When no bucket is provided, loader should use the 'latest' pointer.
    """
    # First bucket
    save_profile_index(index, tmp_path, as_of_bucket="2025-10-01")
    # Second bucket (also becomes latest)
    save_profile_index(index, tmp_path, as_of_bucket="2025-10-05")

    loaded: List[ETFProfileIndexEntry] = load_profile_index(tmp_path)
    assert loaded == index


def test_load_exact_bucket(tmp_path: Path, index: List[ETFProfileIndexEntry]) -> None:
    """
    With an explicit as_of_bucket, load exactly that bucket without consulting 'latest'.
    """
    save_profile_index(index, tmp_path, as_of_bucket="2025-09-30")
    save_profile_index(index, tmp_path, as_of_bucket="2025-10-02")

    loaded: List[ETFProfileIndexEntry] = load_profile_index(
        tmp_path, as_of_bucket="2025-09-30"
    )
    assert loaded == index


def test_load_lexicographic_fallback_when_no_latest(
    tmp_path: Path, index: List[ETFProfileIndexEntry]
) -> None:
    """
    If no 'latest' pointer exists, loader should fall back to lexicographically
    last bucket.
    """
    # Write two buckets but do NOT update 'latest'
    save_profile_index(index, tmp_path, as_of_bucket="2025-09-30", write_latest=False)
    save_profile_index(index, tmp_path, as_of_bucket="2025-10-02", write_latest=False)

    loaded: List[ETFProfileIndexEntry] = load_profile_index(tmp_path)
    # Lexicographically last is "2025-10-02"
    assert loaded == index


def test_load_no_buckets_raises(tmp_path: Path) -> None:
    """
    If profile_index/ does not exist (or has no buckets), raise FileNotFoundError.
    """
    with pytest.raises(FileNotFoundError):
        _ = load_profile_index(tmp_path)


def test_missing_parsed_file_raises(
    tmp_path: Path, index: List[ETFProfileIndexEntry]
) -> None:
    """
    If a bucket exists but its parsed file is missing, raise FileNotFoundError.
    """
    # Create a valid bucket, then remove its parsed file
    out = save_profile_index(index, tmp_path, as_of_bucket="2025-10-10")
    out.unlink()  # remove profile_index.parsed.json

    with pytest.raises(FileNotFoundError):
        _ = load_profile_index(tmp_path, as_of_bucket="2025-10-10")
