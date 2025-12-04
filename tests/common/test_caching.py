from __future__ import annotations

from datetime import date

from mxm_dataio.api import CacheMode

from mxm_datakraken.common.caching import (
    resolve_as_of_bucket,
    resolve_cache_mode,
)


def test_resolve_cache_mode_case_insensitive_and_default() -> None:
    assert resolve_cache_mode("ReVaLiDaTe") is CacheMode.REVALIDATE
    assert resolve_cache_mode(None) is CacheMode.DEFAULT


def test_resolve_as_of_bucket_format_and_literal() -> None:
    assert resolve_as_of_bucket("%Y-%m-%d") == date.today().strftime("%Y-%m-%d")
    assert resolve_as_of_bucket("2025Q4") == "2025Q4"
