# mxm_datakraken/sources/fca_firds/file_index.py
from __future__ import annotations

import dataclasses as dc
import time
from typing import Any, Optional

import requests

API_URL: str = "https://api.data.fca.org.uk/fca_data_firds_files"
UA: str = "mxm-datakraken/0.1 (+https://moneyexmachina.com)"


# ------------------------------
# Dataclass for results
# ------------------------------


@dc.dataclass(frozen=True, slots=True)
class FirdsFile:
    """Metadata record for a published FCA FIRDS file."""

    file_type: str  # e.g. "FULINS", "DLTINS", "FULCAN"
    file_name: str  # e.g. "FULINS_C_20250927_01of01.zip"
    publication_date: str  # YYYY-MM-DD
    download_link: str  # full URL to .zip file


# ------------------------------
# Internal helpers
# ------------------------------


def _request_with_backoff(
    params: dict[str, str],
    max_tries: int = 5,
    timeout: int = 60,
) -> dict[str, Any]:
    """
    Send GET to FCA FIRDS API with exponential backoff on 429/5xx.

    Args:
        params: Query parameters for the API.
        max_tries: Maximum number of attempts before failing.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        RuntimeError: If all attempts fail.
    """
    headers = {"User-Agent": UA}
    delay = 1.0
    resp: requests.Response | None = None
    for _ in range(1, max_tries + 1):
        resp = requests.get(API_URL, params=params, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(delay)
            delay = min(delay * 2, 16)
            continue
        resp.raise_for_status()
    if resp is None:
        raise RuntimeError("Unexpected: no request attempts were made.")
    raise RuntimeError(
        f"FCA API failed after {max_tries} tries (last={resp.status_code})"
    )


def _build_query(
    file_type: str,
    start_date: str,
    end_date: str,
    file_name_wildcard: Optional[str] = None,
) -> str:
    """
    Build ElasticSearch-style query string for FCA FIRDS API.

    Args:
        file_type: One of "FULINS", "DLTINS", "FULCAN".
        start_date: Start of publication_date range (YYYY-MM-DD).
        end_date: End of publication_date range (YYYY-MM-DD).
        file_name_wildcard: Optional filename filter, e.g. "FULINS_C_*".

    Returns:
        Query string usable in FCA FIRDS API.
    """
    clauses: list[str] = [
        f"(file_type:{file_type})",
        f"(publication_date:[{start_date} TO {end_date}])",
    ]
    if file_name_wildcard:
        clauses.append(f"(file_name:{file_name_wildcard})")
    return "(" + " AND ".join(clauses) + ")"


# ------------------------------
# Public API
# ------------------------------


def discover_files(
    file_type: str,
    start_date: str,
    end_date: str,
    file_name_wildcard: str | None = None,
    size: int = 1000,
    sort: str = "publication_date:desc",
    from_idx: int = 0,
) -> list[FirdsFile]:
    """
    Discover FCA FIRDS files between start_date and end_date (YYYY-MM-DD).

    Args:
        file_type: One of "FULINS", "DLTINS", "FULCAN".
        start_date: Start of publication_date range (YYYY-MM-DD).
        end_date: End of publication_date range (YYYY-MM-DD).
        file_name_wildcard: Optional filename filter, e.g. "FULINS_C_*".
        size: Number of results to request (default 1000).
        sort: Sort order for results (default newest first).
        from_idx: Starting offset for pagination.

    Returns:
        List of discovered FIRDS files.
    """
    q = _build_query(file_type, start_date, end_date, file_name_wildcard)
    params: dict[str, str] = {
        "q": q,
        "from": str(from_idx),
        "size": str(size),
        "pretty": "true",
        "sort": sort,
    }
    data = _request_with_backoff(params)
    hits = data.get("hits", {}).get("hits", [])
    out: list[FirdsFile] = []
    for h in hits:
        src = h.get("_source", {})
        if {"download_link", "file_type", "file_name", "publication_date"} <= set(src):
            out.append(
                FirdsFile(
                    file_type=str(src["file_type"]),
                    file_name=str(src["file_name"]),
                    publication_date=str(src["publication_date"])[:10],
                    download_link=str(src["download_link"]),
                )
            )
    return out


def discover_latest_publication_date(file_type: str = "FULINS") -> Optional[str]:
    """
    Get the latest available publication_date for a given file_type.

    Args:
        file_type: One of "FULINS", "DLTINS", "FULCAN".

    Returns:
        Latest publication date as YYYY-MM-DD, or None if not found.
    """
    params: dict[str, str] = {
        "q": f"(file_type:{file_type})",
        "from": "0",
        "size": "1",
        "pretty": "true",
        "sort": "publication_date:desc",
    }
    data = _request_with_backoff(params)
    hits = data.get("hits", {}).get("hits", [])
    if not hits:
        return None
    return str(hits[0]["_source"]["publication_date"])[:10]


def discover_latest_full_etf_bucket() -> list[FirdsFile]:
    """
    Fetch the latest 'C' bucket of FULINS files (contains ETFs).
    Actual ETF filtering happens during XML parsing.

    Returns:
        List of FIRDS files for the 'C' bucket on the latest publication date.
    """
    pub_date = discover_latest_publication_date("FULINS")
    if not pub_date:
        return []
    return discover_files("FULINS", pub_date, pub_date, file_name_wildcard="FULINS_C_*")


# ------------------------------
# CLI test (manual use)
# ------------------------------

if __name__ == "__main__":
    latest = discover_latest_full_etf_bucket()
    for f in latest:
        print(f)
