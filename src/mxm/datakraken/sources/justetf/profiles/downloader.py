"""
Downloader for justETF profiles (DataIO-backed).

This module fetches the raw HTML of a justETF profile page via mxm-dataio.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from mxm_config import MXMConfig
from mxm_dataio.models import Response as IoResponse
from mxm_dataio.types import RequestParams

from mxm.datakraken.sources.justetf.common.io import open_justetf_session


def _response_bytes(resp: IoResponse) -> bytes:
    """Read payload bytes from a DataIO Response (via resp.path) and verify checksum."""
    if not resp.path:
        raise ValueError("DataIO Response has no payload path")
    data = Path(resp.path).read_bytes()
    if resp.checksum and not resp.verify(data):
        raise ValueError("DataIO Response checksum mismatch")
    return data


def download_etf_profile_html(
    cfg: MXMConfig,
    isin: str,
    url: str,
    timeout: float | int = 30,
) -> tuple[str, IoResponse]:
    """
    Fetch a justETF profile page via mxm-dataio and return (HTML, Response).
    """
    _ = isin  # reserved for logging/debug
    params = cast(
        RequestParams,
        {
            "url": url,
            "method": "GET",
            "headers": {"Accept": "text/html"},
            "timeout": float(timeout),
        },
    )
    with open_justetf_session(cfg) as io:
        resp = io.fetch(io.request(kind="profile_html", params=params))
        html = _response_bytes(resp).decode("utf-8", errors="replace")
        return html, resp
