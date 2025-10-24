"""
Downloader for justETF profiles (DataIO-backed).

This module fetches the raw HTML of a justETF profile page via mxm-dataio.

Public API
----------
- download_etf_profile_html(cfg, isin, url, timeout=30) -> tuple[str, IoResponse]
    Returns the decoded HTML and the DataIO `Response` for provenance.

Notes
-----
All I/O is cached and audited through mxm-dataio using the per-source
DataIO configuration derived from the application config.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from mxm_config import MXMConfig
from mxm_dataio.api import DataIoSession
from mxm_dataio.models import Response as IoResponse

from mxm_datakraken.config.config import dataio_for_justetf


def _response_bytes(resp: IoResponse) -> bytes:
    """
    Read payload bytes from a DataIO Response (via resp.path) and verify checksum.

    Raises
    ------
    ValueError
        If the response has no payload path or if checksum verification fails.
    """
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

    Parameters
    ----------
    cfg
        Resolved application config. Only the justETF DataIO section is used.
    isin
        ISIN of the ETF. Currently informational (reserved for logging/debugging).
    url
        Absolute profile URL to fetch.
    timeout
        Timeout in seconds to pass to the HTTP adapter.

    Returns
    -------
    tuple[str, IoResponse]
        A pair of (HTML text decoded as UTF-8 with replacement, DataIO Response).

    Raises
    ------
    ValueError
        If the DataIO response has no payload path or fails checksum verification.
    Exception
        Any exception propagated from DataIoSession or the registered adapter.
    """
    _ = isin
    params: dict[str, Any] = {
        "url": url,
        "method": "GET",
        "headers": {"Accept": "text/html"},
        "timeout": float(timeout),
    }
    alias, dio_cfg = dataio_for_justetf(cfg)
    with DataIoSession(source=alias, cfg=dio_cfg, use_cache=True) as io:
        resp = io.fetch(io.request(kind="profile_html", params=params))
        html = _response_bytes(resp).decode("utf-8", errors="replace")
        return html, resp
