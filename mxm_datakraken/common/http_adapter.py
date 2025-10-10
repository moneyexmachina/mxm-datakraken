from __future__ import annotations

from datetime import timedelta
from time import perf_counter
from types import MappingProxyType
from typing import Any, Mapping, Optional

import requests
from mxm_dataio.adapters import AdapterResult, Fetcher
from mxm_dataio.models import Request
from requests import HTTPError, Response, Session


def _elapsed_ms(resp: Response) -> Optional[int]:
    elapsed: Optional[timedelta] = getattr(resp, "elapsed", None)
    if elapsed is None:
        return None
    return int(round(elapsed.total_seconds() * 1000.0))


def _headers_dict(headers: Mapping[str, Any]) -> dict[str, str]:
    return {str(k): str(v) for k, v in headers.items()}


class HttpRequestsAdapter(Fetcher):
    """
    Generic HTTP adapter (requests-based) implementing the mxm-dataio Fetcher protocol.

    Expected Request.params:
      - url: str (required)
      - method: str = "GET"
      - headers: Mapping[str, str] | None
      - timeout: float | int | None
      - body: bytes | str | None       # for POST/PUT/PATCH
      - allow_redirects: bool | None   # default True
    """

    source: str = "http"

    def __init__(
        self,
        *,
        user_agent: str = "mxm-datakraken/0.2 (contact@moneyexmachina.com)",
        default_timeout: float = 30.0,
        default_headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._session: Session = requests.Session()
        base = {
            "User-Agent": user_agent,
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        if default_headers:
            base.update(default_headers)
        self._session.headers.update(base)
        self._default_timeout = float(default_timeout)
        self.default_headers: Mapping[str, str] = MappingProxyType(
            dict(self._session.headers)
        )

    def fetch(self, request: Request) -> AdapterResult | bytes:
        params: Mapping[str, Any] = request.params or {}
        url = params.get("url")
        if not isinstance(url, str) or not url:
            raise ValueError(
                "HttpRequestsAdapter.fetch: request.params['url'] must be a non-empty string."
            )

        method = str(params.get("method") or "GET").upper()
        extra_headers: Mapping[str, str] = params.get("headers") or {}
        timeout = float(params.get("timeout") or self._default_timeout)
        allow_redirects = (
            True
            if params.get("allow_redirects") is None
            else bool(params["allow_redirects"])
        )

        body = params.get("body")
        if isinstance(body, str):
            body = body.encode("utf-8")

        try:
            resp: Response = self._session.request(
                method=method,
                url=url,
                headers=dict(extra_headers),
                timeout=timeout,
                data=body,
                allow_redirects=allow_redirects,
            )
            resp.raise_for_status()
        except HTTPError:
            raise

        return AdapterResult(
            data=resp.content,
            content_type=resp.headers.get("Content-Type"),
            transport_status=resp.status_code,
            url=resp.url,
            headers=_headers_dict(resp.headers),
            elapsed_ms=_elapsed_ms(resp),
        )

    def describe(self) -> str:
        return "Generic HTTP adapter via 'requests' (mxm-dataio Fetcher implementation)"

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass
