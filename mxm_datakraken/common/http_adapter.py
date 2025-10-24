"""
HTTP adapter for mxm-datakraken (requests-based).

This module provides a thin adapter that implements the mxm-dataio ``Fetcher``
protocol for issuing HTTP requests and returning a transport-level result.
The adapter focuses on performing a single request with sensible defaults
(e.g., User-Agent, Accept headers). Higher-level behaviors (retry, backoff,
politeness delays, caching, auditing) are handled by mxm-dataio sessions
and/or the calling code.

Design notes
------------
- Headers are stored and exposed as ``Mapping[str, str]`` (no ``bytes`` values).
- Default headers and timeout are injected at construction time.
- Network/client exceptions are propagated; mxm-dataio is responsible for
  recording failures at the session boundary.

Thread-safety
-------------
This adapter does not guarantee thread safety. Use one instance per worker or
provide synchronization if you share an underlying ``requests.Session``.
"""

from __future__ import annotations

from datetime import timedelta
from types import MappingProxyType
from typing import Any, Mapping, Optional

import requests
from mxm_dataio.adapters import Fetcher
from mxm_dataio.models import AdapterResult, Request
from requests import HTTPError, Response, Session


def _elapsed_ms(resp: Response) -> Optional[int]:
    """Return the elapsed time in milliseconds for a ``requests.Response``.

    If the response has no timing information, returns ``None``.
    """
    elapsed: Optional[timedelta] = getattr(resp, "elapsed", None)
    if elapsed is None:
        return None
    return int(round(elapsed.total_seconds() * 1000.0))


def _headers_dict(headers: Mapping[str, Any]) -> dict[str, str]:
    """Convert a headers mapping to ``dict[str, str]`` with string values only.

    Any non-string values are coerced to ``str`` to satisfy type expectations
    in downstream consumers and to keep persistence JSON-friendly.
    """
    return {str(k): str(v) for k, v in headers.items()}


class HttpRequestsAdapter(Fetcher):
    """Requests-based HTTP adapter implementing the mxm-dataio ``Fetcher`` protocol.

    The adapter issues a single HTTP request using an internal
    ``requests.Session`` and returns the raw body and transport metadata.
    Default headers and timeout are configured at construction; per-request
    overrides are provided via the ``Request.params`` mapping.

    Parameters
    ----------
    user_agent:
        String for the ``User-Agent`` header. Will be inserted into default
        headers if not already present.
    default_timeout:
        Timeout in seconds applied when a per-request timeout is not supplied.
    default_headers:
        Mapping of default headers applied to all requests. All values must be
        ``str``; header names are handled case-insensitively by the HTTP stack.

    Notes
    -----
    - This class returns an ``AdapterResult`` containing bytes and transport
      metadata (content type, status, headers, URL, elapsed).
    - Retries, rate limiting (politeness), and audit logging are not the
      responsibility of this adapter; they belong in mxm-dataio or orchestrators.
    """

    source: str = "http"
    # Clarify attribute type for Pyright
    default_headers: Mapping[str, str]

    def __init__(
        self,
        *,
        user_agent: str = "mxm-datakraken/0.2 (contact@moneyexmachina.com)",
        default_timeout: float = 30.0,
        default_headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        """Initialize the adapter with default headers and timeout."""
        self._session: Session = requests.Session()

        # Set base defaults; allow caller overrides.
        base: dict[str, str] = {
            "User-Agent": user_agent,
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        if default_headers:
            base.update(default_headers)

        # Apply to requests session (requests merges per-request headers at call).
        self._session.headers.update(base)

        self._default_timeout = float(default_timeout)

        # Coerce to str-only values, then freeze for read-only exposure (Pyright-safe).
        coerced: dict[str, str] = _headers_dict(self._session.headers)
        self.default_headers = MappingProxyType(coerced)

    def fetch(self, request: Request) -> AdapterResult:
        """Perform an HTTP request described by ``Request.params`` and return the result.

        Expected ``request.params`` keys
        --------------------------------
        url : str
            Absolute URL to request (required).
        method : str, default "GET"
            HTTP method to use.
        headers : Mapping[str, str] | None
            Per-request headers merged over the adapter's default headers.
        timeout : float | int | None
            Per-request timeout in seconds; falls back to the adapter default.
        body : bytes | str | None
            Optional request body for methods like POST/PUT/PATCH. Strings are
            UTF-8 encoded.
        allow_redirects : bool | None
            Whether to follow redirects; defaults to ``True`` if not provided.

        Returns
        -------
        AdapterResult
            Metadata-rich result containing raw bytes and transport details.

        Raises
        ------
        ValueError
            If ``url`` is missing or empty.
        requests.HTTPError
            If the response status indicates an HTTP error (4xx/5xx).
        """
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
            # Optional polish: pass only per-request headers; requests will merge with session defaults.
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
            # Surface HTTP errors unchanged; DataIO can record them at the boundary.
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
        """Human-readable description of this adapter (for logs and diagnostics)."""
        return "Generic HTTP adapter via 'requests' (mxm-dataio Fetcher implementation)"

    def close(self) -> None:
        """Close the underlying HTTP session and suppress any client shutdown errors."""
        try:
            self._session.close()
        except Exception:
            pass
