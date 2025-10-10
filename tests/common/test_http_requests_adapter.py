from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from types import MethodType
from typing import Any, Dict, Mapping, MutableMapping, Optional, cast

import pytest
from mxm_dataio.models import Request
from requests import HTTPError, Response, Session

from mxm_datakraken.common.http_adapter import HttpRequestsAdapter

# ----- helpers ---------------------------------------------------------------


def _req(kind: str, params: Mapping[str, Any]) -> Request:
    """Construct a Request with a fixed session_id for tests."""
    return Request(kind=kind, params=dict(params), session_id="test-session")


# ----- test doubles ----------------------------------------------------------


@dataclass
class _Call:
    method: str
    url: str
    headers: Dict[str, str]
    timeout: float | int | None
    data: bytes | None
    allow_redirects: bool


class _DummyResp:
    """Minimal Response-like object with the attributes we use."""

    def __init__(
        self,
        *,
        url: str = "https://example.test/resource",
        status: int = 200,
        content: bytes = b"ok",
        headers: Optional[Mapping[str, str]] = None,
        elapsed_ms: int = 123,
    ) -> None:
        self.url: str = url
        self.status_code: int = status
        self.content: bytes = content
        self.headers: MutableMapping[str, str] = dict(
            headers or {"Content-Type": "text/html; charset=utf-8"}
        )
        self.elapsed = timedelta(milliseconds=elapsed_ms)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}")

    # For typing compatibility when annotated as Response
    def __getattr__(self, name: str) -> Any:  # pragma: no cover - safety net
        raise AttributeError(name)


def _patch_request(
    adapter: HttpRequestsAdapter, dummy_response: _DummyResp, call_sink: list[_Call]
) -> None:
    """Monkeypatch Session.request to capture call args and return dummy response."""

    def _fake_request(  # type: ignore[override]
        self: Session,
        *,
        method: str,
        url: str,
        headers: Optional[Mapping[str, str]] = None,
        timeout: float | int | None = None,
        data: Optional[bytes] = None,
        allow_redirects: bool = True,
        **_: Any,
    ) -> Response:
        call_sink.append(
            _Call(
                method=method,
                url=url,
                headers=dict(headers or {}),
                timeout=timeout,
                data=data,
                allow_redirects=allow_redirects,
            )
        )
        return cast(Response, dummy_response)

    session = cast(Session, adapter._session)  # type: ignore[attr-defined]
    session.request = MethodType(_fake_request, session)


# ----- tests ----------------------------------------------------------------


def test_fetch_get_success() -> None:
    adapter = HttpRequestsAdapter()
    calls: list[_Call] = []
    dummy = _DummyResp()
    _patch_request(adapter, dummy, calls)

    req = _req("sitemap", {"url": "https://example.test/sitemap.xml", "method": "GET"})
    res = adapter.fetch(req)

    assert res.transport_status == 200
    assert res.url == "https://example.test/resource"
    assert res.data == b"ok"
    assert res.content_type and res.content_type.startswith("text/html")
    assert isinstance(res.headers, dict)
    assert res.elapsed_ms == 123

    assert len(calls) == 1
    c = calls[0]
    assert c.method == "GET"
    assert c.url == "https://example.test/sitemap.xml"
    assert c.allow_redirects is True
    assert isinstance(c.headers, dict)


def test_post_with_string_body_encodes_utf8() -> None:
    adapter = HttpRequestsAdapter()
    calls: list[_Call] = []
    dummy = _DummyResp(
        content=b"created", status=200, headers={"Content-Type": "application/json"}
    )
    _patch_request(adapter, dummy, calls)

    body_str = '{"ok": "✓"}'
    req = _req(
        "post",
        {
            "url": "https://example.test/api",
            "method": "POST",
            "body": body_str,  # str should be encoded as utf-8
            "headers": {"Content-Type": "application/json"},
            "timeout": 5,
            "allow_redirects": False,
        },
    )
    res = adapter.fetch(req)

    assert res.transport_status == 200
    assert res.content_type == "application/json"
    assert res.data == b"created"

    assert len(calls) == 1
    c = calls[0]
    assert c.method == "POST"
    assert c.url == "https://example.test/api"
    assert c.allow_redirects is False
    assert c.headers.get("Content-Type") == "application/json"
    assert c.timeout == 5
    assert isinstance(c.data, (bytes, type(None)))
    assert c.data == body_str.encode("utf-8")


def test_request_rejects_bytes_body() -> None:
    # bytes cannot be embedded in Request.params (JSON-serializable only)
    with pytest.raises(TypeError):
        _ = _req(
            "post",
            {
                "url": "https://example.test/api",
                "method": "POST",
                "body": b"\x00\x01\x02",
            },
        )


def test_headers_merge() -> None:
    adapter = HttpRequestsAdapter()
    calls: list[_Call] = []
    dummy = _DummyResp()
    _patch_request(adapter, dummy, calls)

    req = _req(
        "get",
        {
            "url": "https://example.test/x",
            "headers": {"X-Test": "1", "Accept": "application/xml"},
        },
    )
    _ = adapter.fetch(req)

    assert len(calls) == 1
    headers = calls[0].headers
    assert headers.get("X-Test") == "1"
    assert headers.get("Accept") == "application/xml"


def test_missing_url_raises_value_error() -> None:
    adapter = HttpRequestsAdapter()
    req = _req("sitemap", {})
    with pytest.raises(ValueError):
        adapter.fetch(req)


def test_http_error_propagates() -> None:
    adapter = HttpRequestsAdapter()
    calls: list[_Call] = []
    dummy = _DummyResp(status=429)
    _patch_request(adapter, dummy, calls)

    req = _req("get", {"url": "https://example.test/throttle"})
    with pytest.raises(HTTPError):
        adapter.fetch(req)


def test_describe_and_close() -> None:
    adapter = HttpRequestsAdapter()

    assert isinstance(adapter.describe(), str) and adapter.describe()

    closed = {"flag": False}

    def _fake_close(self: Session) -> None:
        closed["flag"] = True

    session = adapter._session  # type: ignore[attr-defined]
    session.close = MethodType(_fake_close, session)
    adapter.close()
    assert closed["flag"] is True
