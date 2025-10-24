from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Mapping, Optional, cast

import pytest
from mxm_config import MXMConfig

from mxm_datakraken.bootstrap import register_adapters_from_config
from mxm_datakraken.common.http_adapter import HttpRequestsAdapter

# ---------- helpers -----------------------------------------------------------


class _RegisterSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def __call__(self, alias: str, adapter: Any) -> None:
        self.calls.append((alias, adapter))


def _cfg_stub() -> MXMConfig:
    """Minimal object satisfying the MXMConfig protocol for unit tests."""
    # We only need .cfg to exist; the view is monkeypatched anyway.
    return cast(MXMConfig, SimpleNamespace(cfg=SimpleNamespace()))


def _node(
    *,
    alias: str = "justetf",
    enabled: bool = True,
    user_agent: str = "mxm-dk/0.2",
    default_timeout: float = 9.0,
    default_headers: Optional[Mapping[str, str]] = None,
) -> Any:
    """Return a simple attribute bag that behaves like the adapter view node."""
    return SimpleNamespace(
        enabled=enabled,
        alias=alias,
        user_agent=user_agent,
        default_timeout=default_timeout,
        default_headers=dict(default_headers or {}),
    )


# ---------- tests -------------------------------------------------------------


def test_register_from_view_registers(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _RegisterSpy()

    def _patch_register(alias: str, adapter: Any) -> None:
        spy(alias, adapter)

    def _patch_view(_cfg: MXMConfig, resolve: bool = True) -> Any:  # noqa: ARG001
        return _node(
            alias="justetf",
            user_agent="mxm-dk/0.2",
            default_timeout=12.5,
            default_headers={"Accept": "*/*"},
        )

    monkeypatch.setattr(
        "mxm_datakraken.bootstrap.register", _patch_register, raising=True
    )
    monkeypatch.setattr(
        "mxm_datakraken.bootstrap.justetf_http_adapter_view", _patch_view, raising=True
    )

    cfg = _cfg_stub()
    register_adapters_from_config(cfg)

    assert len(spy.calls) == 1
    alias, adapter = spy.calls[0]
    assert alias == "justetf"
    assert isinstance(adapter, HttpRequestsAdapter)
    # Spot-check adapter fields
    assert "User-Agent" in adapter.default_headers
    assert adapter.default_headers["User-Agent"] == "mxm-dk/0.2"  # type: ignore[index]


def test_register_disabled_does_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _RegisterSpy()

    def _patch_register(alias: str, adapter: Any) -> None:
        spy(alias, adapter)

    def _patch_view(_cfg: MXMConfig, resolve: bool = True) -> Any:  # noqa: ARG001
        return _node(enabled=False)

    monkeypatch.setattr(
        "mxm_datakraken.bootstrap.register", _patch_register, raising=True
    )
    monkeypatch.setattr(
        "mxm_datakraken.bootstrap.justetf_http_adapter_view", _patch_view, raising=True
    )

    cfg = _cfg_stub()
    register_adapters_from_config(cfg)

    assert spy.calls == []


def test_register_swallows_registry_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(_alias: str, _adapter: Any) -> None:
        raise RuntimeError("duplicate")

    def _patch_view(_cfg: MXMConfig, resolve: bool = True) -> Any:  # noqa: ARG001
        return _node()

    monkeypatch.setattr("mxm_datakraken.bootstrap.register", _boom, raising=True)
    monkeypatch.setattr(
        "mxm_datakraken.bootstrap.justetf_http_adapter_view", _patch_view, raising=True
    )

    cfg = _cfg_stub()
    # Should not raise even if registry rejects the adapter
    register_adapters_from_config(cfg)


def test_missing_adapter_node_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    """If the view is missing or fails, bootstrap should no-op gracefully."""
    spy = _RegisterSpy()

    def _patch_register(alias: str, adapter: Any) -> None:
        spy(alias, adapter)

    def _raise_view(_cfg: MXMConfig, resolve: bool = True) -> Any:  # noqa: ARG001
        raise AttributeError("no node")

    monkeypatch.setattr(
        "mxm_datakraken.bootstrap.register", _patch_register, raising=True
    )
    monkeypatch.setattr(
        "mxm_datakraken.bootstrap.justetf_http_adapter_view", _raise_view, raising=True
    )

    cfg = _cfg_stub()
    register_adapters_from_config(cfg)

    assert spy.calls == []


def test_register_strict_raises_on_missing_http_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    With strict=True, register_adapters_from_config should raise if
    sources.justetf.dataio.adapters.http cannot be resolved.
    """

    # Ensure the view used by bootstrap simulates a missing/ill-formed node
    def _raise_view(_cfg: MXMConfig, resolve: bool = True) -> object:  # noqa: ARG001
        raise AttributeError("no node")

    # We don't expect register() to be called at all, but patch it to be safe
    def _should_not_be_called(alias: str, adapter: object) -> None:  # noqa: ARG001
        pytest.fail("register() was called despite strict mode and missing node")

    monkeypatch.setattr(
        "mxm_datakraken.bootstrap.justetf_http_adapter_view", _raise_view, raising=True
    )
    monkeypatch.setattr(
        "mxm_datakraken.bootstrap.register", _should_not_be_called, raising=True
    )

    cfg = _cfg_stub()  # If you don't have this helper, see below for a fallback.

    with pytest.raises(RuntimeError, match="Adapter config missing"):
        register_adapters_from_config(cfg, strict=True)
