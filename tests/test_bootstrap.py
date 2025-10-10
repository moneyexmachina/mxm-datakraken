from __future__ import annotations

import sys
from types import ModuleType
from typing import Any, Mapping

import pytest

from mxm_datakraken.bootstrap import (
    register_adapters_from_config,
)
from mxm_datakraken.common.http_adapter import HttpRequestsAdapter

# ---------- helpers -----------------------------------------------------------


class _RegisterSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def __call__(self, alias: str, adapter: Any) -> None:
        self.calls.append((alias, adapter))


def _plain_cfg(
    alias: str = "http",
    enabled: bool = True,
    user_agent: str = "UA",
    default_timeout: float = 9.0,
    default_headers: Mapping[str, str] | None = None,
) -> Mapping[str, Any]:
    return {
        "dataio": {
            "adapters": {
                "http": {
                    "enabled": enabled,
                    "alias": alias,
                    "user_agent": user_agent,
                    "default_timeout": default_timeout,
                    "default_headers": dict(default_headers or {}),
                }
            }
        }
    }


# ---------- tests -------------------------------------------------------------


def test_register_from_plain_dict_registers(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _RegisterSpy()
    monkeypatch.setattr("mxm_datakraken.bootstrap.register", spy)

    cfg = _plain_cfg(alias="justetf", user_agent="mxm-dk/0.2", default_timeout=12.5)
    register_adapters_from_config(cfg)

    assert len(spy.calls) == 1
    alias, adapter = spy.calls[0]
    assert alias == "justetf"
    assert isinstance(adapter, HttpRequestsAdapter)

    # optional: check a couple of defaults carried through
    assert "User-Agent" in adapter.default_headers
    # mypy/pyright: Mapping[str, str] -> read access only
    assert adapter.default_headers["User-Agent"] == "mxm-dk/0.2"  # type: ignore[index]


def test_register_disabled_does_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _RegisterSpy()
    monkeypatch.setattr("mxm_datakraken.bootstrap.register", spy)

    cfg = _plain_cfg(enabled=False)
    register_adapters_from_config(cfg)

    assert spy.calls == []


def test_register_swallows_registry_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(alias: str, adapter: Any) -> None:  # noqa: ARG001
        raise RuntimeError("duplicate")

    monkeypatch.setattr("mxm_datakraken.bootstrap.register", _boom)

    cfg = _plain_cfg()
    # should not raise
    register_adapters_from_config(cfg)


def test_register_from_dictconfig_via_omegaconf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Simulate an OmegaConf DictConfig present in sys.modules without a hard dependency.
    _to_plain_dict should import OmegaConf and call to_container(...).
    """
    spy = _RegisterSpy()
    monkeypatch.setattr("mxm_datakraken.bootstrap.register", spy)

    # Fake omegaconf module with OmegaConf.to_container
    fake_mod = ModuleType("omegaconf")

    class _FakeOmegaConf:
        @staticmethod
        def to_container(cfg: Any, resolve: bool = False) -> Mapping[str, Any]:  # noqa: ARG002
            # Return the dict we stashed on the fake cfg
            return cfg._as_dict  # type: ignore[attr-defined]

    fake_mod.OmegaConf = _FakeOmegaConf  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "omegaconf", fake_mod)

    class _FakeDictConfig:
        def __init__(self, as_dict: Mapping[str, Any]) -> None:
            self._as_dict = as_dict

    cfg = _FakeDictConfig(
        _plain_cfg(alias="http+dc", user_agent="ua-dc", default_timeout=3.0)
    )
    register_adapters_from_config(cfg)  # type: ignore[arg-type]

    assert len(spy.calls) == 1
    alias, adapter = spy.calls[0]
    assert alias == "http+dc"
    assert isinstance(adapter, HttpRequestsAdapter)
    assert adapter.default_headers["User-Agent"] == "ua-dc"  # type: ignore[index]
