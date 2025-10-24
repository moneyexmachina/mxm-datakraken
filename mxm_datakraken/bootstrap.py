"""
Bootstrap adapter registration for mxm-datakraken.

This module wires up transport adapters (currently: a requests-based HTTP adapter)
into the mxm-dataio registry using values sourced from the package’s MXMConfig.
It performs **no implicit side effects on import**; callers should invoke
`register_adapters_from_config(cfg)` from an explicit entry point (CLI, script,
or test) once the config is loaded.

Configuration
-------------
The HTTP adapter is configured under:

    sources.justetf.dataio.adapters.http

Example (`default.yaml`):

    sources:
      justetf:
        dataio:
          adapters:
            http:
              enabled: true
              alias: "justetf"  # registry key; use "http" if sharing a single adapter
              user_agent: "mxm-datakraken/0.2 (contact@moneyexmachina.com)"
              default_timeout: 30.0
              default_headers:
                Accept: "*/*"

Behavior & guarantees
---------------------
- Reads adapter settings via MXMConfig **views** (dot access), no dict casting.
- Registers an `HttpRequestsAdapter` under the chosen alias.
- Safe to call multiple times; duplicate registrations are soft-failed (ignored).
- If the config subtree is missing or disabled, registration is skipped.
- Designed to be source-scoped (JustETF) but trivially extensible to more adapters.

Usage
-----
    from mxm_config import load_config
    from mxm_datakraken.bootstrap import register_adapters_from_config

    cfg = load_config(package="mxm-datakraken", env="dev", profile="default")
    register_adapters_from_config(cfg)

Testing
-------
In unit tests, monkeypatch the registry to assert the call without touching global state:

    def test_bootstrap_registers(monkeypatch):
        calls = {}
        def fake_register(alias, adapter):
            calls["alias"] = alias
            calls["type"] = type(adapter).__name__
        monkeypatch.setattr("mxm_dataio.registry.register", fake_register)
        register_adapters_from_config(cfg)
        assert calls == {"alias": "justetf", "type": "HttpRequestsAdapter"}
"""

from __future__ import annotations

from typing import Any, Mapping, Optional, cast

from mxm_config import MXMConfig
from mxm_dataio.registry import register

from mxm_datakraken.common.http_adapter import HttpRequestsAdapter
from mxm_datakraken.config.config import justetf_http_adapter_view


def _coerce_headers(m: Optional[Mapping[str, Any]]) -> Optional[Mapping[str, str]]:
    if m is None:
        return None
    # Keep it defensive: if node isn't a Mapping at runtime, just ignore.
    try:
        return {str(k): str(v) for k, v in m.items()}
    except Exception:
        return None


def register_adapters_from_config(cfg: MXMConfig, strict: bool = False) -> None:
    """
    Register DataIO adapters based on package config (JustETF-specific).
    Uses MXMConfig views and dot-access only; no dict conversion.
    """
    # If the node doesn't exist, make_view may raise; treat as "not configured".
    try:
        http = justetf_http_adapter_view(cfg, resolve=True)
    except Exception as e:
        if strict:
            raise RuntimeError(
                "Adapter config missing: sources.justetf.dataio.adapters.http"
            ) from e
        return

    # Dot access only; fall back to sane defaults if fields are missing.
    enabled = getattr(http, "enabled", True)
    if not bool(enabled):
        return

    alias = str(getattr(http, "alias", "justetf"))
    user_agent = str(
        getattr(http, "user_agent", "mxm-datakraken/0.2 (contact@moneyexmachina.com)")
    )
    default_timeout = float(getattr(http, "default_timeout", 30.0))

    raw_headers_any = getattr(http, "default_headers", None)
    raw_headers = (
        cast(Optional[Mapping[str, Any]], raw_headers_any)
        if isinstance(raw_headers_any, Mapping)
        else None
    )
    headers = _coerce_headers(raw_headers)

    try:
        register(
            alias,
            HttpRequestsAdapter(
                user_agent=user_agent,
                default_timeout=default_timeout,
                default_headers=headers,
            ),
        )
    except Exception:
        # Soft-fail: ignore duplicate registrations or missing registry in this process.
        pass
