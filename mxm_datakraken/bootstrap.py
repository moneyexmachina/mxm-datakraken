from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Union, cast

from mxm_dataio.registry import register

from mxm_datakraken.common.http_adapter import HttpRequestsAdapter

if TYPE_CHECKING:
    # Only for type checking; avoids a hard runtime dep here
    from omegaconf import DictConfig  # pragma: no cover
else:
    DictConfig = object  # type: ignore[misc,assignment]


ConfigLike = Union[Mapping[str, Any], "DictConfig"]


def _to_plain_dict(cfg: ConfigLike) -> Mapping[str, Any]:
    """
    Convert a DictConfig to a plain dict if OmegaConf is available.
    If cfg is already a Mapping, return it as-is.

    We import OmegaConf lazily to avoid making it a hard dependency of datakraken.
    """
    if isinstance(cfg, Mapping):
        return cfg

    try:  # pragma: no cover - exercised in integration, trivial to test if needed
        from omegaconf import OmegaConf  # type: ignore
    except Exception:
        # Fall back: treat it as Mapping at runtime (best-effort)
        return cast(Mapping[str, Any], cfg)

    as_dict = OmegaConf.to_container(cfg, resolve=False)
    # OmegaConf returns a dict[str, Any] (or nested dicts/lists)
    return cast(Mapping[str, Any], as_dict)


def _get_path(d: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    """
    Defensive nested-get helper for plain dict-like configs.
    Example: _get_path(cfg, "dataio", "adapters", "http", default={})
    """
    cur: Any = d
    for k in keys:
        if not isinstance(cur, Mapping):  # type: ignore[unreachable]
            return default
        if k not in cur:
            return default
        cur = cur[k]
    return cur


def register_adapters_from_config(cfg: ConfigLike) -> None:
    """
    Register dataio adapters explicitly, driven by mxm-config.

    Expected config shape (example):

      dataio:
        adapters:
          http:
            enabled: true
            alias: "http"                     # or "justetf" if you want per-source aliasing
            user_agent: "mxm-datakraken/0.2 (contact@moneyexmachina.com)"
            default_timeout: 30.0
            default_headers:
              Accept: "*/*"

    Notes:
    - This function is idempotent at the registry level (duplicates may be ignored by the registry).
    - Keep this bootstrap explicit in your app entry points; no auto-registration on import.
    """
    plain = _to_plain_dict(cfg)

    http_cfg = _get_path(plain, "dataio", "adapters", "http", default=None)
    if isinstance(http_cfg, Mapping) and http_cfg.get("enabled", True):
        alias = str(http_cfg.get("alias", "http"))
        user_agent = str(
            http_cfg.get(
                "user_agent", "mxm-datakraken/0.2 (contact@moneyexmachina.com)"
            )
        )
        default_timeout = float(http_cfg.get("default_timeout", 30.0))
        default_headers = cast(
            Mapping[str, str] | None, http_cfg.get("default_headers") or None
        )

        try:
            register(
                alias,
                HttpRequestsAdapter(  # type: ignore[arg-type]
                    user_agent=user_agent,
                    default_timeout=default_timeout,
                    default_headers=default_headers,
                ),
            )
        except Exception:
            # If registry rejects duplicates or dataio isn't active in this process,
            # fail softly to keep bootstrap side-effect free.
            pass
