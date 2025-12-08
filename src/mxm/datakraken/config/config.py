"""
Config views for mxm-datakraken.

- justetf_view(cfg):          filesystem + source-level settings for JustETF
- justetf_dataio_view(cfg):   DataIO subtree for JustETF (pass to DataIoSession)
- dataio_for_justetf(cfg):    convenience tuple (source_name, dataio_view)
- justetf_http_adapter_view(cfg): Http adapter config
"""

from __future__ import annotations

from typing import Iterable

from mxm.config import MXMConfig, make_view

from mxm.datakraken.common.caching import (
    CachePolicy,
    resolve_as_of_bucket,
    resolve_cache_mode,
)

SOURCE_JUSTETF = "justetf"


def justetf_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """Read-only view rooted at `sources.justetf`."""
    return make_view(cfg, "sources.justetf", resolve=resolve)


def justetf_dataio_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """Read-only view rooted at `sources.justetf.dataio` (for DataIoSession/Store)."""
    return make_view(cfg, "sources.justetf.dataio", resolve=resolve)


def dataio_for_justetf(
    cfg: MXMConfig, *, resolve: bool = True
) -> tuple[str, MXMConfig]:
    """Convenience: return (source_name, dataio_view) for JustETF."""
    return SOURCE_JUSTETF, justetf_dataio_view(cfg, resolve=resolve)


def justetf_http_adapter_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """
    Read-only view rooted at `sources.justetf.dataio.adapters.http`.
    """
    return make_view(cfg, "sources.justetf.dataio.adapters.http", resolve=resolve)


def justetf_policy_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """Read-only view rooted at `sources.justetf.policy`."""
    return make_view(cfg, "sources.justetf.policy", resolve=resolve)


def load_justetf_policy(cfg: MXMConfig) -> CachePolicy:
    """Load and resolve the `sources.justetf.policy` block into a `CachePolicy`.

    This function reads the **unresolved** policy view
    (`resolve=False`) so that strftime-style formats (e.g. ``"%Y-%m-%d"``)
    can be resolved explicitly via `resolve_as_of_bucket`. It then converts:
      * `cache_mode` (str) → `CacheMode`
      * `ttl_seconds` (int|float|None) → `float|None`
      * `as_of_bucket` (format or literal str) → resolved `str`

    Args:
      cfg: An already-composed `MXMConfig` tree for mxm-datakraken.

    Returns:
      A `CachePolicy` with fully resolved and typed fields, suitable for
      passing directly to `DataIoSession` (as `cache_mode`, `ttl`, `as_of_bucket`).

    Raises:
      AttributeError: If required keys (`cache_mode`, `ttl_seconds`, `as_of_bucket`)
        are missing from `sources.justetf.policy`.
      ValueError: If `cache_mode` is not a valid `CacheMode` string accepted by
        `resolve_cache_mode` (implementation-dependent).

    Example:
      >>> policy = load_justetf_policy(cfg)
      >>> policy.cache_mode
      <CacheMode.DEFAULT: 'default'>
      >>> policy.ttl_seconds
      0.0
      >>> policy.as_of_bucket
      '2025-10-28'
    """
    pview = justetf_policy_view(cfg, resolve=False)
    result = CachePolicy(
        cache_mode=resolve_cache_mode(pview.cache_mode),
        ttl_seconds=float(pview.ttl_seconds) if pview.ttl_seconds is not None else None,
        as_of_bucket=resolve_as_of_bucket(pview.as_of_bucket),
    )
    return result


class ConfigError(RuntimeError):
    pass


def _must_have(d: MXMConfig, path: str, keys: Iterable[str]) -> None:
    missing = [k for k in keys if not hasattr(d, k)]
    if missing:
        raise ConfigError(f"Missing keys at {path}: {', '.join(missing)}")


def ensure_justetf_config(cfg: MXMConfig) -> None:
    """works for DictConfig or wrapper; views already do the right thing
    after your _root_cfg shim.
    """
    j = justetf_view(cfg)
    _must_have(
        j,
        "sources.justetf",
        ("root", "profile_index_dir", "profiles_dir", "parsed_dir", "logs_dir"),
    )

    dio = justetf_dataio_view(cfg)
    _must_have(dio, "sources.justetf.dataio.paths", ("paths",))
    _must_have(
        dio.paths, "sources.justetf.dataio.paths", ("root", "db_path", "responses_dir")
    )

    http = justetf_http_adapter_view(cfg)
    _must_have(
        http,
        "sources.justetf.dataio.adapters.http",
        ("enabled", "alias", "user_agent", "default_timeout"),
    )
    p = justetf_policy_view(cfg)
    _must_have(
        p, "sources.justetf.policy", ("cache_mode", "ttl_seconds", "as_of_bucket")
    )


__all__ = [
    "SOURCE_JUSTETF",
    "justetf_view",
    "justetf_dataio_view",
    "dataio_for_justetf",
    "justetf_http_adapter_view",
    "ensure_justetf_config",
    "justetf_policy_view",
    "load_justetf_policy",
]
