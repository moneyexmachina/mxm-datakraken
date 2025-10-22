from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, cast

import pytest
from mxm_config import load_config
from omegaconf import DictConfig, OmegaConf
from omegaconf.errors import ReadonlyConfigError

from mxm_datakraken.config.config import (
    datakraken_view,
    justetf_dataio_view,
    justetf_http_view,
    justetf_paths_view,
)


def _load_cfg_from_repo_yaml(
    mxm_config_home: Callable[[str, str], Path],
    *,
    env: str = "dev",
    profile: str = "default",
) -> DictConfig:
    # Mirror mxm_datakraken/config/*.yaml into MXM_CONFIG_HOME/mxm-datakraken/
    mxm_config_home("mxm-datakraken", "mxm_datakraken")
    cfg = cast(
        DictConfig,
        load_config(package="mxm-datakraken", env=env, profile=profile),
    )
    assert isinstance(cfg, DictConfig)
    return cfg


def test_datakraken_view_mapping_readonly_and_identity(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg = _load_cfg_from_repo_yaml(mxm_config_home)

    view = cast(DictConfig, datakraken_view(cfg))  # resolve=True by default
    assert isinstance(view, DictConfig)

    # The view should be the same underlying subtree (no deep copy).
    assert view is cfg.mxm_datakraken  # type: ignore[attr-defined]

    # Basic expected subtrees present
    for key in ("paths", "sources"):
        assert key in view

    # Read-only enforced
    with pytest.raises(ReadonlyConfigError):
        view.paths.root = "/tmp/override"  # type: ignore[attr-defined]


def test_justetf_paths_view_has_core_paths_and_is_readonly(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg = _load_cfg_from_repo_yaml(mxm_config_home)
    pview = cast(DictConfig, justetf_paths_view(cfg))  # resolved

    # Core keys exist and are non-empty strings
    required_top = (
        "root",
        "profile_index_dir",
        "profiles_dir",
        "parsed_dir",
        "logs_dir",
    )
    for key in required_top:
        assert key in pview
        val = pview[key]
        assert isinstance(val, str) and len(val) > 0

    # Nested dataio paths
    assert "dataio" in pview
    assert isinstance(pview.dataio.db_path, str)  # type: ignore[attr-defined]
    assert isinstance(pview.dataio.responses_dir, str)  # type: ignore[attr-defined]

    # Composition includes env/profile suffixes (sanity)
    assert "/dev/datakraken/default" in pview.root  # type: ignore[attr-defined]
    assert pview.root.endswith("/sources/justetf")  # type: ignore[attr-defined]
    assert pview.parsed_dir.endswith("/sources/justetf/parsed")  # type: ignore[attr-defined]

    # Read-only enforced
    with pytest.raises(ReadonlyConfigError):
        pview.db_path = "/tmp/x.sqlite"  # type: ignore[attr-defined]


def test_justetf_http_view_defaults_env_override_and_copy_safety(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg_dev = _load_cfg_from_repo_yaml(mxm_config_home, env="dev", profile="default")
    cfg_prod = _load_cfg_from_repo_yaml(mxm_config_home, env="prod", profile="default")

    hview_dev = cast(DictConfig, justetf_http_view(cfg_dev))  # resolved
    hview_prod = cast(DictConfig, justetf_http_view(cfg_prod))  # resolved

    # Expected knobs exist with sensible types
    assert str(hview_dev.base_url) == "https://www.justetf.com"  # type: ignore[attr-defined]
    assert isinstance(hview_dev.timeout_s, (int, float))  # type: ignore[attr-defined]
    assert isinstance(hview_dev.verify_ssl, bool)  # type: ignore[attr-defined]
    assert "headers" in hview_dev and "retries" in hview_dev

    # Environment override for politeness_ms
    assert int(hview_dev.politeness_ms) == 1200  # type: ignore[attr-defined]
    assert int(hview_prod.politeness_ms) == 1500  # type: ignore[attr-defined]

    # Vendor-inherited retries structure
    retries = cast(
        Dict[str, Any], OmegaConf.to_container(hview_dev.retries, resolve=True)
    )  # type: ignore[attr-defined]
    assert retries.get("max_attempts") == 3
    assert retries.get("backoff_ms") == [500, 1000, 2000]

    # Converting to a dict allows local mutation without affecting cfg
    params = cast(Dict[str, Any], OmegaConf.to_container(hview_dev, resolve=True))
    old_timeout = params.get("timeout_s")
    params["timeout_s"] = 999  # local change

    hview_dev2 = cast(DictConfig, justetf_http_view(cfg_dev))
    assert hview_dev2.timeout_s == old_timeout  # type: ignore[attr-defined]

    # Read-only enforced on the view itself
    with pytest.raises(ReadonlyConfigError):
        hview_dev.timeout_s = 999  # type: ignore[attr-defined]


def test_justetf_dataio_view_policy_and_copy_safety(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg = _load_cfg_from_repo_yaml(mxm_config_home)
    dview = cast(DictConfig, justetf_dataio_view(cfg))  # resolved

    # Serialization block
    assert str(dview.serialization.response_format) == "raw"  # type: ignore[attr-defined]
    assert str(dview.serialization.compression) == "none"  # type: ignore[attr-defined]
    assert str(dview.serialization.hash_algo) == "sha256"  # type: ignore[attr-defined]

    # Audit block
    audit = cast(Dict[str, Any], OmegaConf.to_container(dview.audit, resolve=True))
    assert audit.get("record_request") is True
    assert audit.get("record_response") is True
    assert "Authorization" in (audit.get("redact_headers") or [])

    # Cache block
    cache = cast(Dict[str, Any], OmegaConf.to_container(dview.cache, resolve=True))
    assert cache.get("use_cache") is True
    assert cache.get("write_cache") is True
    assert cache.get("force_refresh") is False

    # Dict conversion is copy-safe
    dopts = cast(Dict[str, Any], OmegaConf.to_container(dview, resolve=True))
    dopts["serialization"] = {**dopts["serialization"], "response_format": "json"}
    dview2 = cast(DictConfig, justetf_dataio_view(cfg))
    assert str(dview2.serialization.response_format) == "raw"  # type: ignore[attr-defined]

    # Read-only enforced
    with pytest.raises(ReadonlyConfigError):
        dview.cache.use_cache = False  # type: ignore[attr-defined]


def test_profile_overlay_changes_paths_for_research(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg = _load_cfg_from_repo_yaml(mxm_config_home, env="dev", profile="research")
    pview = cast(DictConfig, justetf_paths_view(cfg))
    assert pview.parsed_dir.endswith("/sources/justetf/parsed_research")  # type: ignore[attr-defined]
