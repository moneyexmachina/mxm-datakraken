from pathlib import Path

import pytest
from mxm.config import DefaultsMode, install_config

APP_ID = "datakraken"
SHIPPED_PACKAGE = "mxm.datakraken"


def test_install_datakraken_config_creates_expected_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Direct mxm-config to use a temporary config home, not ~/.config/mxm
    monkeypatch.setenv("MXM_CONFIG_HOME", str(tmp_path))

    # Install shipped defaults for datakraken
    install_config(
        app_id=APP_ID,
        mode=DefaultsMode.shipped,
        shipped_package=SHIPPED_PACKAGE,
    )

    # mxm-config will install into <MXM_CONFIG_HOME>/<app_id>/
    config_root = tmp_path / APP_ID

    expected_files = [
        "default.yaml",
        "environment.yaml",
        "machine.yaml",
        "profile.yaml",
        "local.yaml",  # your stub/empty local.yaml
    ]

    for name in expected_files:
        path = config_root / name
        assert path.is_file(), f"Expected {path} to exist"
