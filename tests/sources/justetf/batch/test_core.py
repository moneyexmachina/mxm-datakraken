from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Protocol, Tuple, cast

import pytest
from mxm_config import MXMConfig

from mxm_datakraken.sources.justetf.batch.core import (
    process_one_entry,
    resolve_bucket,
    should_skip,
)
from mxm_datakraken.sources.justetf.common.models import JustETFProfile

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class SaveFn(Protocol):
    def __call__(
        self,
        profile: JustETFProfile,
        base_path: Path,
        *,
        as_of_bucket: str,
        provenance: object,
        write_latest: bool,
    ) -> Path: ...


@dataclass
class SaveCall:
    profile: JustETFProfile
    base_path: Path
    as_of_bucket: str
    provenance: object
    write_latest: bool


def make_fake_save(tmp_path: Path, sink: List[SaveCall]) -> SaveFn:
    """Factory for a save() double with a Pyright-compliant signature."""

    def _fake_save(
        profile: JustETFProfile,
        base_path: Path,
        *,
        as_of_bucket: str,
        provenance: object,
        write_latest: bool,
    ) -> Path:
        sink.append(
            SaveCall(
                profile=profile,
                base_path=base_path,
                as_of_bucket=as_of_bucket,
                provenance=provenance,
                write_latest=write_latest,
            )
        )
        # Return a plausible target path
        return (
            tmp_path
            / "profiles"
            / as_of_bucket
            / profile["isin"]
            / "profile.parsed.json"
        )

    return _fake_save


# ---------- resolve_bucket ----------


def test_resolve_bucket_prefers_provided(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Even if latest exists or resp bucket exists, provided wins
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.core.resolve_latest_bucket",
        lambda _root: "2000-01-01",
    )
    out = resolve_bucket(
        provided="2025-10-30",
        first_resp_bucket="2020-01-01",
        profiles_root=tmp_path,
        today_iso="1999-12-31",
    )
    assert out == "2025-10-30"


def test_resolve_bucket_uses_first_resp_when_no_provided(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.core.resolve_latest_bucket",
        lambda _root: "2000-01-01",
    )
    out = resolve_bucket(
        provided=None,
        first_resp_bucket="2023-07-15",
        profiles_root=tmp_path,
        today_iso="1999-12-31",
    )
    assert out == "2023-07-15"


def test_resolve_bucket_uses_latest_on_disk_when_no_provided_or_resp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.core.resolve_latest_bucket",
        lambda _root: "2011-11-11",
    )
    out = resolve_bucket(
        provided=None,
        first_resp_bucket=None,
        profiles_root=tmp_path,
        today_iso="1999-12-31",
    )
    assert out == "2011-11-11"


def test_resolve_bucket_falls_back_to_today_when_no_other_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Simulate resolve_latest_bucket raising
    def boom(_root: Path) -> str:
        raise RuntimeError("no buckets")

    monkeypatch.setattr(
        "mxm_datakraken.sources.justetf.batch.core.resolve_latest_bucket",
        boom,
    )
    # Without today_iso, it should use real today; just check format
    out = resolve_bucket(
        provided=None,
        first_resp_bucket=None,
        profiles_root=tmp_path,
        today_iso=None,
    )
    assert DATE_RE.match(out), f"expected YYYY-MM-DD, got {out}"


# ---------- should_skip ----------


def test_should_skip_false_when_bucket_unknown(tmp_path: Path) -> None:
    skip, reason = should_skip(
        base_path=tmp_path, bucket=None, isin="IE00AAA11111", force_refresh=False
    )
    assert skip is False and reason is None


def test_should_skip_false_when_force_refresh(tmp_path: Path) -> None:
    skip, reason = should_skip(
        base_path=tmp_path, bucket="2025-10-30", isin="IE00AAA11111", force_refresh=True
    )
    assert skip is False and reason is None


def test_should_skip_true_when_bucket_known_and_file_exists(tmp_path: Path) -> None:
    # create profiles/<bucket>/<isin>/profile.parsed.json
    p = tmp_path / "profiles" / "2025-10-30" / "IE00AAA11111" / "profile.parsed.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{}", encoding="utf-8")

    skip, reason = should_skip(
        base_path=tmp_path,
        bucket="2025-10-30",
        isin="IE00AAA11111",
        force_refresh=False,
    )
    assert skip is True and reason == "exists"


def test_should_skip_false_when_file_missing(tmp_path: Path) -> None:
    skip, reason = should_skip(
        base_path=tmp_path,
        bucket="2025-10-30",
        isin="IE00BBB22222",
        force_refresh=False,
    )
    assert skip is False and reason is None


# ---------- process_one_entry ----------


def _fake_download_with_resp_bucket(
    cfg: MXMConfig, isin: str, url: str
) -> Tuple[str, object]:
    _ = (cfg, isin, url)
    resp = SimpleNamespace(as_of_bucket="2025-10-30")
    return "<html>dummy</html>", resp


def _fake_download_without_resp_bucket(
    cfg: MXMConfig, isin: str, url: str
) -> Tuple[str, object]:
    _ = (cfg, isin, url)
    resp = SimpleNamespace()  # no as_of_bucket
    return "<html>dummy</html>", resp


def _fake_parse(html: str, isin: str) -> Dict[str, Any]:
    _ = html
    return {"isin": isin, "name": "Dummy"}


def test_process_one_entry_uses_provided_bucket_and_calls_save(tmp_path: Path) -> None:
    cfg: MXMConfig = cast(MXMConfig, {})
    entry = {"isin": "IE00AAA11111", "url": "http://dummy/etf"}
    calls: List[SaveCall] = []
    fake_save = make_fake_save(tmp_path, calls)

    status, profile, bucket_used, error = process_one_entry(
        cfg=cfg,
        base_path=tmp_path,
        entry=entry,
        bucket="2025-10-30",
        download_html=_fake_download_with_resp_bucket,
        parse=_fake_parse,
        save=fake_save,
        write_latest=True,
    )

    assert status == "ok"
    assert error is None
    assert profile is not None
    assert bucket_used == "2025-10-30"
    assert profile["source_url"] == "http://dummy/etf"

    # verify save() call
    assert len(calls) == 1
    call = calls[0]
    assert call.as_of_bucket == "2025-10-30"
    assert call.profile["isin"] == "IE00AAA11111"
    assert call.write_latest is True


def test_process_one_entry_uses_resp_bucket_when_not_provided(tmp_path: Path) -> None:
    cfg: MXMConfig = cast(MXMConfig, {})
    entry = {"isin": "IE00BBB22222", "url": "http://dummy/etf2"}
    saved: List[SaveCall] = []
    fake_save = make_fake_save(tmp_path, saved)

    status, profile, bucket_used, error = process_one_entry(
        cfg=cfg,
        base_path=tmp_path,
        entry=entry,
        bucket=None,
        download_html=_fake_download_with_resp_bucket,
        parse=_fake_parse,
        save=fake_save,
        write_latest=False,
    )

    assert status == "ok"
    assert error is None
    assert profile is not None
    assert bucket_used == "2025-10-30"
    assert profile["source_url"] == "http://dummy/etf2"
    assert saved and saved[0].as_of_bucket == "2025-10-30"
    assert saved[0].write_latest is False


def test_process_one_entry_falls_back_to_today_when_no_buckets_available(
    tmp_path: Path,
) -> None:
    cfg: MXMConfig = cast(MXMConfig, {})
    entry = {"isin": "IE00CCC33333", "url": "http://dummy/etf3"}
    seen: List[SaveCall] = []
    fake_save = make_fake_save(tmp_path, seen)
    status, profile, bucket_used, error = process_one_entry(
        cfg=cfg,
        base_path=tmp_path,
        entry=entry,
        bucket=None,
        download_html=_fake_download_without_resp_bucket,
        parse=_fake_parse,
        save=fake_save,
        write_latest=True,
    )

    assert status == "ok"
    assert error is None
    assert profile is not None
    assert DATE_RE.match(cast(str, bucket_used)), (
        f"expected YYYY-MM-DD, got {bucket_used}"
    )
    assert seen and DATE_RE.match(seen[0].as_of_bucket)


def test_process_one_entry_propagates_errors(tmp_path: Path) -> None:
    cfg: MXMConfig = cast(MXMConfig, {})
    entry = {"isin": "IE00BAD00001", "url": "http://dummy/boom"}

    def boom_download(cfg: MXMConfig, isin: str, url: str) -> Tuple[str, object]:
        raise RuntimeError("network boom")

    # save should never be called, but we provide a dummy anyway
    seen_again: List[SaveCall] = []
    fake_save = make_fake_save(tmp_path, seen_again)

    status, profile, bucket_used, error = process_one_entry(
        cfg=cfg,
        base_path=tmp_path,
        entry=entry,
        bucket=None,
        download_html=boom_download,
        parse=_fake_parse,
        save=fake_save,
        write_latest=True,
    )

    assert status == "err"
    assert profile is None
    # bucket_used stays as the input bucket (None) in error case
    assert bucket_used is None
    assert isinstance(error, str) and "boom" in error
