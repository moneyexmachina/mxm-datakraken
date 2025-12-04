from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple, cast

import pytest
from mxm.types import JSONLike, JSONObj
from mxm_config import MXMConfig

# Target the orchestrator module for monkeypatches
import mxm_datakraken.sources.justetf.batch.run as run_mod


def _read_jsonl(path: Path) -> List[JSONObj]:
    txt = path.read_text(encoding="utf-8").strip()
    return [json.loads(line) for line in txt.splitlines()] if txt else []


def test_happy_two_ok(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg: MXMConfig = cast(MXMConfig, {})
    entries: Sequence[Dict[str, str]] = [
        {"isin": "IE00AAA11111", "url": "http://dummy/etf1"},
        {"isin": "IE00BBB22222", "url": "http://dummy/etf2"},
    ]

    # get_profile_index → 2 items
    def fake_get_index(
        cfg_arg: MXMConfig,
        base_path: Path,
        **kwargs: object,
    ) -> Sequence[Dict[str, str]]:
        assert cfg_arg is cfg
        _ = base_path, kwargs
        return entries

    # should_skip → never
    def fake_should_skip(
        *,
        base_path: Path,
        bucket: Optional[str],
        isin: str,
        force_refresh: bool,
    ) -> Tuple[bool, Optional[str]]:
        _ = base_path, bucket, isin, force_refresh
        return (False, None)

    # process_one_entry → OK with deterministic bucket
    def fake_process_one_entry(
        *,
        cfg: MXMConfig,
        base_path: Path,
        entry: Dict[str, str],
        bucket: Optional[str],
        download_html: Callable[..., object],
        parse: Callable[..., object],
        save: Callable[..., object],
        write_latest: bool,
    ) -> Tuple[str, JSONObj, str, None]:
        _ = cfg, base_path, bucket, download_html, parse, save, write_latest
        profile: JSONObj = {
            "isin": entry["isin"],
            "name": "Dummy",
            "source_url": entry["url"],
        }
        return ("ok", profile, "2025-10-30", None)

    # save_profiles_snapshot writes file and returns the path
    def fake_save_snapshot(
        profiles: List[JSONObj],
        *,
        base_path: Path,
        as_of_bucket: str,
        write_latest: bool,
    ) -> Path:
        _ = write_latest
        p = base_path / "profiles" / as_of_bucket / "profiles.parsed.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(profiles, ensure_ascii=False), encoding="utf-8")
        return p

    monkeypatch.setattr(run_mod, "get_profile_index", fake_get_index)
    monkeypatch.setattr(run_mod, "should_skip", fake_should_skip)
    monkeypatch.setattr(run_mod, "process_one_entry", fake_process_one_entry)
    monkeypatch.setattr(run_mod, "save_profiles_snapshot", fake_save_snapshot)

    snapshot = run_mod.run_batch(
        cfg=cfg,
        base_path=tmp_path,
        write_latest=True,
        rate_seconds=0.0,  # avoid sleep
        force_refresh=False,
        run_id="testrun",
    )

    # Snapshot path & content
    assert snapshot == tmp_path / "profiles" / "2025-10-30" / "profiles.parsed.json"
    data: List[JSONLike] = json.loads(snapshot.read_text(encoding="utf-8"))
    assert isinstance(data, list) and len(data) == 2

    # Progress lines
    progress = _read_jsonl(
        tmp_path / "profiles" / "runs" / "testrun" / "progress.jsonl"
    )
    statuses = [r["status"] for r in progress]
    assert statuses.count("ok") == 2

    # OK markers
    ok_dir = tmp_path / "profiles" / "runs" / "testrun" / "ok"
    assert (ok_dir / "IE00AAA11111.ok").exists()
    assert (ok_dir / "IE00BBB22222.ok").exists()


def test_skip_then_ok(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg: MXMConfig = cast(MXMConfig, {})
    entries: Sequence[Dict[str, str]] = [
        {"isin": "SKIP00000001", "url": "http://dummy/skip"},
        {"isin": "OK0000000002", "url": "http://dummy/ok"},
    ]

    def fake_get_index(
        cfg_arg: MXMConfig, base_path: Path, **kwargs: object
    ) -> Sequence[Dict[str, str]]:
        _ = cfg_arg, base_path, kwargs
        return entries

    # skip first, then ok
    calls = {"n": 0}

    def fake_should_skip(
        *,
        base_path: Path,
        bucket: Optional[str],
        isin: str,
        force_refresh: bool,
    ) -> Tuple[bool, Optional[str]]:
        _ = base_path, bucket, isin, force_refresh
        calls["n"] += 1
        return (calls["n"] == 1, "exists" if calls["n"] == 1 else None)

    def fake_process_one_entry(
        *,
        cfg: MXMConfig,
        base_path: Path,
        entry: Dict[str, str],
        bucket: Optional[str],
        download_html: Callable[..., object],
        parse: Callable[..., object],
        save: Callable[..., object],
        write_latest: bool,
    ) -> Tuple[str, JSONObj, str, None]:
        _ = cfg, base_path, bucket, download_html, parse, save, write_latest
        profile: JSONObj = {
            "isin": entry["isin"],
            "name": "X",
            "source_url": entry["url"],
        }
        return ("ok", profile, "2025-10-30", None)

    def fake_save_snapshot(
        profiles: List[JSONObj],
        *,
        base_path: Path,
        as_of_bucket: str,
        write_latest: bool,
    ) -> Path:
        _ = write_latest
        p = base_path / "profiles" / as_of_bucket / "profiles.parsed.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(profiles), encoding="utf-8")
        return p

    monkeypatch.setattr(run_mod, "get_profile_index", fake_get_index)
    monkeypatch.setattr(run_mod, "should_skip", fake_should_skip)
    monkeypatch.setattr(run_mod, "process_one_entry", fake_process_one_entry)
    monkeypatch.setattr(run_mod, "save_profiles_snapshot", fake_save_snapshot)

    _ = run_mod.run_batch(cfg, tmp_path, rate_seconds=0.0, run_id="skipok")

    progress = _read_jsonl(tmp_path / "profiles" / "runs" / "skipok" / "progress.jsonl")
    statuses = [r["status"] for r in progress]
    assert statuses == ["skip", "ok"]


def test_error_flow_logs_and_err_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg: MXMConfig = cast(MXMConfig, {})
    entries: Sequence[Dict[str, str]] = [
        {"isin": "BAD00000001", "url": "http://dummy/bad"}
    ]

    def fake_get_index(
        cfg_arg: MXMConfig, base_path: Path, **kwargs: object
    ) -> Sequence[Dict[str, str]]:
        _ = cfg_arg, base_path, kwargs
        return entries

    def fake_should_skip(
        *,
        base_path: Path,
        bucket: Optional[str],
        isin: str,
        force_refresh: bool,
    ) -> Tuple[bool, Optional[str]]:
        _ = base_path, bucket, isin, force_refresh
        return (False, None)

    def fake_process_one_entry(
        *,
        cfg: MXMConfig,
        base_path: Path,
        entry: Dict[str, str],
        bucket: Optional[str],
        download_html: Callable[..., object],
        parse: Callable[..., object],
        save: Callable[..., object],
        write_latest: bool,
    ) -> Tuple[str, None, str, str]:
        _ = cfg, base_path, entry, bucket, download_html, parse, save, write_latest
        return ("err", None, "2025-10-30", "boom")

    def fake_save_snapshot(
        profiles: List[JSONObj],
        *,
        base_path: Path,
        as_of_bucket: str,
        write_latest: bool,
    ) -> Path:
        _ = profiles, write_latest
        p = base_path / "profiles" / as_of_bucket / "profiles.parsed.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("[]", encoding="utf-8")
        return p

    monkeypatch.setattr(run_mod, "get_profile_index", fake_get_index)
    monkeypatch.setattr(run_mod, "should_skip", fake_should_skip)
    monkeypatch.setattr(run_mod, "process_one_entry", fake_process_one_entry)
    monkeypatch.setattr(run_mod, "save_profiles_snapshot", fake_save_snapshot)

    _ = run_mod.run_batch(cfg, tmp_path, rate_seconds=0.0, run_id="errrun")

    progress = _read_jsonl(tmp_path / "profiles" / "runs" / "errrun" / "progress.jsonl")
    assert len(progress) == 1 and progress[0]["status"] == "err"

    err_dir = tmp_path / "profiles" / "runs" / "errrun" / "err"
    assert any(p.name.startswith("BAD00000001") for p in err_dir.glob("*.json"))


def test_bucket_resolution_when_all_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg: MXMConfig = cast(MXMConfig, {})
    entries: Sequence[Dict[str, str]] = [{"isin": "X1", "url": "http://dummy/x1"}]

    def fake_get_index(
        cfg_arg: MXMConfig, base_path: Path, **kwargs: object
    ) -> Sequence[Dict[str, str]]:
        _ = cfg_arg, base_path, kwargs
        return entries

    def fake_should_skip(
        *,
        base_path: Path,
        bucket: Optional[str],
        isin: str,
        force_refresh: bool,
    ) -> Tuple[bool, Optional[str]]:
        _ = base_path, bucket, isin, force_refresh
        return (True, "exists")

    seen: Dict[str, Optional[str]] = {"bucket": None}

    def fake_resolve_bucket(
        *,
        provided: Optional[str],
        first_resp_bucket: Optional[str],
        profiles_root: Path,
        today_iso: str,
    ) -> str:
        _ = provided, first_resp_bucket, profiles_root, today_iso
        seen["bucket"] = "2099-01-01"
        return cast(str, seen["bucket"])

    def fake_save_snapshot(
        profiles: List[JSONObj],
        *,
        base_path: Path,
        as_of_bucket: str,
        write_latest: bool,
    ) -> Path:
        assert as_of_bucket == seen["bucket"]
        _ = profiles, write_latest
        p = base_path / "profiles" / as_of_bucket / "profiles.parsed.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("[]", encoding="utf-8")
        return p

    monkeypatch.setattr(run_mod, "get_profile_index", fake_get_index)
    monkeypatch.setattr(run_mod, "should_skip", fake_should_skip)
    monkeypatch.setattr(run_mod, "resolve_bucket", fake_resolve_bucket)
    monkeypatch.setattr(run_mod, "save_profiles_snapshot", fake_save_snapshot)

    snapshot = run_mod.run_batch(cfg, tmp_path, rate_seconds=0.0, run_id="allskip")
    assert snapshot == tmp_path / "profiles" / "2099-01-01" / "profiles.parsed.json"
