"""时序核查：相邻重启段边界判定、供数资格、更正联动、失败回看与重启补算。"""
from __future__ import annotations

import time

import pytest

from tests.test_app import make_client, set_anchors, upload, wait_idle


def seq(client):
    r = client.get("/api/sequence")
    assert r.status_code == 200, r.text
    return r.json()


def check(client, prev_seg, next_seg):
    return next(c for c in seq(client)["checks"]
                if c["prev_segment"] == prev_seg and c["next_segment"] == next_seg)


def delivery(client, segment):
    return next(s["delivery"] for s in seq(client)["segments"] if s["segment"] == segment)


@pytest.fixture()
def client(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as c:
        yield c


def seed_two_segments(client, seg2_start="2026-01-01T01:30:00Z"):
    """段 1 末点 01:00；段 2 首点默认 01:30（通过），可指定为更早时刻制造冲突。"""
    upload(client, [(1, 0, 1.0), (1, 50, 1.5), (1, 100, 2.0),
                    (2, 0, 3.0), (2, 40, 3.5), (2, 100, 4.0)])
    set_anchors(client, 1, 0, "2026-01-01T00:00:00Z", 100, "2026-01-01T01:00:00Z")
    set_anchors(client, 2, 0, seg2_start, 100, "2026-01-01T02:30:00Z")


# ---------------------------------------------------------------- 判定

def test_adjacent_segments_pass(client):
    seed_two_segments(client)
    c = check(client, 1, 2)
    assert c["status"] == "pass"
    assert c["prev"]["utc_time"] == "2026-01-01T01:00:00.000Z"
    assert c["next"]["utc_time"] == "2026-01-01T01:30:00.000Z"
    # 两端批次号与原行号来自原始读数
    assert (c["prev"]["batch_id"], c["prev"]["row_no"]) == (1, 3)
    assert (c["next"]["batch_id"], c["next"]["row_no"]) == (1, 4)
    assert delivery(client, 1)["status"] == "deliverable"
    assert delivery(client, 2)["status"] == "deliverable"


def test_conflict_when_prev_end_not_before_next_start(client):
    seed_two_segments(client, seg2_start="2026-01-01T00:30:00Z")
    c = check(client, 1, 2)
    assert c["status"] == "conflict"
    assert c["prev"]["utc_time"] >= c["next"]["utc_time"]
    assert c["reason"]
    assert delivery(client, 1)["status"] == "blocked"
    assert delivery(client, 2)["status"] == "blocked"
    body = seq(client)
    assert body["summary"]["blocked_segments"] == [1, 2]
    # 按日读数仍可查看，但标记不可交付
    day = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    assert day["count"] == 6
    assert day["blocked_count"] == 6
    assert all(not r["deliverable"] and r["delivery_status"] == "blocked"
               for r in day["readings"])


def test_equal_boundary_times_are_conflict(client):
    # 前段末点与后段首点同一时刻：不满足“严格早于”，必须判冲突
    seed_two_segments(client, seg2_start="2026-01-01T01:00:00Z")
    assert check(client, 1, 2)["status"] == "conflict"


def test_uncalibrated_boundary_is_pending_never_pass(client):
    upload(client, [(1, 0, 1.0), (1, 100, 2.0), (2, 0, 3.0), (2, 100, 4.0)])
    set_anchors(client, 1, 0, "2026-01-01T00:00:00Z", 100, "2026-01-01T01:00:00Z")
    # 段 2 无锚点：首末点均未校时
    c = check(client, 1, 2)
    assert c["status"] == "pending"
    assert c["next"]["utc_time"] is None
    assert "未校时" in c["reason"]
    assert delivery(client, 1)["status"] == "pending"
    assert delivery(client, 2)["status"] == "pending"
    body = seq(client)
    assert body["summary"]["pass"] == 0 and body["summary"]["pending"] == 1


def test_anchor_range_gap_makes_boundary_pending(client):
    upload(client, [(1, 0, 1.0), (1, 100, 2.0), (2, 0, 3.0), (2, 100, 4.0)])
    set_anchors(client, 1, 0, "2026-01-01T00:00:00Z", 100, "2026-01-01T01:00:00Z")
    # 段 2 的锚点只覆盖 10..100，首点 0 落在范围外
    set_anchors(client, 2, 10, "2026-01-01T01:30:00Z", 100, "2026-01-01T02:30:00Z")
    c = check(client, 1, 2)
    assert c["status"] == "pending"
    assert c["next"]["utc_time"] is None
    assert seq(client)["summary"]["pass"] == 0


def test_single_segment_has_no_checks_and_is_deliverable(client):
    upload(client, [(7, 0, 1.0), (7, 10, 2.0)])
    set_anchors(client, 7, 0, "2026-01-01T00:00:00Z", 10, "2026-01-01T00:10:00Z")
    body = seq(client)
    assert body["checks"] == []
    assert delivery(client, 7)["status"] == "deliverable"


def test_segment_order_is_numeric(client):
    # 段号 10 在 2 之后，即便上传顺序相反
    upload(client, [(10, 0, 1.0), (10, 10, 1.5), (2, 0, 2.0), (2, 10, 2.5)])
    set_anchors(client, 2, 0, "2026-01-01T00:00:00Z", 10, "2026-01-01T00:10:00Z")
    set_anchors(client, 10, 0, "2026-01-01T00:20:00Z", 10, "2026-01-01T00:30:00Z")
    pairs = [(c["prev_segment"], c["next_segment"]) for c in seq(client)["checks"]]
    assert pairs == [(2, 10)]


# ---------------------------------------------------------------- 更正联动

def test_anchor_correction_updates_sequence_and_delivery_atomically(client):
    seed_two_segments(client, seg2_start="2026-01-01T00:30:00Z")
    assert check(client, 1, 2)["status"] == "conflict"
    v_before = seq(client)["calc_version"]

    set_anchors(client, 2, 0, "2026-01-01T01:30:00Z", 100, "2026-01-01T02:30:00Z",
                version=1)
    body = seq(client)
    assert body["calc_version"] > v_before
    assert body["checks"][0]["status"] == "pass"
    assert delivery(client, 1)["status"] == "deliverable"
    # 同日读数也随之整体换版，版本号与核查一致
    day = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    assert day["calc_version"] == body["calc_version"]
    assert day["sequence"]["calc_version"] == body["calc_version"]
    assert day["blocked_count"] == 0


def test_upload_creating_conflict_updates_delivery(client):
    upload(client, [(1, 100, 2.0), (2, 0, 3.0)])
    set_anchors(client, 1, 0, "2026-01-01T00:00:00Z", 1000, "2026-01-01T10:00:00Z")
    set_anchors(client, 2, 0, "2026-01-01T01:30:00Z", 100, "2026-01-01T02:30:00Z")
    assert check(client, 1, 2)["status"] == "pass"   # 段 1 末点 01:00
    assert delivery(client, 1)["status"] == "deliverable"

    # 新上传的点成为段 1 末点（05:00），晚于段 2 首点 → 冲突，两段同时转不可交付
    assert upload(client, [(1, 500, 2.5)]).status_code == 200
    c = check(client, 1, 2)
    assert c["status"] == "conflict"
    assert c["prev"]["local_seconds"] == 500
    assert c["prev"]["utc_time"] == "2026-01-01T05:00:00.000Z"
    assert delivery(client, 1)["status"] == "blocked"
    assert delivery(client, 2)["status"] == "blocked"


# ---------------------------------------------------------------- 失败 / 回看 / 重启

def test_failed_recalc_keeps_old_sequence_viewable_only(client, monkeypatch):
    seed_two_segments(client, seg2_start="2026-01-01T00:30:00Z")
    old = seq(client)
    assert old["checks"][0]["status"] == "conflict"
    old_version = old["calc_version"]

    from app import calibration

    def boom(readings, anchors):
        raise RuntimeError("模拟计算失败")

    monkeypatch.setattr(calibration, "compute_calibration", boom)
    # 更正锚点本应消除冲突，但重算失败：旧结论原样保留、标记未追平
    assert set_anchors(client, 2, 0, "2026-01-01T01:30:00Z",
                       100, "2026-01-01T02:30:00Z", version=1).status_code == 200
    st = client.get("/api/status").json()["calc"]
    assert st["status"] == "failed" and st["dirty"]

    body = seq(client)
    assert body["calc_version"] == old_version
    assert body["stale"] is True
    assert body["checks"][0]["status"] == "conflict"  # 不得出现半版新结论
    day = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    assert day["calc_version"] == old_version
    assert day["blocked_count"] == 6

    monkeypatch.undo()
    assert client.post("/api/recalc").status_code == 200
    body = seq(client)
    assert body["calc_version"] > old_version
    assert body["stale"] is False
    assert body["checks"][0]["status"] == "pass"


def test_restart_recovers_unfinished_sequence(tmp_path, monkeypatch):
    from app import calibration
    from app.main import create_app

    monkeypatch.setenv("DB_PATH", str(tmp_path / "s.db"))
    monkeypatch.setenv("WORKER_ENABLED", "0")
    orig = calibration.RecalcWorker.run_until_quiet
    monkeypatch.setattr(calibration.RecalcWorker, "run_until_quiet", lambda self: None)
    with __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(create_app()) as c1:
        upload(c1, [(1, 0, 1.0), (1, 100, 2.0), (2, 0, 3.0), (2, 100, 4.0)])
        set_anchors(c1, 1, 0, "2026-01-01T00:00:00Z", 100, "2026-01-01T01:00:00Z")
        set_anchors(c1, 2, 0, "2026-01-01T01:30:00Z", 100, "2026-01-01T02:30:00Z")
        body = c1.get("/api/sequence").json()
        assert body["calc_version"] == 0 and body["stale"] is True
        assert body["checks"] == []          # 未追平：没有可展示的结论，而不是“通过”

    monkeypatch.setattr(calibration.RecalcWorker, "run_until_quiet", orig)
    monkeypatch.setenv("WORKER_ENABLED", "1")
    with __import__("fastapi.testclient", fromlist=["TestClient"]).TestClient(create_app()) as c2:
        wait_idle(c2)
        body = c2.get("/api/sequence").json()
        assert body["stale"] is False
        assert body["checks"][0]["status"] == "pass"
        assert delivery(c2, 1)["status"] == "deliverable"


def test_interleaved_upload_keeps_sequence_and_readings_in_one_version(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch, worker="1", delay="0.6") as c:
        upload(c, [(1, 0, 1.0), (2, 0, 3.0)])
        set_anchors(c, 1, 0, "2026-01-01T00:00:00Z", 100, "2026-01-01T01:00:00Z")
        set_anchors(c, 2, 0, "2026-01-01T01:30:00Z", 100, "2026-01-01T02:30:00Z")
        wait_idle(c)
        upload(c, [(1, 100, 2.0)])
        t0 = time.time()
        while time.time() - t0 < 5:
            if c.get("/api/status").json()["calc"]["status"] == "running":
                break
            time.sleep(0.02)
        upload(c, [(2, 100, 4.0)])
        st = wait_idle(c)
        body = seq(c)
        day = c.get("/api/readings", params={"date": "2026-01-01"}).json()
        assert body["calc_version"] == day["calc_version"] == st["version"]
        assert body["checks"][0]["status"] == "pass"
        assert body["checks"][0]["prev"]["local_seconds"] == 100
        assert body["checks"][0]["next"]["local_seconds"] == 0


# ---------------------------------------------------------------- 页面

def test_sequence_page_shows_conflict_batch_row_and_times(client):
    seed_two_segments(client, seg2_start="2026-01-01T00:30:00Z")
    r = client.get("/sequence")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    html = r.text
    assert "时序核查" in html and "冲突" in html
    assert "2026-01-01T01:00:00.000Z" in html and "2026-01-01T00:30:00.000Z" in html
    assert "不可交付" in html
    # 按日查询页同样标注
    q = client.get("/query?date=2026-01-01").text
    assert "不可交付" in q and "供数资格" in q


def test_status_and_nav_include_sequence(client):
    upload(client, [(1, 0, 1.0)])
    html = client.get("/").text
    assert "时序核查" in html and 'href="/sequence"' in html


def test_stale_view_never_labels_pass_or_deliverable(client, monkeypatch):
    seed_two_segments(client)
    assert seq(client)["checks"][0]["status"] == "pass"

    from app import calibration

    monkeypatch.setattr(calibration, "build_sequence",
                        lambda r, k: (_ for _ in ()).throw(RuntimeError("模拟核查失败")))
    assert set_anchors(client, 2, 0, "2026-01-01T00:30:00Z",
                       100, "2026-01-01T02:30:00Z", version=1).status_code == 200

    body = seq(client)
    assert body["stale"] is True
    assert body["segments"][0]["delivery"]["status"] == "stale"
    assert body["summary"]["deliverable_segments"] == []
    day = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    assert day["blocked_count"] == day["count"] > 0
    assert all(r["delivery_status"] == "stale" and not r["deliverable"]
               for r in day["readings"])
    html = client.get("/sequence").text
    assert "仅可回看" in html
    assert '<span class="ok">通过' not in html
    assert '可交付</span>' not in html


def test_empty_state_has_no_false_pass(client):
    body = seq(client)
    assert body["checks"] == [] and body["segments"] == []
    assert body["summary"]["deliverable_segments"] == []
    r = client.get("/sequence")
    assert r.status_code == 200
    assert "尚无读数" in r.text or "不足两个开机段" in r.text


def test_upgraded_database_forces_recalc(tmp_path, monkeypatch):
    """旧库没有时序结论时不能当作“通过”展示：升级后强制补算一次。"""
    from app.db import Database

    path = str(tmp_path / "old.db")
    db = Database(path)
    # 模拟旧库：已发布过校时版（version > 0），但没有时序核查表标记
    db.execute("UPDATE calc_state SET version = 3 WHERE id = 1")
    db.execute("UPDATE meta SET value = '7' WHERE key = 'state_generation'")
    db.execute("UPDATE meta SET value = '7' WHERE key = 'calibrated_generation'")
    db.execute("UPDATE meta SET value = '0' WHERE key = 'sequence_schema'")
    db.close()

    db2 = Database(path)
    meta = {r["key"]: r["value"] for r in db2.query("SELECT key, value FROM meta")}
    assert meta["state_generation"] == "8"
    assert meta["calibrated_generation"] == "7"
    assert meta["sequence_schema"] == "1"
    db2.close()

    # 全新库不应因迁移白白重算
    fresh = str(tmp_path / "fresh.db")
    db3 = Database(fresh)
    meta = {r["key"]: r["value"] for r in db3.query("SELECT key, value FROM meta")}
    assert meta["state_generation"] == "0" and meta["sequence_schema"] == "1"
    db3.close()


def test_readings_api_exposes_sequence_flags(client):
    seed_two_segments(client, seg2_start="2026-01-01T00:30:00Z")
    body = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    assert body["sequence"]["checks"][0]["status"] == "conflict"
    assert body["stale"] is False
    assert all({"deliverable", "delivery_status", "delivery_reason"} <= set(r)
               for r in body["readings"])
    # 字段完整：批次号 / 原行号
    check = body["sequence"]["checks"][0]
    assert check["prev"]["batch_id"] == 1 and check["prev"]["row_no"] == 3
    assert check["next"]["batch_id"] == 1 and check["next"]["row_no"] == 4
