
import threading

import pytest
from fastapi.testclient import TestClient

import app as appmod
from app import app, init_db, consumer_lock


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _login(c):
    r = c.post("/api/login", json={"username": "admin", "password": "123456"})
    assert r.status_code == 200
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _make_released_slice(c, h, slice_no="SL-T-1", biz_date="2026-11-04", consumer="ads_x"):
    ok = c.post("/api/slices", headers=h, json={
        "slice_no": slice_no, "biz_date": biz_date, "partition_key": "mid",
        "row_count": 10, "expected_count": 10})
    assert ok.status_code == 200
    sid = ok.json()["id"]
    rel = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": consumer})
    assert rel.status_code == 200
    return sid


def _lock_policy(c, h, caliber):
    r = c.post("/api/rollback/policy", headers=h, json={"caliber": caliber, "reason": "验收用例"})
    assert r.status_code == 200
    return r


def test_rollback_requires_policy_and_logs_deviation():
    c = TestClient(app)
    h = _login(c)
    _make_released_slice(c, h)
    # 规则簿未择口径即回拨：拒绝并按「回拨口径偏了」留痕
    r = c.post("/api/rollback", headers=h, json={"consumer": "ads_x", "target_biz_date": "2026-11-04"})
    assert r.status_code == 409
    assert "回拨口径偏了" in r.json()["detail"]
    ledger = c.get("/api/rollback", headers=h).json()["items"]
    assert any(x["result"] == "拒绝" and "口径偏了" in x["reason"] for x in ledger)


def test_policy_locked_today_cannot_change_and_deviation_is_recorded():
    c = TestClient(app)
    h = _login(c)
    _lock_policy(c, h, "软回拨")
    # 当日改口为乙口径：拒绝
    r = c.post("/api/rollback/policy", headers=h, json={"caliber": "硬回拨作废", "reason": "改口"})
    assert r.status_code == 409
    assert "禁止改口" in r.json()["detail"]
    # 同口径重复锁定是幂等沿用，不产生新簿
    r2 = c.post("/api/rollback/policy", headers=h, json={"caliber": "软回拨"})
    assert r2.status_code == 200 and r2.json()["caliber"] == "软回拨"
    p = c.get("/api/rollback/policy", headers=h).json()
    assert p["today"]["caliber"] == "软回拨"
    ledger = c.get("/api/rollback", headers=h).json()["items"]
    assert any("回拨口径偏了" in x["reason"] for x in ledger)


def test_soft_rollback_is_atomic_across_watermark_slice_and_history():
    c = TestClient(app)
    h = _login(c)
    sid = _make_released_slice(c, h)
    _lock_policy(c, h, "软回拨")
    r = c.post("/api/rollback", headers=h, json={"consumer": "ads_x", "target_biz_date": "2026-11-04"})
    assert r.status_code == 200
    body = r.json()
    assert body["caliber"] == "软回拨" and body["affected_releases"] == 1
    # 切片改回可复检未放行，不再显示已放行
    s = next(x for x in c.get("/api/slices", headers=h).json()["items"] if x["id"] == sid)
    assert s["release_status"] == "可复检未放行"
    # 成功流水保留（只读史）并标注已回拨，不得抹掉
    rels = [x for x in c.get("/api/releases", headers=h).json()["items"] if x["slice_no"] == "SL-T-1"]
    assert len(rels) == 1 and rels[0]["status"] == "成功" and rels[0]["lifecycle"] == "已回拨(软)"
    # 水位标软回拨，侧栏当前有效水位不再包含该业务日
    wm = c.get("/api/watermarks", headers=h).json()
    w = next(x for x in wm["items"] if x["source_slice"] == "SL-T-1")
    assert w["status"] == "已回拨(软)" and w["is_current"] is False
    assert all(cur["biz_date"] < "2026-11-04" for cur in wm["current"] if cur["consumer"] == "ads_x")
    # 软回拨后切片可复检并再次成功放行
    again = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_x"})
    assert again.status_code == 200
    s2 = next(x for x in c.get("/api/slices", headers=h).json()["items"] if x["id"] == sid)
    assert s2["release_status"] == "已放行"


def test_hard_rollback_voids_and_blocks_same_slice_no_rerelease():
    c = TestClient(app)
    h = _login(c)
    sid = _make_released_slice(c, h, slice_no="SL-HARD-1")
    _lock_policy(c, h, "硬回拨作废")
    r = c.post("/api/rollback", headers=h, json={"consumer": "ads_x", "target_biz_date": "2026-11-04"})
    assert r.status_code == 200 and r.json()["caliber"] == "硬回拨作废"
    s = next(x for x in c.get("/api/slices", headers=h).json()["items"] if x["id"] == sid)
    assert s["release_status"] == "硬作废-须新切片"
    rels = [x for x in c.get("/api/releases", headers=h).json()["items"] if x["slice_no"] == "SL-HARD-1"]
    assert rels[0]["lifecycle"] == "硬作废"
    w = next(x for x in c.get("/api/watermarks", headers=h).json()["items"] if x["source_slice"] == "SL-HARD-1")
    assert w["status"] == "已撤回(硬)"
    # 同一切片号禁止再次成功放行
    blocked = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_x"})
    assert blocked.status_code == 409 and "禁止再次放行" in blocked.json()["detail"]
    # 须新切片重走完整度：不完整先拒，完整后可放行
    bad = c.post("/api/slices", headers=h, json={
        "slice_no": "SL-HARD-1-R", "biz_date": "2026-11-04", "partition_key": "mid",
        "row_count": 3, "expected_count": 10})
    nsid = bad.json()["id"]
    assert c.post(f"/api/slices/{nsid}/release", headers=h, json={"consumer": "ads_x"}).status_code == 409
    ok2 = c.post("/api/slices", headers=h, json={
        "slice_no": "SL-HARD-2", "biz_date": "2026-11-04", "partition_key": "mid",
        "row_count": 10, "expected_count": 10})
    nsid2 = ok2.json()["id"]
    assert c.post(f"/api/slices/{nsid2}/release", headers=h, json={"consumer": "ads_x"}).status_code == 200


def test_empty_rollback_rejected_and_logged():
    c = TestClient(app)
    h = _login(c)
    _lock_policy(c, h, "软回拨")
    r = c.post("/api/rollback", headers=h, json={"consumer": "ghost_consumer", "target_biz_date": "2026-11-04"})
    assert r.status_code == 409 and "空回拨" in r.json()["detail"]
    ledger = c.get("/api/rollback", headers=h).json()["items"]
    assert any(x["result"] == "拒绝" and "空回拨" in x["reason"] for x in ledger)


def test_invalid_caliber_and_bad_date_rejected():
    c = TestClient(app)
    h = _login(c)
    r = c.post("/api/rollback/policy", headers=h, json={"caliber": "随便混"})
    assert r.status_code == 422
    _lock_policy(c, h, "软回拨")
    r2 = c.post("/api/rollback", headers=h, json={"consumer": "ads_x", "target_biz_date": "11/04"})
    assert r2.status_code == 422


def test_concurrent_rollback_lock_second_hand_rejected_and_logged():
    c = TestClient(app)
    h = _login(c)
    _make_released_slice(c, h)
    _lock_policy(c, h, "软回拨")
    lk = consumer_lock("ads_x")
    assert lk.acquire(blocking=False)
    try:
        # 后手在锁占用期间回拨：直接争用失败，不默许
        r = c.post("/api/rollback", headers=h, json={"consumer": "ads_x", "target_biz_date": "2026-11-04"})
        assert r.status_code == 409 and "回拨争用禁默许" in r.json()["detail"]
    finally:
        lk.release()
    ledger = c.get("/api/rollback", headers=h).json()["items"]
    assert any(x["result"] == "拒绝" and "回拨争用禁默许" in x["reason"] for x in ledger)
    # 锁释放后同口径回拨可成功（串行，不丢账）
    ok = c.post("/api/rollback", headers=h, json={"consumer": "ads_x", "target_biz_date": "2026-11-04"})
    assert ok.status_code == 200


def test_parallel_rollback_requests_serialized_one_wins_one_contention():
    c = TestClient(app)
    h = _login(c)
    _make_released_slice(c, h)
    _lock_policy(c, h, "软回拨")
    results = []

    def fire():
        r = c.post("/api/rollback", headers=h, json={"consumer": "ads_x", "target_biz_date": "2026-11-04"})
        results.append(r.status_code)

    t1 = threading.Thread(target=fire)
    t2 = threading.Thread(target=fire)
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(results) == [200, 409]
    ledger = c.get("/api/rollback", headers=h).json()["items"]
    assert any(x["result"] == "成功" for x in ledger)
    assert any("回拨争用禁默许" in x["reason"] for x in ledger)
    # 只成功一次：没有重复回拨、没有重复标注
    rels = [x for x in c.get("/api/releases", headers=h).json()["items"] if x["slice_no"] == "SL-T-1"]
    assert len([x for x in rels if x["lifecycle"] == "已回拨(软)"]) == 1


def test_rollback_only_hits_target_window_and_other_consumers_untouched():
    c = TestClient(app)
    h = _login(c)
    _make_released_slice(c, h, slice_no="SL-A", biz_date="2026-11-05", consumer="ads_x")
    _make_released_slice(c, h, slice_no="SL-B", biz_date="2026-11-03", consumer="ads_x")
    _make_released_slice(c, h, slice_no="SL-C", biz_date="2026-11-05", consumer="ads_y")
    _lock_policy(c, h, "软回拨")
    r = c.post("/api/rollback", headers=h, json={"consumer": "ads_x", "target_biz_date": "2026-11-05"})
    assert r.status_code == 200
    assert r.json()["affected_slices"] == ["SL-A"]
    relmap = {x["slice_no"]: x["lifecycle"] for x in c.get("/api/releases", headers=h).json()["items"]}
    assert relmap["SL-A"] == "已回拨(软)"
    assert relmap["SL-B"] == "有效"
    assert relmap["SL-C"] == "有效"


def test_ledger_and_policy_survive_second_connection_same_db(tmp_path):
    # 同库可核：换一个 TestClient（新连接）仍能读到规则簿与回拨目录
    c1 = TestClient(app)
    h1 = _login(c1)
    _make_released_slice(c1, h1)
    _lock_policy(c1, h1, "硬回拨作废")
    assert c1.post("/api/rollback", headers=h1, json={"consumer": "ads_x", "target_biz_date": "2026-11-04"}).status_code == 200
    c2 = TestClient(app)
    h2 = _login(c2)
    pol = c2.get("/api/rollback/policy", headers=h2).json()
    assert pol["today"]["caliber"] == "硬回拨作废"
    ledger = c2.get("/api/rollback", headers=h2).json()["items"]
    assert any(x["result"] == "成功" and x["caliber"] == "硬回拨作废" for x in ledger)
    # 新连接同样受当日口径约束，混甲口径也拿不到别的执行路径（簿锁的是乙）
    assert c2.post("/api/rollback/policy", headers=h2, json={"caliber": "软回拨"}).status_code == 409
