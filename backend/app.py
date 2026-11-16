
import hashlib, secrets, sqlite3, threading
from datetime import date
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 同一消费方水位回拨串行锁（进程内互斥；写库另有 BEGIN IMMEDIATE 兜底）
WM_LOCKS: dict[str, threading.Lock] = {}
WM_LOCKS_GUARD = threading.Lock()

CALIBER_SOFT = "软回拨"
CALIBER_HARD = "硬回拨作废"
VALID_CALIBERS = (CALIBER_SOFT, CALIBER_HARD)

def consumer_lock(consumer: str) -> threading.Lock:
    with WM_LOCKS_GUARD:
        lk = WM_LOCKS.get(consumer)
        if lk is None:
            lk = threading.Lock()
            WM_LOCKS[consumer] = lk
        return lk

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

def _has_column(conn, table: str, column: str) -> bool:
    return any(r["name"] == column for r in conn.execute(f"PRAGMA table_info({table})"))

def _add_column(conn, table: str, column: str, ddl: str):
    if not _has_column(conn, table, column):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from typing import Any
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "日切放行"
SKELETON = "业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS slices(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_no TEXT UNIQUE NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          expected_count INTEGER NOT NULL,
          completeness TEXT NOT NULL,
          release_status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS releases(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermarks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          consumer TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          watermark TEXT NOT NULL,
          source_slice TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rollback_policy(
          biz_date TEXT PRIMARY KEY,
          caliber TEXT NOT NULL,
          locked_at TEXT NOT NULL,
          locked_by TEXT NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rollbacks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          rollback_no TEXT UNIQUE NOT NULL,
          consumer TEXT NOT NULL,
          target_biz_date TEXT NOT NULL,
          caliber TEXT NOT NULL,
          result TEXT NOT NULL,
          reason TEXT NOT NULL,
          affected_release_ids TEXT NOT NULL,
          affected_slice_nos TEXT NOT NULL,
          operator TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        # 存量库平滑迁移：放行流水/水位增加生命周期与回拨留痕列
        _add_column(conn, "releases", "lifecycle", "TEXT NOT NULL DEFAULT '有效'")
        _add_column(conn, "releases", "rollback_id", "INTEGER")
        _add_column(conn, "releases", "rollback_caliber", "TEXT")
        _add_column(conn, "watermarks", "status", "TEXT NOT NULL DEFAULT '有效'")
        _add_column(conn, "watermarks", "rollback_id", "INTEGER")
        _add_column(conn, "watermarks", "rollback_caliber", "TEXT")
        if conn.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"] == 0:
            rows = [
                ("SL-2026-11-01-east", "2026-11-01", "east", 980, 1000, "部分完整", "未放行"),
                ("SL-2026-11-01-west", "2026-11-01", "west", 1000, 1000, "完整", "已放行"),
                ("SL-2026-11-02-east", "2026-11-02", "east", 0, 1000, "待检", "未放行"),
            ]
            for r in rows:
                conn.execute("INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)", r)
            conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (2,'ads_order','成功','完整切片放行','2026-11-01T18:00:00')")
            conn.execute("INSERT INTO watermarks(consumer,biz_date,watermark,source_slice) VALUES ('ads_order','2026-11-01','2026-11-01T18:00:00','SL-2026-11-01-west')")

@app.on_event("startup")
def _start():
    init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"]
        p = c.execute("SELECT caliber FROM rollback_policy WHERE biz_date=?", (date.today().isoformat(),)).fetchone()
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "slices": n,
            "db": str(DB_PATH), "today_caliber": p["caliber"] if p else None}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username, role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/slices")
def list_slices(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM slices ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}

class SliceIn(BaseModel):
    slice_no: str
    biz_date: str
    partition_key: str
    row_count: int
    expected_count: int

@app.post("/api/slices")
def add_slice(body: SliceIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    comp = "完整" if body.row_count >= body.expected_count and body.expected_count > 0 else ("部分完整" if body.row_count > 0 else "待检")
    with connect() as c:
        try:
            cur = c.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)",
                (body.slice_no, body.biz_date, body.partition_key, body.row_count, body.expected_count, comp, "未放行"),
            )
            return {"id": cur.lastrowid, "completeness": comp}
        except sqlite3.IntegrityError:
            raise HTTPException(409, "切片号已存在")

@app.post("/api/slices/{sid}/evaluate")
def evaluate(sid: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        comp = "完整" if row["row_count"] >= row["expected_count"] and row["expected_count"] > 0 else ("部分完整" if row["row_count"] > 0 else "待检")
        c.execute("UPDATE slices SET completeness=? WHERE id=?", (comp, sid))
    return {"id": sid, "completeness": comp}

class ReleaseIn(BaseModel):
    consumer: str

@app.post("/api/slices/{sid}/release")
def release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    from datetime import datetime
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        # 乙口径硬作废的切片：同一切片号禁止再次成功放行，须新切片重走完整度
        voided = c.execute(
            "SELECT 1 FROM releases WHERE slice_id=? AND lifecycle='硬作废' LIMIT 1", (sid,)).fetchone()
        if voided:
            raise HTTPException(409, "该切片已硬回拨作废，同一切片号禁止再次放行，请新建切片重走完整度")
        if row["completeness"] != "完整":
            c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                      (sid, body.consumer, "拒绝", "完整度未达标", datetime.now().isoformat(timespec="seconds")))
            c.commit()
            raise HTTPException(409, "未完整禁止放行")
        c.execute("UPDATE slices SET release_status='已放行' WHERE id=?", (sid,))
        c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                  (sid, body.consumer, "成功", "完整切片放行", datetime.now().isoformat(timespec="seconds")))
        c.execute("INSERT INTO watermarks(consumer,biz_date,watermark,source_slice) VALUES (?,?,?,?)",
                  (body.consumer, row["biz_date"], datetime.now().isoformat(timespec="seconds"), row["slice_no"]))
    return {"ok": True}

@app.get("/api/releases")
def list_releases(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT r.*, s.slice_no FROM releases r
                            JOIN slices s ON s.id=r.slice_id ORDER BY r.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows]}

@app.get("/api/watermarks")
def list_wm(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM watermarks ORDER BY id DESC").fetchall()
        items = [dict(r) for r in rows]
        current = c.execute("""SELECT consumer, MAX(biz_date) AS biz_date FROM watermarks
                                WHERE status='有效' GROUP BY consumer""").fetchall()
        cur_map = {r["consumer"]: r["biz_date"] for r in current}
        for it in items:
            it["is_current"] = (it["status"] == "有效" and cur_map.get(it["consumer"]) == it["biz_date"])
    return {"items": items, "current": [{"consumer": k, "biz_date": v} for k, v in sorted(cur_map.items())]}

# ---------------- 水位回拨：规则簿 + 同库账本 + 互斥口径 + 串行锁 ----------------

class PolicyIn(BaseModel):
    caliber: str
    reason: str = ""

def _today() -> str:
    return date.today().isoformat()

@app.get("/api/rollback/policy")
def get_policy(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM rollback_policy ORDER BY biz_date DESC").fetchall()
        today_row = c.execute("SELECT * FROM rollback_policy WHERE biz_date=?", (_today(),)).fetchone()
    return {"items": [dict(r) for r in rows], "today": dict(today_row) if today_row else None}

@app.post("/api/rollback/policy")
def set_policy(body: PolicyIn, authorization: str | None = Header(default=None)):
    user = require_user(authorization)
    if body.caliber not in VALID_CALIBERS:
        raise HTTPException(422, "口径必须是软回拨或硬回拨作废，二者互斥择一")
    from datetime import datetime
    today = _today()
    with connect() as c:
        c.execute("BEGIN IMMEDIATE")
        existing = c.execute("SELECT * FROM rollback_policy WHERE biz_date=?", (today,)).fetchone()
        if existing:
            if existing["caliber"] == body.caliber:
                c.commit()
                return {"ok": True, "locked": True, "caliber": existing["caliber"], "message": "今日口径已锁定，沿用既有口径"}
            # 当日改口：拒绝并按偏口径记入账本
            no = "RB-POLICY-" + secrets.token_hex(4)
            c.execute("""INSERT INTO rollbacks(rollback_no,consumer,target_biz_date,caliber,result,reason,
                        affected_release_ids,affected_slice_nos,operator,created_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?)""",
                      (no, "-", today, body.caliber, "拒绝",
                       f"回拨口径偏了：今日已锁定{existing['caliber']}，禁止改口为{body.caliber}",
                       "[]", "[]", user, datetime.now().isoformat(timespec="seconds")))
            c.commit()
            raise HTTPException(409, f"回拨口径偏了：今日口径已锁定为{existing['caliber']}，当日禁止改口")
        c.execute("INSERT INTO rollback_policy(biz_date,caliber,locked_at,locked_by,reason) VALUES (?,?,?,?,?)",
                  (today, body.caliber, datetime.now().isoformat(timespec="seconds"), user, body.reason))
        c.commit()
    return {"ok": True, "locked": True, "caliber": body.caliber}

class RollbackIn(BaseModel):
    consumer: str
    target_biz_date: str
    note: str = ""

def _record_rollback(c, *, no, consumer, target, caliber, result, reason, rel_ids, slice_nos, user):
    from datetime import datetime
    import json
    c.execute("""INSERT INTO rollbacks(rollback_no,consumer,target_biz_date,caliber,result,reason,
                affected_release_ids,affected_slice_nos,operator,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
              (no, consumer, target, caliber, result, reason,
               json.dumps(rel_ids, ensure_ascii=False), json.dumps(slice_nos, ensure_ascii=False),
               user, datetime.now().isoformat(timespec="seconds")))
    return c.execute("SELECT id FROM rollbacks WHERE rollback_no=?", (no,)).fetchone()["id"]

@app.post("/api/rollback")
def do_rollback(body: RollbackIn, authorization: str | None = Header(default=None)):
    user = require_user(authorization)
    from datetime import datetime
    consumer = body.consumer.strip()
    target = body.target_biz_date.strip()
    if not consumer:
        raise HTTPException(422, "消费方不能为空")
    try:
        date.fromisoformat(target)
    except ValueError:
        raise HTTPException(422, "目标业务日格式应为 YYYY-MM-DD")

    no = "RB-" + datetime.now().strftime("%Y%m%d%H%M%S") + "-" + secrets.token_hex(3)
    lk = consumer_lock(consumer)
    if not lk.acquire(blocking=False):
        # 后手争用：拒绝、不默许，并写入回拨账本
        with connect() as c:
            rid = _record_rollback(c, no=no, consumer=consumer, target=target, caliber="-",
                                   result="拒绝", reason="回拨争用禁默许：同一消费方已有回拨在执行",
                                   rel_ids=[], slice_nos=[], user=user)
        raise HTTPException(409, "回拨争用禁默许：该消费方正有另一笔回拨执行中，本笔已拒绝并留痕")
    try:
        with connect() as c:
            c.execute("BEGIN IMMEDIATE")
            today = _today()
            pol = c.execute("SELECT * FROM rollback_policy WHERE biz_date=?", (today,)).fetchone()
            if not pol:
                rid = _record_rollback(c, no=no, consumer=consumer, target=target, caliber="-",
                                       result="拒绝", reason="回拨口径偏了：今日尚未在回拨规则簿择定口径，禁止回拨",
                                       rel_ids=[], slice_nos=[], user=user)
                c.commit()
                raise HTTPException(409, "回拨口径偏了：请先在回拨规则簿择定今日口径（软回拨/硬回拨作废互斥择一）")
            caliber = pol["caliber"]

            rels = c.execute("""SELECT r.id, r.slice_id, s.slice_no, s.release_status
                                FROM releases r JOIN slices s ON s.id=r.slice_id
                                WHERE r.consumer=? AND r.status='成功' AND r.lifecycle='有效'
                                  AND s.biz_date >= ? ORDER BY r.id""", (consumer, target)).fetchall()
            wms = c.execute("""SELECT id FROM watermarks
                               WHERE consumer=? AND status='有效' AND biz_date >= ? ORDER BY id""",
                            (consumer, target)).fetchall()
            if not rels and not wms:
                rid = _record_rollback(c, no=no, consumer=consumer, target=target, caliber=caliber,
                                       result="拒绝", reason="目标业务日之后无有效成功放行/有效水位，空回拨拒绝",
                                       rel_ids=[], slice_nos=[], user=user)
                c.commit()
                raise HTTPException(409, "无有效成功放行或有效水位可回拨，空回拨拒绝并留痕")

            rel_ids = [r["id"] for r in rels]
            slice_nos = sorted({r["slice_no"] for r in rels})

            if caliber == CALIBER_SOFT:
                # 甲口径软回拨：水位退后；切片改回可复检未放行；成功流水保留只读史并标注已回拨
                rid = _record_rollback(c, no=no, consumer=consumer, target=target, caliber=caliber,
                                       result="成功", reason=("软回拨至 " + target + (("；" + body.note) if body.note else "")),
                                       rel_ids=rel_ids, slice_nos=slice_nos, user=user)
                placeholders = ",".join("?" * len(rel_ids))
                c.execute(f"UPDATE releases SET lifecycle='已回拨(软)', rollback_id=?, rollback_caliber=? WHERE id IN ({placeholders})",
                          [rid, caliber, *rel_ids])
                wm_ids = [w["id"] for w in wms]
                c.execute(f"UPDATE watermarks SET status='已回拨(软)', rollback_id=?, rollback_caliber=? WHERE id IN ({','.join('?' * len(wm_ids))})",
                          [rid, caliber, *wm_ids])
                for r in rels:
                    active = c.execute("""SELECT COUNT(*) n FROM releases
                                          WHERE slice_id=? AND status='成功' AND lifecycle='有效'""",
                                       (r["slice_id"],)).fetchone()["n"]
                    if active == 0:
                        c.execute("UPDATE slices SET release_status='可复检未放行' WHERE id=?", (r["slice_id"],))
            else:
                # 乙口径硬回拨作废：水位撤回；相关成功放行作废；同切片号禁止再次成功放行
                rid = _record_rollback(c, no=no, consumer=consumer, target=target, caliber=caliber,
                                       result="成功", reason=("硬回拨作废至 " + target + (("；" + body.note) if body.note else "")),
                                       rel_ids=rel_ids, slice_nos=slice_nos, user=user)
                placeholders = ",".join("?" * len(rel_ids))
                c.execute(f"UPDATE releases SET lifecycle='硬作废', rollback_id=?, rollback_caliber=? WHERE id IN ({placeholders})",
                          [rid, caliber, *rel_ids])
                wm_ids = [w["id"] for w in wms]
                c.execute(f"UPDATE watermarks SET status='已撤回(硬)', rollback_id=?, rollback_caliber=? WHERE id IN ({','.join('?' * len(wm_ids))})",
                          [rid, caliber, *wm_ids])
                for r in rels:
                    active = c.execute("""SELECT COUNT(*) n FROM releases
                                          WHERE slice_id=? AND status='成功' AND lifecycle='有效'""",
                                       (r["slice_id"],)).fetchone()["n"]
                    if active == 0:
                        c.execute("UPDATE slices SET release_status='硬作废-须新切片' WHERE id=?", (r["slice_id"],))
            c.commit()
            return {"ok": True, "rollback_no": no, "caliber": caliber,
                    "affected_releases": len(rel_ids), "affected_watermarks": len(wms),
                    "affected_slices": slice_nos}
    except sqlite3.OperationalError as e:
        # 库级写锁争用同样按争用留痕，不默许
        with connect() as lc:
            _record_rollback(lc, no=no, consumer=consumer, target=target, caliber="-",
                             result="拒绝", reason="回拨争用禁默许：库写锁等待超时 " + str(e),
                             rel_ids=[], slice_nos=[], user=user)
        raise HTTPException(409, "回拨争用禁默许：库写锁争用，本笔已拒绝并留痕")
    finally:
        lk.release()

@app.get("/api/rollback")
def list_rollback(authorization: str | None = Header(default=None)):
    import json
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM rollbacks ORDER BY id DESC").fetchall()
        items = []
        for r in rows:
            d = dict(r)
            try:
                d["affected_release_ids"] = json.loads(d["affected_release_ids"])
                d["affected_slice_nos"] = json.loads(d["affected_slice_nos"])
            except (TypeError, ValueError):
                pass
            items.append(d)
    return {"items": items}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")

app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
