"""SQLite 数据访问层。

单连接 + 可重入锁串行化所有访问；写操作走 ``tx()``（BEGIN IMMEDIATE 事务）。
数据量（≤2000 点）极小，简单可靠优先。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

from .timeutil import utcnow_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    filename      TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    rows_accepted INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS readings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id      INTEGER NOT NULL REFERENCES batches(id),
    row_no        INTEGER NOT NULL,          -- CSV 内的原始行号（数据行从 1 起）
    segment       INTEGER NOT NULL,          -- 开机段号
    local_seconds INTEGER NOT NULL,          -- 记录仪本地秒数
    temperature   REAL NOT NULL,
    created_at    TEXT NOT NULL,
    UNIQUE (segment, local_seconds)          -- 同段秒数不能重复
);

CREATE TABLE IF NOT EXISTS anchors (
    segment    INTEGER PRIMARY KEY,
    local1     INTEGER NOT NULL,
    utc1       TEXT NOT NULL,
    local2     INTEGER NOT NULL,
    utc2       TEXT NOT NULL,
    version    INTEGER NOT NULL,             -- 每次更正 +1，更正须携带所见版本
    updated_at TEXT NOT NULL
);

-- 当前生效的校时结果（整体换版，永远完整）
CREATE TABLE IF NOT EXISTS calibrated (
    reading_id     INTEGER PRIMARY KEY REFERENCES readings(id),
    utc_time       TEXT NOT NULL,
    utc_date       TEXT NOT NULL,
    anchor_version INTEGER NOT NULL,
    calc_version   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_calibrated_date ON calibrated(utc_date);

CREATE TABLE IF NOT EXISTS calc_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    version    INTEGER NOT NULL,             -- 校时结果版本号，单调递增
    status     TEXT NOT NULL,                -- idle / running / failed
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

-- 相邻开机段时序核查：与 calibrated / calc_state 在同一事务内整体换版。
-- 段号即重启顺序；只保存当前已发布校时版的结论，旧版不保留。
CREATE TABLE IF NOT EXISTS sequence_segments (
    calc_version        INTEGER NOT NULL,
    segment             INTEGER NOT NULL,
    status              TEXT NOT NULL,       -- ok / conflict / pending
    reason              TEXT NOT NULL DEFAULT '',
    point_count         INTEGER NOT NULL,
    calibrated_count    INTEGER NOT NULL,
    first_reading_id    INTEGER NOT NULL,
    first_batch_id      INTEGER NOT NULL,
    first_row_no        INTEGER NOT NULL,
    first_local_seconds INTEGER NOT NULL,
    first_utc           TEXT,                -- NULL 表示该边界读数未校时
    first_anchor_version INTEGER,
    last_reading_id     INTEGER NOT NULL,
    last_batch_id       INTEGER NOT NULL,
    last_row_no         INTEGER NOT NULL,
    last_local_seconds  INTEGER NOT NULL,
    last_utc            TEXT,                -- NULL 表示该边界读数未校时
    last_anchor_version INTEGER,
    PRIMARY KEY (calc_version, segment)
);

CREATE TABLE IF NOT EXISTS sequence_checks (
    calc_version        INTEGER NOT NULL,
    prev_segment        INTEGER NOT NULL,
    next_segment        INTEGER NOT NULL,
    status              TEXT NOT NULL,       -- pass / conflict / pending
    reason              TEXT NOT NULL DEFAULT '',
    prev_reading_id     INTEGER NOT NULL,
    prev_batch_id       INTEGER NOT NULL,
    prev_row_no         INTEGER NOT NULL,
    prev_local_seconds  INTEGER NOT NULL,
    prev_utc            TEXT,
    prev_anchor_version INTEGER,
    next_reading_id     INTEGER NOT NULL,
    next_batch_id       INTEGER NOT NULL,
    next_row_no         INTEGER NOT NULL,
    next_local_seconds  INTEGER NOT NULL,
    next_utc            TEXT,
    next_anchor_version INTEGER,
    PRIMARY KEY (calc_version, prev_segment, next_segment)
);
"""


class Database:
    def __init__(self, path: str):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL")
        self._lock = threading.RLock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('state_generation', '0')"
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('calibrated_generation', '-1')"
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO calc_state(id, version, status, updated_at)"
                " VALUES (1, 0, 'idle', ?)",
                (utcnow_iso(),),
            )
            # 时序核查表随本次升级引入：已有历史结论的旧库必须强制补算一次，
            # 避免把「没有核查结果」误当成「通过」。全新库无需额外触发。
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('sequence_schema', '0')"
            )
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'sequence_schema'"
            ).fetchone()
            if row["value"] != "1":
                published = self._conn.execute(
                    "SELECT version FROM calc_state WHERE id = 1"
                ).fetchone()["version"]
                if published > 0:
                    self._conn.execute(
                        "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)"
                        " WHERE key = 'state_generation'"
                    )
                self._conn.execute(
                    "UPDATE meta SET value = '1' WHERE key = 'sequence_schema'"
                )
            # 上次进程在重算中途退出：状态不能继续谎称“重算中”，
            # 记为上轮未完成，旧版本仍完整，等待本次启动补算。
            self._conn.execute(
                "UPDATE calc_state SET status = 'failed',"
                " last_error = '上次重算未完成（服务中断），旧版结果仅可回看，等待补算',"
                " updated_at = ? WHERE id = 1 AND status = 'running'",
                (utcnow_iso(),),
            )

    @contextmanager
    def tx(self):
        """写事务：BEGIN IMMEDIATE，异常自动回滚（不留半版）。"""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                yield cur
                cur.execute("COMMIT")
            except BaseException:
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()

    def snapshot(self, fn):
        """在锁内执行只读操作，保证读到一致快照。"""
        with self._lock:
            return fn(self._conn)

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(sql, params)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
