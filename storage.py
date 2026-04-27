"""
SQLite-backed persistent store.
- WAL mode  →  concurrent reads while writing
- asyncio.Lock  →  serialises writes safely in async context
- Every write is replicated to backup nodes via the caller (node.py)
"""

import sqlite3
import asyncio
import json
import time
import logging
from typing import Optional, List, Dict, Any

from config import DB_PATH, RESULT_TTL

log = logging.getLogger("storage")


class Storage:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path   = db_path
        self._lock     = asyncio.Lock()
        self._init_db()

    # ── Init ─────────────────────────────────────────────────────────────────

    def _init_db(self):
        conn = self._conn()
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id          TEXT PRIMARY KEY,
                submitter_id    TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending',
                matrix_A        TEXT NOT NULL,
                matrix_B        TEXT NOT NULL,
                rows_A          INTEGER NOT NULL,
                cols_A          INTEGER NOT NULL,
                cols_B          INTEGER NOT NULL,
                total_blocks    INTEGER NOT NULL,
                coordinator_id  TEXT NOT NULL,
                backup_nodes    TEXT NOT NULL DEFAULT '[]',
                created_at      REAL NOT NULL,
                expires_at      REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS blocks (
                block_id        TEXT NOT NULL,
                job_id          TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending',
                row_start       INTEGER NOT NULL,
                row_end         INTEGER NOT NULL,
                worker_id       TEXT,
                partial_result  TEXT,
                assigned_at     REAL,
                completed_at    REAL,
                compute_time_ms REAL,
                mflops          REAL,
                device_type     TEXT,
                PRIMARY KEY (block_id, job_id)
            );

            CREATE TABLE IF NOT EXISTS results (
                job_id         TEXT PRIMARY KEY,
                result_matrix  TEXT NOT NULL,
                completed_at   REAL NOT NULL,
                expires_at     REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS nodes (
                node_id     TEXT PRIMARY KEY,
                ip          TEXT NOT NULL,
                port        INTEGER NOT NULL,
                join_time   REAL NOT NULL,
                last_seen   REAL NOT NULL,
                device_type TEXT DEFAULT 'python',
                status      TEXT DEFAULT 'idle'
            );
        """)
        conn.commit()
        conn.close()
        log.info("SQLite initialised at %s (WAL mode)", self.db_path)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    # ── Job CRUD ──────────────────────────────────────────────────────────────

    async def create_job(self, job: dict) -> None:
        async with self._lock:
            conn = self._conn()
            conn.execute("""
                INSERT OR REPLACE INTO jobs
                (job_id, submitter_id, status, matrix_A, matrix_B,
                 rows_A, cols_A, cols_B, total_blocks, coordinator_id,
                 backup_nodes, created_at, expires_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                job["job_id"], job["submitter_id"], job.get("status","pending"),
                json.dumps(job["matrix_A"]), json.dumps(job["matrix_B"]),
                job["rows_A"], job["cols_A"], job["cols_B"],
                job["total_blocks"], job["coordinator_id"],
                json.dumps(job.get("backup_nodes",[])),
                job["created_at"], job["expires_at"],
            ))
            conn.commit()
            conn.close()

    async def get_job(self, job_id: str) -> Optional[dict]:
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        conn.close()
        if not row:
            return None
        d = dict(row)
        d["matrix_A"]    = json.loads(d["matrix_A"])
        d["matrix_B"]    = json.loads(d["matrix_B"])
        d["backup_nodes"]= json.loads(d["backup_nodes"])
        return d

    async def update_job_status(self, job_id: str, status: str) -> None:
        async with self._lock:
            conn = self._conn()
            conn.execute(
                "UPDATE jobs SET status=? WHERE job_id=?", (status, job_id)
            )
            conn.commit()
            conn.close()

    async def update_job_coordinator(self, job_id: str,
                                     coordinator_id: str) -> None:
        async with self._lock:
            conn = self._conn()
            conn.execute(
                "UPDATE jobs SET coordinator_id=? WHERE job_id=?",
                (coordinator_id, job_id)
            )
            conn.commit()
            conn.close()

    # ── Block CRUD ────────────────────────────────────────────────────────────

    async def create_block(self, block: dict) -> None:
        async with self._lock:
            conn = self._conn()
            conn.execute("""
                INSERT OR IGNORE INTO blocks
                (block_id, job_id, status, row_start, row_end)
                VALUES (?,?,?,?,?)
            """, (block["block_id"], block["job_id"], "pending",
                  block["row_start"], block["row_end"]))
            conn.commit()
            conn.close()

    async def assign_block(self, block_id: str, job_id: str,
                           worker_id: str) -> None:
        async with self._lock:
            conn = self._conn()
            conn.execute("""
                UPDATE blocks SET status='assigned', worker_id=?,
                assigned_at=? WHERE block_id=? AND job_id=?
            """, (worker_id, time.time(), block_id, job_id))
            conn.commit()
            conn.close()

    async def complete_block(self, block_id: str, job_id: str,
                             partial_result: list, metrics: dict) -> None:
        async with self._lock:
            conn = self._conn()
            conn.execute("""
                UPDATE blocks SET status='done', partial_result=?,
                completed_at=?, compute_time_ms=?, mflops=?, device_type=?
                WHERE block_id=? AND job_id=?
            """, (
                json.dumps(partial_result),
                time.time(),
                metrics.get("compute_time_ms"),
                metrics.get("mflops"),
                metrics.get("device_type"),
                block_id, job_id
            ))
            conn.commit()
            conn.close()

    async def fail_block(self, block_id: str, job_id: str) -> None:
        """Reset a block to pending so it can be reassigned."""
        async with self._lock:
            conn = self._conn()
            conn.execute("""
                UPDATE blocks SET status='pending', worker_id=NULL,
                assigned_at=NULL WHERE block_id=? AND job_id=?
            """, (block_id, job_id))
            conn.commit()
            conn.close()

    async def get_pending_blocks(self, job_id: str) -> List[dict]:
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM blocks WHERE job_id=? AND status='pending'",
            (job_id,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    async def get_all_blocks(self, job_id: str) -> List[dict]:
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM blocks WHERE job_id=?", (job_id,)
        ).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            if d["partial_result"]:
                d["partial_result"] = json.loads(d["partial_result"])
            result.append(d)
        return result

    async def get_timed_out_blocks(self, job_id: str,
                                   timeout: float) -> List[dict]:
        """Return blocks assigned but not completed within timeout seconds."""
        cutoff = time.time() - timeout
        conn = self._conn()
        rows = conn.execute("""
            SELECT * FROM blocks WHERE job_id=?
            AND status='assigned' AND assigned_at < ?
        """, (job_id, cutoff)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    async def count_done_blocks(self, job_id: str) -> int:
        conn = self._conn()
        n = conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE job_id=? AND status='done'",
            (job_id,)
        ).fetchone()[0]
        conn.close()
        return n

    # ── Result CRUD ───────────────────────────────────────────────────────────

    async def store_result(self, job_id: str,
                           result_matrix: list) -> None:
        now = time.time()
        async with self._lock:
            conn = self._conn()
            conn.execute("""
                INSERT OR REPLACE INTO results
                (job_id, result_matrix, completed_at, expires_at)
                VALUES (?,?,?,?)
            """, (job_id, json.dumps(result_matrix), now,
                  now + RESULT_TTL))
            conn.commit()
            conn.close()

    async def get_result(self, job_id: str) -> Optional[list]:
        conn = self._conn()
        row = conn.execute(
            "SELECT result_matrix, expires_at FROM results WHERE job_id=?",
            (job_id,)
        ).fetchone()
        conn.close()
        if not row:
            return None
        if time.time() > row["expires_at"]:
            return None
        return json.loads(row["result_matrix"])

    # ── Node registry ─────────────────────────────────────────────────────────

    async def upsert_node(self, node: dict) -> None:
        async with self._lock:
            conn = self._conn()
            conn.execute("""
                INSERT INTO nodes (node_id, ip, port, join_time,
                                   last_seen, device_type, status)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(node_id) DO UPDATE SET
                    last_seen=excluded.last_seen,
                    status=excluded.status,
                    ip=excluded.ip
            """, (
                node["node_id"], node["ip"], node["port"],
                node.get("join_time", time.time()),
                time.time(),
                node.get("device_type","python"),
                node.get("status","idle"),
            ))
            conn.commit()
            conn.close()

    async def remove_node(self, node_id: str) -> None:
        async with self._lock:
            conn = self._conn()
            conn.execute("DELETE FROM nodes WHERE node_id=?", (node_id,))
            conn.commit()
            conn.close()

    async def get_active_nodes(self, exclude_id: str = "") -> List[dict]:
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM nodes WHERE node_id != ? ORDER BY join_time ASC",
            (exclude_id,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ── Cleanup ───────────────────────────────────────────────────────────────

    async def cleanup_expired(self) -> None:
        now = time.time()
        async with self._lock:
            conn = self._conn()
            conn.execute("DELETE FROM results WHERE expires_at < ?", (now,))
            conn.execute("DELETE FROM jobs WHERE expires_at < ?", (now,))
            conn.commit()
            conn.close()
        log.info("Cleanup: expired jobs and results removed")

    # ── Bulk sync (received from coordinator as backup node) ──────────────────

    async def apply_sync(self, operation: str, data: dict) -> None:
        """Apply a state-sync operation received from coordinator."""
        ops = {
            "create_job":          self.create_job,
            "create_block":        self.create_block,
            "assign_block":        lambda d: self.assign_block(
                                       d["block_id"], d["job_id"], d["worker_id"]),
            "complete_block":      lambda d: self.complete_block(
                                       d["block_id"], d["job_id"],
                                       d["partial_result"], d["metrics"]),
            "fail_block":          lambda d: self.fail_block(
                                       d["block_id"], d["job_id"]),
            "update_job_status":   lambda d: self.update_job_status(
                                       d["job_id"], d["status"]),
            "update_coordinator":  lambda d: self.update_job_coordinator(
                                       d["job_id"], d["coordinator_id"]),
            "store_result":        lambda d: self.store_result(
                                       d["job_id"], d["result_matrix"]),
        }
        fn = ops.get(operation)
        if fn:
            await fn(data)
        else:
            log.warning("Unknown sync operation: %s", operation)
