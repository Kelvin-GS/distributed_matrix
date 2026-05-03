"""
SQLite-backed persistent store.
- WAL mode     →  concurrent readers + single writer
- asyncio.Lock →  serialises writes safely in async context
- try/finally  →  connections always closed, even on exception
- State machine enforcement for job and block status transitions
- Attempt-ID protection against stale block results
- Every write is replicated to backup nodes via the caller (node.py)
"""

import sqlite3
import asyncio
import json
import time
import logging
from typing import Optional, List

from config import (DB_PATH, RESULT_TTL, HEARTBEAT_TIMEOUT,
                    JobStatus, BlockStatus)

log = logging.getLogger("storage")


class Storage:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._lock   = asyncio.Lock()
        self._init_db()

    # ── Init ─────────────────────────────────────────────────────────────────

    def _init_db(self):
        conn = self._conn()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
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
                    attempt_id      INTEGER NOT NULL DEFAULT 0,
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
        finally:
            conn.close()
        log.info("SQLite initialised at %s (WAL mode)", self.db_path)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    # ── Job CRUD ──────────────────────────────────────────────────────────────

    async def create_job(self, job: dict) -> None:
        """Insert a new job. Raises if job_id already exists (no silent replace)."""
        async with self._lock:
            conn = self._conn()
            try:
                conn.execute("""
                    INSERT INTO jobs
                    (job_id, submitter_id, status, matrix_A, matrix_B,
                     rows_A, cols_A, cols_B, total_blocks, coordinator_id,
                     backup_nodes, created_at, expires_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    job["job_id"], job["submitter_id"],
                    job.get("status", JobStatus.PENDING),
                    json.dumps(job["matrix_A"]), json.dumps(job["matrix_B"]),
                    job["rows_A"], job["cols_A"], job["cols_B"],
                    job["total_blocks"], job["coordinator_id"],
                    json.dumps(job.get("backup_nodes", [])),
                    job["created_at"], job["expires_at"],
                ))
                conn.commit()
            except sqlite3.IntegrityError:
                log.warning("Job %s already exists — skipping create", job["job_id"][:8])
            finally:
                conn.close()

    async def get_job(self, job_id: str) -> Optional[dict]:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if not row:
                return None
            d = dict(row)
            d["matrix_A"]     = json.loads(d["matrix_A"])
            d["matrix_B"]     = json.loads(d["matrix_B"])
            d["backup_nodes"] = json.loads(d["backup_nodes"])
            return d
        finally:
            conn.close()

    async def update_job_status(self, job_id: str, new_status: str) -> bool:
        """Transition job status. Enforces state machine. Returns True if updated."""
        if new_status not in JobStatus.ALL:
            log.error("Invalid job status: %s", new_status)
            return False
        async with self._lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    "SELECT status FROM jobs WHERE job_id=?", (job_id,)
                ).fetchone()
                if not row:
                    return False
                current = row["status"]
                allowed = JobStatus.TRANSITIONS.get(current, set())
                if new_status not in allowed:
                    log.warning("Invalid job transition %s → %s for %s",
                                current, new_status, job_id[:8])
                    return False
                conn.execute(
                    "UPDATE jobs SET status=? WHERE job_id=?",
                    (new_status, job_id)
                )
                conn.commit()
                return True
            finally:
                conn.close()

    async def update_job_coordinator(self, job_id: str,
                                     coordinator_id: str) -> None:
        async with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    "UPDATE jobs SET coordinator_id=? WHERE job_id=?",
                    (coordinator_id, job_id)
                )
                conn.commit()
            finally:
                conn.close()

    async def get_running_jobs(self) -> List[dict]:
        """Return all jobs with status='running'."""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT job_id, coordinator_id, backup_nodes FROM jobs WHERE status=?",
                (JobStatus.RUNNING,)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["backup_nodes"] = json.loads(d["backup_nodes"])
                result.append(d)
            return result
        finally:
            conn.close()

    # ── Block CRUD ────────────────────────────────────────────────────────────

    async def create_block(self, block: dict) -> None:
        """Insert a new block. Idempotent — ignores duplicates."""
        async with self._lock:
            conn = self._conn()
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO blocks
                    (block_id, job_id, status, row_start, row_end, attempt_id)
                    VALUES (?,?,?,?,?,?)
                """, (block["block_id"], block["job_id"], BlockStatus.PENDING,
                      block["row_start"], block["row_end"],
                      block.get("attempt_id", 0)))
                conn.commit()
            finally:
                conn.close()

    async def assign_block(self, block_id: str, job_id: str,
                           worker_id: str) -> Optional[int]:
        """
        Assign block to a worker. Increments attempt_id.
        Returns the new attempt_id (used as a lease token).
        """
        async with self._lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    "SELECT attempt_id, status FROM blocks WHERE block_id=? AND job_id=?",
                    (block_id, job_id)
                ).fetchone()
                if not row:
                    return None
                new_attempt = row["attempt_id"] + 1
                conn.execute("""
                    UPDATE blocks SET status=?, worker_id=?,
                    assigned_at=?, attempt_id=?
                    WHERE block_id=? AND job_id=?
                """, (BlockStatus.ASSIGNED, worker_id, time.time(),
                      new_attempt, block_id, job_id))
                conn.commit()
                return new_attempt
            finally:
                conn.close()

    async def complete_block(self, block_id: str, job_id: str,
                             partial_result: list, metrics: dict,
                             attempt_id: int = -1) -> bool:
        """
        Mark a block as done. If attempt_id is provided, only accepts
        results from the current attempt (stale-result protection).
        Returns True if the block was actually updated.
        """
        async with self._lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    "SELECT attempt_id, status FROM blocks WHERE block_id=? AND job_id=?",
                    (block_id, job_id)
                ).fetchone()
                if not row:
                    return False
                # Reject stale results
                if attempt_id >= 0 and row["attempt_id"] != attempt_id:
                    log.warning("Stale result for block %s: attempt %d != current %d",
                                block_id[:8], attempt_id, row["attempt_id"])
                    return False
                # Already done — idempotent
                if row["status"] == BlockStatus.DONE:
                    return True
                conn.execute("""
                    UPDATE blocks SET status=?, partial_result=?,
                    completed_at=?, compute_time_ms=?, mflops=?, device_type=?
                    WHERE block_id=? AND job_id=?
                """, (
                    BlockStatus.DONE,
                    json.dumps(partial_result),
                    time.time(),
                    metrics.get("compute_time_ms"),
                    metrics.get("mflops"),
                    metrics.get("device_type"),
                    block_id, job_id
                ))
                conn.commit()
                return True
            finally:
                conn.close()

    async def fail_block(self, block_id: str, job_id: str) -> None:
        """Reset a block to pending so it can be reassigned."""
        async with self._lock:
            conn = self._conn()
            try:
                conn.execute("""
                    UPDATE blocks SET status=?, worker_id=NULL,
                    assigned_at=NULL WHERE block_id=? AND job_id=?
                    AND status IN (?, ?)
                """, (BlockStatus.PENDING, block_id, job_id,
                      BlockStatus.ASSIGNED, BlockStatus.FAILED))
                conn.commit()
            finally:
                conn.close()

    async def get_pending_blocks(self, job_id: str) -> List[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM blocks WHERE job_id=? AND status=?",
                (job_id, BlockStatus.PENDING)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    async def get_all_blocks(self, job_id: str) -> List[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM blocks WHERE job_id=?", (job_id,)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                if d["partial_result"]:
                    d["partial_result"] = json.loads(d["partial_result"])
                result.append(d)
            return result
        finally:
            conn.close()

    async def get_timed_out_blocks(self, job_id: str,
                                   timeout: float) -> List[dict]:
        """Return blocks assigned but not completed within timeout seconds."""
        cutoff = time.time() - timeout
        conn = self._conn()
        try:
            rows = conn.execute("""
                SELECT * FROM blocks WHERE job_id=?
                AND status=? AND assigned_at < ?
            """, (job_id, BlockStatus.ASSIGNED, cutoff)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    async def count_done_blocks(self, job_id: str) -> int:
        conn = self._conn()
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM blocks WHERE job_id=? AND status=?",
                (job_id, BlockStatus.DONE)
            ).fetchone()[0]
            return n
        finally:
            conn.close()

    async def all_blocks_done(self, job_id: str) -> bool:
        """Check that every block for a job is in DONE state."""
        conn = self._conn()
        try:
            total = conn.execute(
                "SELECT COUNT(*) FROM blocks WHERE job_id=?", (job_id,)
            ).fetchone()[0]
            done = conn.execute(
                "SELECT COUNT(*) FROM blocks WHERE job_id=? AND status=?",
                (job_id, BlockStatus.DONE)
            ).fetchone()[0]
            return total > 0 and done == total
        finally:
            conn.close()

    # ── Result CRUD ───────────────────────────────────────────────────────────

    async def store_result(self, job_id: str,
                           result_matrix: list) -> None:
        now = time.time()
        async with self._lock:
            conn = self._conn()
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO results
                    (job_id, result_matrix, completed_at, expires_at)
                    VALUES (?,?,?,?)
                """, (job_id, json.dumps(result_matrix), now,
                      now + RESULT_TTL))
                conn.commit()
            finally:
                conn.close()

    async def get_result(self, job_id: str) -> Optional[list]:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT result_matrix, expires_at FROM results WHERE job_id=?",
                (job_id,)
            ).fetchone()
            if not row:
                return None
            if time.time() > row["expires_at"]:
                return None
            return json.loads(row["result_matrix"])
        finally:
            conn.close()

    # ── Node registry ─────────────────────────────────────────────────────────

    async def upsert_node(self, node: dict) -> None:
        async with self._lock:
            conn = self._conn()
            try:
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
                    node.get("device_type", "python"),
                    node.get("status", "idle"),
                ))
                conn.commit()
            finally:
                conn.close()

    async def remove_node(self, node_id: str) -> None:
        async with self._lock:
            conn = self._conn()
            try:
                conn.execute("DELETE FROM nodes WHERE node_id=?", (node_id,))
                conn.commit()
            finally:
                conn.close()

    async def get_active_nodes(self, exclude_id: str = "",
                               max_age: float = None) -> List[dict]:
        """Return nodes, optionally filtered by heartbeat recency."""
        conn = self._conn()
        try:
            if max_age is not None:
                cutoff = time.time() - max_age
                rows = conn.execute(
                    "SELECT * FROM nodes WHERE node_id != ? AND last_seen > ? "
                    "ORDER BY join_time ASC",
                    (exclude_id, cutoff)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM nodes WHERE node_id != ? ORDER BY join_time ASC",
                    (exclude_id,)
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ── Cleanup ───────────────────────────────────────────────────────────────

    async def cleanup_expired(self) -> None:
        now = time.time()
        expired_ids = []
        async with self._lock:
            conn = self._conn()
            try:
                # Get expired job IDs first
                expired_jobs = conn.execute(
                    "SELECT job_id FROM jobs WHERE expires_at < ?", (now,)
                ).fetchall()
                expired_ids = [r["job_id"] for r in expired_jobs]

                # Clean up related blocks for expired jobs
                for jid in expired_ids:
                    conn.execute("DELETE FROM blocks WHERE job_id=?", (jid,))

                conn.execute("DELETE FROM results WHERE expires_at < ?", (now,))
                conn.execute("DELETE FROM jobs WHERE expires_at < ?", (now,))
                conn.commit()
            finally:
                conn.close()
        if expired_ids:
            log.info("Cleanup: removed %d expired jobs and their blocks",
                     len(expired_ids))

    # ── Bulk sync (received from coordinator as backup node) ──────────────────

    async def apply_sync(self, operation: str, data: dict) -> None:
        """
        Apply a state-sync operation received from coordinator.
        All operations are idempotent — safe to replay on duplicate messages.
        """
        ops = {
            "create_job":         self.create_job,
            "create_block":       self.create_block,
            "assign_block":       lambda d: self.assign_block(
                                      d["block_id"], d["job_id"], d["worker_id"]),
            "complete_block":     lambda d: self.complete_block(
                                      d["block_id"], d["job_id"],
                                      d["partial_result"], d["metrics"],
                                      d.get("attempt_id", -1)),
            "fail_block":         lambda d: self.fail_block(
                                      d["block_id"], d["job_id"]),
            "update_job_status":  lambda d: self._sync_job_status(
                                      d["job_id"], d["status"]),
            "update_coordinator": lambda d: self.update_job_coordinator(
                                      d["job_id"], d["coordinator_id"]),
            "store_result":       lambda d: self.store_result(
                                      d["job_id"], d["result_matrix"]),
        }
        fn = ops.get(operation)
        if fn:
            try:
                await fn(data)
            except Exception as e:
                log.error("Sync operation '%s' failed: %s", operation, e)
        else:
            log.warning("Unknown sync operation: %s", operation)

    async def _sync_job_status(self, job_id: str, status: str) -> None:
        """Force-set job status during sync (bypasses state machine for replicas)."""
        async with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    "UPDATE jobs SET status=? WHERE job_id=?",
                    (status, job_id)
                )
                conn.commit()
            finally:
                conn.close()
