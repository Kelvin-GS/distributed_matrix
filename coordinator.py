"""
Stateless coordinator.
ALL state lives in SQLite — the coordinator only reads and writes the DB.
If this coordinator dies, a new one picks up from exactly where this left off.
"""

import asyncio
import logging
import math
import time
import uuid
from typing import List, Optional

from config import (NUM_BACKUP_NODES, RESULT_TTL, BLOCK_TIMEOUT,
                    MIN_DIM, MAX_DIM)
from models  import make_assign_block, make_state_sync, make_job_complete
from storage import Storage
from worker  import execute_block

log = logging.getLogger("coordinator")


class Coordinator:
    def __init__(self, node_id: str, storage: Storage,
                 post_to_node,         # async (ip, port, path, body) → bool
                 broadcast_to_browsers,# async (msg: dict) → None
                 get_active_nodes):    # () → List[dict]
        self.node_id           = node_id
        self._db               = storage
        self._post             = post_to_node
        self._broadcast_browser= broadcast_to_browsers
        self._get_nodes        = get_active_nodes
        self._active_jobs:  set= set()   # job_ids this node is coordinating

    # ── Job intake ────────────────────────────────────────────────────────────

    async def submit_job(self, matrix_A: list, matrix_B: list,
                         submitter_id: str) -> str:
        # ── Validate dimensions ────────────────────────────────────────────
        rows_A, cols_A = len(matrix_A), len(matrix_A[0])
        rows_B, cols_B = len(matrix_B), len(matrix_B[0])

        if cols_A != rows_B:
            raise ValueError(
                f"Incompatible: A is {rows_A}×{cols_A}, "
                f"B is {rows_B}×{cols_B}. "
                f"Columns of A must equal rows of B."
            )
        if not (MIN_DIM <= rows_A <= MAX_DIM and MIN_DIM <= cols_B <= MAX_DIM):
            raise ValueError(f"Matrix dimensions must be between "
                             f"{MIN_DIM} and {MAX_DIM}.")

        job_id = str(uuid.uuid4())
        now    = time.time()

        # ── Choose backup nodes ────────────────────────────────────────────
        # Pick the NUM_BACKUP_NODES nodes with the oldest join_time (most stable)
        nodes   = self._get_nodes()
        backups = [n["node_id"] for n in nodes[:NUM_BACKUP_NODES]
                   if n["node_id"] != self.node_id]

        # ── Partition into row-blocks ──────────────────────────────────────
        workers       = [n for n in nodes if n["node_id"] != self.node_id]
        # Add self as a worker too
        all_workers   = workers  # coordinator also acts as worker below

        # Dynamic block size: at least 1 row per block
        num_workers   = max(1, len(all_workers) + 1)  # +1 = self
        block_size    = max(1, math.ceil(rows_A / num_workers))
        blocks        = []
        row = 0
        block_idx = 0
        while row < rows_A:
            end = min(row + block_size, rows_A)
            blocks.append({
                "block_id":  f"{job_id}-blk-{block_idx}",
                "job_id":    job_id,
                "row_start": row,
                "row_end":   end,
            })
            row += block_size
            block_idx += 1

        job_record = {
            "job_id":         job_id,
            "submitter_id":   submitter_id,
            "status":         "running",
            "matrix_A":       matrix_A,
            "matrix_B":       matrix_B,
            "rows_A":         rows_A,
            "cols_A":         cols_A,
            "cols_B":         cols_B,
            "total_blocks":   len(blocks),
            "coordinator_id": self.node_id,
            "backup_nodes":   backups,
            "created_at":     now,
            "expires_at":     now + RESULT_TTL,
        }

        # ── Persist everything BEFORE any computation starts ───────────────
        await self._db.create_job(job_record)
        for b in blocks:
            await self._db.create_block(b)

        await self._sync_to_backups("create_job", job_record, backups)
        for b in blocks:
            await self._sync_to_backups("create_block", b, backups)

        log.info("[Coord] Job %s — %d blocks across %d workers",
                 job_id[:8], len(blocks), num_workers)

        self._active_jobs.add(job_id)
        asyncio.create_task(self._run_job(job_id, matrix_A, matrix_B,
                                          blocks, all_workers, backups))
        return job_id

    # ── Job execution ─────────────────────────────────────────────────────────

    async def _run_job(self, job_id: str, A: list, B: list,
                       blocks: list, workers: list, backups: list) -> None:
        try:
            await self._assign_all_blocks(job_id, A, B, blocks, workers, backups)
            await self._monitor_until_done(job_id, A, B, workers, backups)
            await self._assemble_and_store(job_id, backups)
        except Exception as e:
            log.exception("[Coord] Job %s failed: %s", job_id[:8], e)
            await self._db.update_job_status(job_id, "failed")
        finally:
            self._active_jobs.discard(job_id)

    async def _assign_all_blocks(self, job_id, A, B,
                                  blocks, workers, backups):
        """Assign blocks to workers; self handles any leftover blocks."""
        worker_iter = iter(workers)
        self_blocks = []

        for i, block in enumerate(blocks):
            worker = next(worker_iter, None)
            if worker is None:
                # Cycle workers if more blocks than workers
                worker_iter = iter(workers)
                worker = next(worker_iter, None)

            if worker is None:
                # Only self is available
                self_blocks.append(block)
                continue

            A_block = A[block["row_start"]:block["row_end"]]
            assigned = await self._assign_block_to_node(
                job_id, block, A_block, B, worker, backups
            )
            if not assigned:
                self_blocks.append(block)

        # Coordinator handles its own blocks locally
        for block in self_blocks:
            await self._db.assign_block(block["block_id"], job_id,
                                        self.node_id)
            A_block   = A[block["row_start"]:block["row_end"]]
            assignment = {
                "job_id":    job_id,
                "block_id":  block["block_id"],
                "row_start": block["row_start"],
                "row_end":   block["row_end"],
                "A_block":   A_block,
                "B":         B,
            }
            result = await execute_block(assignment)
            await self.receive_result(result, backups)

    async def _assign_block_to_node(self, job_id, block, A_block, B,
                                     worker, backups) -> bool:
        msg = make_assign_block(job_id, type("B", (), block)(), A_block, B)
        msg["block_id"]  = block["block_id"]
        msg["row_start"] = block["row_start"]
        msg["row_end"]   = block["row_end"]

        ok = await self._post(worker["ip"], worker["port"], "/work", msg)
        if ok:
            await self._db.assign_block(block["block_id"], job_id,
                                        worker["node_id"])
            await self._sync_to_backups("assign_block", {
                "block_id":  block["block_id"],
                "job_id":    job_id,
                "worker_id": worker["node_id"],
            }, backups)
        return ok

    async def _monitor_until_done(self, job_id, A, B,
                                   workers, backups) -> None:
        """Poll for timed-out blocks and reassign them."""
        job = await self._db.get_job(job_id)
        total = job["total_blocks"]

        while True:
            done = await self._db.count_done_blocks(job_id)
            if done >= total:
                break

            # Re-assign timed-out blocks
            timed_out = await self._db.get_timed_out_blocks(
                job_id, BLOCK_TIMEOUT
            )
            for block in timed_out:
                log.warning("[Coord] Block %s timed out, reassigning",
                            block["block_id"][:8])
                await self._db.fail_block(block["block_id"], job_id)
                await self._sync_to_backups("fail_block", {
                    "block_id": block["block_id"],
                    "job_id":   job_id,
                }, backups)
                A_block = A[block["row_start"]:block["row_end"]]
                # Reassign to self or any available worker
                assignment = {
                    "job_id":    job_id,
                    "block_id":  block["block_id"],
                    "row_start": block["row_start"],
                    "row_end":   block["row_end"],
                    "A_block":   A_block,
                    "B":         B,
                }
                await self._db.assign_block(block["block_id"], job_id,
                                            self.node_id)
                result = await execute_block(assignment)
                await self.receive_result(result, backups)

            await asyncio.sleep(2)

    async def _assemble_and_store(self, job_id: str,
                                   backups: list) -> None:
        """Combine all partial results into the final matrix C."""
        blocks = await self._db.get_all_blocks(job_id)
        blocks.sort(key=lambda b: b["row_start"])

        C = []
        for b in blocks:
            if b["partial_result"]:
                C.extend(b["partial_result"])

        await self._db.store_result(job_id, C)
        await self._db.update_job_status(job_id, "complete")
        await self._sync_to_backups("store_result",
                                    {"job_id": job_id, "result_matrix": C},
                                    backups)
        await self._sync_to_backups("update_job_status",
                                    {"job_id": job_id, "status": "complete"},
                                    backups)

        job    = await self._db.get_job(job_id)
        dur    = (time.time() - job["created_at"]) * 1000
        all_bl = await self._db.get_all_blocks(job_id)
        wids   = list({b["worker_id"] for b in all_bl if b["worker_id"]})

        msg = make_job_complete(job_id, C, dur, wids)
        await self._broadcast_browser(job_id, msg)

        log.info("[Coord] Job %s COMPLETE in %.1f ms", job_id[:8], dur)

    # ── Receive result from worker ────────────────────────────────────────────

    async def receive_result(self, data: dict,
                              backups: list = None) -> None:
        job_id   = data["job_id"]
        block_id = data["block_id"]
        partial  = data["partial_C"]
        metrics  = data["metrics"]
        worker   = data.get("worker_id", self.node_id)

        await self._db.complete_block(block_id, job_id, partial, metrics)

        if backups:
            await self._sync_to_backups("complete_block", {
                "block_id":      block_id,
                "job_id":        job_id,
                "partial_result":partial,
                "metrics":       metrics,
            }, backups)

        log.info("[Coord] Block %s received from %s — %.2f ms, %.4f MFLOPS",
                 block_id[:8], worker[:8],
                 metrics.get("compute_time_ms", 0),
                 metrics.get("mflops", 0))

    # ── Takeover: resume a job after election win ─────────────────────────────

    async def resume_job(self, job_id: str) -> None:
        """Called after winning election for job_id."""
        log.info("[Coord] Resuming job %s after election win", job_id[:8])
        job = await self._db.get_job(job_id)
        if not job or job["status"] == "complete":
            return

        await self._db.update_job_coordinator(job_id, self.node_id)
        A       = job["matrix_A"]
        B       = job["matrix_B"]
        backups = job["backup_nodes"]
        pending = await self._db.get_pending_blocks(job_id)
        workers = self._get_nodes()

        self._active_jobs.add(job_id)
        log.info("[Coord] %d pending blocks to reassign for job %s",
                 len(pending), job_id[:8])

        asyncio.create_task(
            self._assign_all_blocks(job_id, A, B, pending, workers, backups)
        )
        asyncio.create_task(
            self._monitor_until_done(job_id, A, B, workers, backups)
        )
        asyncio.create_task(
            self._assemble_and_store(job_id, backups)
        )

    # ── State replication ─────────────────────────────────────────────────────

    async def _sync_to_backups(self, operation: str,
                                data: dict, backups: list) -> None:
        msg   = make_state_sync(operation, data)
        nodes = self._get_nodes()
        tasks = []
        for node_id in backups:
            target = next((n for n in nodes if n["node_id"] == node_id), None)
            if target:
                tasks.append(
                    self._post(target["ip"], target["port"],
                               "/sync/state", msg)
                )
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)