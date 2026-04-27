"""
Stateless coordinator.
ALL state lives in SQLite — the coordinator only reads and writes the DB.
If this coordinator dies, a new one picks up from exactly where it left off.

Key guarantees:
  - resume_job() follows a strict sequential pipeline (assign → monitor → assemble)
  - Attempt-ID lease tokens prevent stale block results from overwriting newer ones
  - Final assembly verifies that every block is DONE before producing the result
  - Backup sync failures are logged and the unreachable backup is removed for that job
  - Browser workers receive assignments via WebSocket (not HTTP)
"""

import asyncio
import json
import logging
import math
import time
import uuid
from typing import List, Optional

from config import (NUM_BACKUP_NODES, RESULT_TTL, BLOCK_TIMEOUT,
                    MIN_DIM, MAX_DIM, SYNC_RETRY_COUNT,
                    JobStatus, BlockStatus)
from models  import make_assign_block, make_state_sync, make_job_complete
from storage import Storage
from worker  import execute_block, WorkerError

log = logging.getLogger("coordinator")


class Coordinator:
    def __init__(self, node_id: str, storage: Storage,
                 post_to_node,          # async (ip, port, path, body) → bool
                 broadcast_to_browsers, # async (job_id, msg) → None
                 get_active_nodes,      # () → List[dict]
                 send_to_browser=None): # async (node_id, msg) → bool
        self.node_id            = node_id
        self._db                = storage
        self._post              = post_to_node
        self._broadcast_browser = broadcast_to_browsers
        self._get_nodes         = get_active_nodes
        self._send_browser      = send_to_browser
        self._active_jobs: set  = set()
        # Track blocks currently being computed locally (prevent double-assign)
        self._local_computing: set = set()

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
        nodes   = self._get_nodes()
        backups = [n["node_id"] for n in nodes[:NUM_BACKUP_NODES]
                   if n["node_id"] != self.node_id]

        # ── Partition into row-blocks ──────────────────────────────────────
        workers       = [n for n in nodes if n["node_id"] != self.node_id]
        num_workers   = max(1, len(workers) + 1)  # +1 = self
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
            "status":         JobStatus.RUNNING,
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

        # ── Persist everything BEFORE any computation ──────────────────────
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
                                          blocks, workers, backups))
        return job_id

    # ── Job execution (strict sequential pipeline) ────────────────────────────

    async def _run_job(self, job_id: str, A: list, B: list,
                       blocks: list, workers: list, backups: list) -> None:
        """Sequential pipeline: assign → monitor → assemble."""
        try:
            await self._assign_all_blocks(job_id, A, B, blocks, workers, backups)
            await self._monitor_until_done(job_id, A, B, workers, backups)
            await self._assemble_and_store(job_id, backups)
        except Exception as e:
            log.exception("[Coord] Job %s failed: %s", job_id[:8], e)
            await self._db.update_job_status(job_id, JobStatus.FAILED)
        finally:
            self._active_jobs.discard(job_id)

    async def _assign_all_blocks(self, job_id, A, B,
                                  blocks, workers, backups):
        """Assign blocks to workers; self handles any leftover blocks."""
        worker_iter = iter(workers)
        self_blocks = []

        for block in blocks:
            # Skip already-done blocks (important for resume)
            if block.get("status") == BlockStatus.DONE:
                continue

            worker = next(worker_iter, None)
            if worker is None:
                worker_iter = iter(workers)
                worker = next(worker_iter, None)

            if worker is None:
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
            block_id = block["block_id"]
            if block_id in self._local_computing:
                continue  # already computing this block locally

            attempt_id = await self._db.assign_block(block_id, job_id,
                                                      self.node_id)
            self._local_computing.add(block_id)
            try:
                A_block    = A[block["row_start"]:block["row_end"]]
                assignment = {
                    "job_id":     job_id,
                    "block_id":   block_id,
                    "row_start":  block["row_start"],
                    "row_end":    block["row_end"],
                    "A_block":    A_block,
                    "B":          B,
                    "attempt_id": attempt_id,
                }
                result = await execute_block(assignment)
                await self.receive_result(result, backups)
            except (WorkerError, Exception) as e:
                log.error("[Coord] Local block %s failed: %s", block_id[:8], e)
                await self._db.fail_block(block_id, job_id)
            finally:
                self._local_computing.discard(block_id)

    async def _assign_block_to_node(self, job_id, block, A_block, B,
                                     worker, backups) -> bool:
        """
        Assign a block to a remote worker.
        - Python workers: HTTP POST to /work
        - Browser workers (port=0): WebSocket message
        DB is updated BEFORE the assignment is sent.
        """
        block_id = block["block_id"]

        # Update DB first (atomic: DB knows who owns this block)
        attempt_id = await self._db.assign_block(block_id, job_id,
                                                  worker["node_id"])
        await self._sync_to_backups("assign_block", {
            "block_id":  block_id,
            "job_id":    job_id,
            "worker_id": worker["node_id"],
        }, backups)

        # Build assignment message (no more make_assign_block with fake object)
        msg = make_assign_block(
            job_id=job_id, block_id=block_id,
            row_start=block["row_start"], row_end=block["row_end"],
            A_block=A_block, B=B, attempt_id=attempt_id,
        )

        # Route to browser workers via WebSocket
        if worker.get("port", 0) == 0 or worker.get("device_type") == "browser":
            if self._send_browser:
                ok = await self._send_browser(worker["node_id"], msg)
                if ok:
                    log.info("[Coord] Block %s → browser %s via WS",
                             block_id[:8], worker["node_id"][:8])
                    return True
            # Browser unreachable — fail the assignment so it can be retried
            await self._db.fail_block(block_id, job_id)
            return False

        # Route to Python workers via HTTP
        ok = await self._post(worker["ip"], worker["port"], "/work", msg)
        if ok:
            return True

        # HTTP failed — reset the block so it can be reassigned
        await self._db.fail_block(block_id, job_id)
        return False

    async def _monitor_until_done(self, job_id, A, B,
                                   workers, backups) -> None:
        """Poll for timed-out blocks and reassign them."""
        job   = await self._db.get_job(job_id)
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
                block_id = block["block_id"]
                # Don't reassign blocks we're currently computing locally
                if block_id in self._local_computing:
                    continue
                log.warning("[Coord] Block %s timed out (attempt %d), reassigning",
                            block_id[:8], block.get("attempt_id", 0))
                await self._db.fail_block(block_id, job_id)
                await self._sync_to_backups("fail_block", {
                    "block_id": block_id,
                    "job_id":   job_id,
                }, backups)

                # Reassign to self
                A_block    = A[block["row_start"]:block["row_end"]]
                attempt_id = await self._db.assign_block(block_id, job_id,
                                                          self.node_id)
                self._local_computing.add(block_id)
                try:
                    assignment = {
                        "job_id":     job_id,
                        "block_id":   block_id,
                        "row_start":  block["row_start"],
                        "row_end":    block["row_end"],
                        "A_block":    A_block,
                        "B":          B,
                        "attempt_id": attempt_id,
                    }
                    result = await execute_block(assignment)
                    await self.receive_result(result, backups)
                except (WorkerError, Exception) as e:
                    log.error("[Coord] Reassigned block %s failed: %s",
                              block_id[:8], e)
                    await self._db.fail_block(block_id, job_id)
                finally:
                    self._local_computing.discard(block_id)

            await asyncio.sleep(2)

    async def _assemble_and_store(self, job_id: str,
                                   backups: list) -> None:
        """
        Combine all partial results into the final matrix C.
        ONLY proceeds if every block is verified DONE.
        """
        # Verify completeness before assembly
        if not await self._db.all_blocks_done(job_id):
            log.error("[Coord] Job %s: not all blocks done — cannot assemble",
                      job_id[:8])
            await self._db.update_job_status(job_id, JobStatus.FAILED)
            return

        blocks = await self._db.get_all_blocks(job_id)
        blocks.sort(key=lambda b: b["row_start"])

        C = []
        for b in blocks:
            if b["partial_result"]:
                C.extend(b["partial_result"])
            else:
                log.error("[Coord] Block %s has no partial_result", b["block_id"][:8])
                await self._db.update_job_status(job_id, JobStatus.FAILED)
                return

        await self._db.store_result(job_id, C)
        await self._db.update_job_status(job_id, JobStatus.COMPLETE)

        # Sync completion flag to backups (NOT the full matrix — saves bandwidth)
        await self._sync_to_backups("update_job_status",
                                    {"job_id": job_id, "status": JobStatus.COMPLETE},
                                    backups)

        job    = await self._db.get_job(job_id)
        dur    = (time.time() - job["created_at"]) * 1000
        all_bl = await self._db.get_all_blocks(job_id)
        wids   = list({b["worker_id"] for b in all_bl if b["worker_id"]})

        msg = make_job_complete(job_id, C, dur, wids)
        await self._broadcast_browser(job_id, msg)

        log.info("[Coord] Job %s COMPLETE in %.1f ms — %d workers",
                 job_id[:8], dur, len(wids))

    # ── Receive result from worker ────────────────────────────────────────────

    async def receive_result(self, data: dict,
                              backups: list = None) -> None:
        job_id     = data["job_id"]
        block_id   = data["block_id"]
        partial    = data["partial_C"]
        metrics    = data["metrics"]
        attempt_id = data.get("attempt_id", -1)
        worker     = data.get("worker_id", self.node_id)

        accepted = await self._db.complete_block(
            block_id, job_id, partial, metrics, attempt_id
        )
        if not accepted:
            log.warning("[Coord] Result for block %s rejected (stale/duplicate)",
                        block_id[:8])
            return

        if backups:
            await self._sync_to_backups("complete_block", {
                "block_id":       block_id,
                "job_id":         job_id,
                "partial_result": partial,
                "metrics":        metrics,
                "attempt_id":     attempt_id,
            }, backups)

        log.info("[Coord] Block %s from %s — %.2f ms, %.4f MFLOPS",
                 block_id[:8], worker[:8],
                 metrics.get("compute_time_ms", 0),
                 metrics.get("mflops", 0))

    # ── Takeover: resume a job after election win ─────────────────────────────

    async def resume_job(self, job_id: str) -> None:
        """
        Called after winning election for job_id.
        Resumes the job using the strict sequential pipeline (_run_job),
        NOT concurrent tasks. Fully reconciles block state from DB.
        """
        log.info("[Coord] Resuming job %s after election win", job_id[:8])
        job = await self._db.get_job(job_id)
        if not job or job["status"] == JobStatus.COMPLETE:
            return

        await self._db.update_job_coordinator(job_id, self.node_id)
        A       = job["matrix_A"]
        B       = job["matrix_B"]
        backups = job["backup_nodes"]

        # Get blocks that still need work (pending or timed-out assigned)
        pending = await self._db.get_pending_blocks(job_id)
        timed_out = await self._db.get_timed_out_blocks(job_id, BLOCK_TIMEOUT)

        # Reset timed-out blocks to pending
        for block in timed_out:
            await self._db.fail_block(block["block_id"], job_id)
            pending.append(block)

        workers = self._get_nodes()

        self._active_jobs.add(job_id)
        log.info("[Coord] %d blocks to process for job %s",
                 len(pending), job_id[:8])

        # Use the SAME sequential pipeline as submit_job
        asyncio.create_task(
            self._run_job(job_id, A, B, pending, workers, backups)
        )

    # ── State replication ─────────────────────────────────────────────────────

    async def _sync_to_backups(self, operation: str,
                                data: dict, backups: list) -> None:
        """
        Replicate state change to backup nodes.
        Retries on failure. Removes unreachable backups for this job.
        """
        msg   = make_state_sync(operation, data)
        nodes = self._get_nodes()
        dead_backups = []

        for node_id in backups:
            target = next((n for n in nodes if n["node_id"] == node_id), None)
            if not target:
                continue

            ok = False
            for attempt in range(1 + SYNC_RETRY_COUNT):
                ok = await self._post(target["ip"], target["port"],
                                      "/sync/state", msg)
                if ok:
                    break
                if attempt < SYNC_RETRY_COUNT:
                    await asyncio.sleep(0.5)

            if not ok:
                log.warning("[Coord] Backup sync to %s failed after %d attempts",
                            node_id[:8], SYNC_RETRY_COUNT + 1)
                dead_backups.append(node_id)

        # Remove unreachable backups from this job's backup list
        for nid in dead_backups:
            if nid in backups:
                backups.remove(nid)