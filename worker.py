"""
Python worker logic.
Receives a block assignment, computes the partial matrix product
using pure Python (as specified — no numpy), returns result + metrics.
"""

import asyncio
import logging
import time
from typing import List

log = logging.getLogger("worker")


# ── Core matrix multiply (pure Python) ───────────────────────────────────────

def multiply_block(A_block: List[List[float]],
                   B: List[List[float]]) -> List[List[float]]:
    """
    Multiply A_block (a subset of rows from A) by the full matrix B.

    A_block : shape (r, k)
    B       : shape (k, n)
    result  : shape (r, n)

    This is the canonical O(r·k·n) triple-loop algorithm.
    Chosen over Strassen intentionally (see design doc §6).
    """
    r = len(A_block)
    k = len(B)
    n = len(B[0])
    result = [[0.0] * n for _ in range(r)]

    for i in range(r):
        for j in range(n):
            s = 0.0
            for p in range(k):
                s += A_block[i][p] * B[p][j]
            result[i][j] = s
    return result


# ── Worker task (runs when coordinator sends a block) ─────────────────────────

async def execute_block(assignment: dict) -> dict:
    """
    assignment keys: job_id, block_id, row_start, row_end, A_block, B
    Returns the block_result dict ready to POST back to coordinator.
    """
    job_id    = assignment["job_id"]
    block_id  = assignment["block_id"]
    A_block   = assignment["A_block"]
    B         = assignment["B"]
    rows      = len(A_block)
    k         = len(B)
    n         = len(B[0]) if B else 0

    log.info("[Worker] Computing block %s (job %s) — %d rows",
             block_id[:8], job_id[:8], rows)

    t_start  = time.perf_counter()

    # Run in executor to avoid blocking the event loop on large matrices
    loop     = asyncio.get_event_loop()
    partial_C = await loop.run_in_executor(
        None, multiply_block, A_block, B
    )

    elapsed_ms  = (time.perf_counter() - t_start) * 1000
    operations  = 2 * rows * k * n          # multiply-add pairs
    mflops      = (operations / elapsed_ms / 1e3) if elapsed_ms > 0 else 0.0

    metrics = {
        "compute_time_ms":   round(elapsed_ms, 3),
        "rows_processed":    rows,
        "total_operations":  operations,
        "mflops":            round(mflops, 4),
        "device_type":       "python",
    }

    log.info("[Worker] Block %s done — %.2f ms, %.4f MFLOPS",
             block_id[:8], elapsed_ms, mflops)

    return {
        "job_id":    job_id,
        "block_id":  block_id,
        "partial_C": partial_C,
        "metrics":   metrics,
    }