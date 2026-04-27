"""
Python worker logic.
Receives a block assignment, validates it, computes the partial matrix
product using pure Python (no numpy), returns result + metrics.

Separation of concerns:
  1. validate_assignment()  — reject bad upstream data
  2. multiply_block()       — pure computation, no I/O
  3. execute_block()        — async orchestrator
"""

import asyncio
import logging
import time
from typing import List

log = logging.getLogger("worker")


# ── Input validation ─────────────────────────────────────────────────────────

class WorkerError(Exception):
    """Raised when assignment data is invalid or computation fails."""


def validate_assignment(assignment: dict) -> None:
    """
    Validate that the assignment has the required fields and that
    the matrices are well-formed and dimension-compatible.
    Fails fast with a clear error instead of crashing in the loop.
    """
    for key in ("job_id", "block_id", "A_block", "B"):
        if key not in assignment:
            raise WorkerError(f"Missing required field: '{key}'")

    A_block = assignment["A_block"]
    B       = assignment["B"]

    if not isinstance(A_block, list) or len(A_block) == 0:
        raise WorkerError("A_block must be a non-empty list of rows")
    if not isinstance(B, list) or len(B) == 0:
        raise WorkerError("B must be a non-empty list of rows")

    cols_A = len(A_block[0])
    if cols_A == 0:
        raise WorkerError("A_block rows must not be empty")

    # Check A_block is rectangular
    for i, row in enumerate(A_block):
        if not isinstance(row, (list, tuple)) or len(row) != cols_A:
            raise WorkerError(f"A_block row {i} has inconsistent length")

    # Check B is rectangular
    cols_B = len(B[0])
    if cols_B == 0:
        raise WorkerError("B rows must not be empty")
    for i, row in enumerate(B):
        if not isinstance(row, (list, tuple)) or len(row) != cols_B:
            raise WorkerError(f"B row {i} has inconsistent length")

    # Dimension compatibility: cols(A) must equal rows(B)
    if cols_A != len(B):
        raise WorkerError(
            f"Dimension mismatch: A_block has {cols_A} columns, "
            f"B has {len(B)} rows. They must be equal."
        )


# ── Core matrix multiply (pure Python) ───────────────────────────────────────

def multiply_block(A_block: List[List[float]],
                   B: List[List[float]]) -> List[List[float]]:
    """
    Multiply A_block (a subset of rows from A) by the full matrix B.

    A_block : shape (r, k)
    B       : shape (k, n)
    result  : shape (r, n)

    This is the canonical O(r·k·n) triple-loop algorithm.
    Chosen over Strassen intentionally:
      - Strassen's constant factor is large for small matrices
      - Cache behaviour is worse for Strassen on row-block partitions
      - Pure Python cannot exploit the O(n^2.807) gain effectively
    See DESIGN.md §6 for full rationale.
    """
    r = len(A_block)
    k = len(B)
    n = len(B[0])
    result = [[0.0] * n for _ in range(r)]

    for i in range(r):
        A_row = A_block[i]
        C_row = result[i]
        for p in range(k):
            a_ip  = A_row[p]
            B_row = B[p]
            for j in range(n):
                C_row[j] += a_ip * B_row[j]
    return result


# ── Metrics computation ──────────────────────────────────────────────────────

def compute_metrics(rows: int, k: int, n: int,
                    elapsed_ms: float) -> dict:
    """
    Build metrics dict.
    Operation count: 2·r·k·n (one multiply + one add per inner product step).
    MFLOPS = operations / elapsed_ms / 1000 (mega floating-point ops/sec).
    """
    operations = 2 * rows * k * n
    mflops     = (operations / elapsed_ms / 1e3) if elapsed_ms > 0 else 0.0
    return {
        "compute_time_ms":  round(elapsed_ms, 3),
        "rows_processed":   rows,
        "total_operations": operations,
        "mflops":           round(mflops, 4),
        "device_type":      "python",
    }


# ── Async worker task ─────────────────────────────────────────────────────────

async def execute_block(assignment: dict) -> dict:
    """
    Full worker pipeline: validate → compute → package result.
    Runs the CPU-bound multiply in a thread executor so the event loop
    stays responsive for heartbeats and other async tasks.

    assignment keys: job_id, block_id, row_start, row_end, A_block, B
    Returns the block_result dict ready to POST back to coordinator.
    """
    # 1. Validate
    validate_assignment(assignment)

    job_id   = assignment["job_id"]
    block_id = assignment["block_id"]
    A_block  = assignment["A_block"]
    B        = assignment["B"]
    rows     = len(A_block)
    k        = len(B)
    n        = len(B[0])

    log.info("[Worker] Computing block %s (job %s) — %d rows × %d cols",
             block_id[:8], job_id[:8], rows, n)

    # 2. Compute in executor (non-blocking)
    t_start = time.perf_counter()
    loop    = asyncio.get_running_loop()

    try:
        partial_C = await loop.run_in_executor(
            None, multiply_block, A_block, B
        )
    except asyncio.CancelledError:
        log.warning("[Worker] Block %s cancelled", block_id[:8])
        raise
    except Exception as e:
        log.error("[Worker] Block %s failed: %s", block_id[:8], e)
        raise WorkerError(f"Computation failed: {e}") from e

    elapsed_ms = (time.perf_counter() - t_start) * 1000

    # 3. Package result
    metrics = compute_metrics(rows, k, n, elapsed_ms)

    log.info("[Worker] Block %s done — %.2f ms, %.4f MFLOPS",
             block_id[:8], elapsed_ms, metrics["mflops"])

    return {
        "job_id":     job_id,
        "block_id":   block_id,
        "partial_C":  partial_C,
        "metrics":    metrics,
        "attempt_id": assignment.get("attempt_id", 0),
    }