"""
Shared data structures and wire-message factories.

Using plain dataclasses (stdlib only) so browser-side code never needs
Pydantic.  FastAPI endpoints use dict payloads validated manually.

Internal vs wire-safe:
  - NodeInfo, JobRecord, BlockRecord  → internal / storage
  - make_*  helpers                   → wire messages (JSON over HTTP/WS)
"""

from dataclasses import dataclass, field, asdict
from typing import List, Optional, Any
import time
import uuid

from config import JobStatus, BlockStatus, NodeStatus


# ── Validation helpers ────────────────────────────────────────────────────────

def _require(condition: bool, msg: str) -> None:
    """Raise ValueError if condition is False."""
    if not condition:
        raise ValueError(msg)


def validate_matrix(m: Any, name: str = "matrix") -> None:
    """Ensure m is a non-empty rectangular list-of-lists of numbers."""
    _require(isinstance(m, list) and len(m) > 0,
             f"{name} must be a non-empty list of rows")
    cols = len(m[0])
    _require(cols > 0, f"{name} rows must not be empty")
    for i, row in enumerate(m):
        _require(isinstance(row, (list, tuple)) and len(row) == cols,
                 f"{name} row {i} has {len(row)} cols, expected {cols}")


# ── Node identity ─────────────────────────────────────────────────────────────

@dataclass
class NodeInfo:
    node_id:     str
    ip:          str
    port:        int
    join_time:   float = field(default_factory=time.time)
    last_seen:   float = field(default_factory=time.time)
    device_type: str   = "python"          # 'python' | 'browser'
    status:      str   = NodeStatus.IDLE

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "NodeInfo":
        """Construct from dict, ignoring unknown keys."""
        known = {"node_id", "ip", "port", "join_time", "last_seen",
                 "device_type", "status"}
        filtered = {k: v for k, v in d.items() if k in known}
        if "port" not in filtered:
            filtered["port"] = 0
        return NodeInfo(**filtered)


# ── Job lifecycle ─────────────────────────────────────────────────────────────

@dataclass
class JobRecord:
    job_id:         str
    submitter_id:   str
    status:         str             # JobStatus.*
    matrix_A:       List[List[float]]
    matrix_B:       List[List[float]]
    rows_A:         int
    cols_A:         int
    cols_B:         int
    total_blocks:   int
    coordinator_id: str
    backup_nodes:   List[str]       # list of node_ids
    created_at:     float
    expires_at:     float

    def __post_init__(self):
        _require(self.status in JobStatus.ALL,
                 f"Invalid job status: {self.status}")

    def to_dict(self) -> dict:
        return asdict(self)


# ── Block lifecycle ───────────────────────────────────────────────────────────

@dataclass
class BlockRecord:
    block_id:       str
    job_id:         str
    status:         str             # BlockStatus.*
    row_start:      int
    row_end:        int
    attempt_id:     int             = 0      # incremented on each reassignment
    worker_id:      Optional[str]  = None
    partial_result: Optional[Any]  = None
    assigned_at:    Optional[float]= None
    completed_at:   Optional[float]= None
    compute_time_ms:Optional[float]= None
    mflops:         Optional[float]= None
    device_type:    Optional[str]  = None

    def __post_init__(self):
        _require(self.status in BlockStatus.ALL,
                 f"Invalid block status: {self.status}")
        _require(self.row_start >= 0 and self.row_end > self.row_start,
                 f"Invalid row range: {self.row_start}–{self.row_end}")

    def to_dict(self) -> dict:
        return asdict(self)


# ── Wire messages (JSON bodies sent between nodes / browser) ──────────────────
# These are the system's vocabulary — names here must match what server.py,
# coordinator.py, app.js, and worker.js expect.

def make_assign_block(job_id: str, block_id: str, row_start: int,
                      row_end: int, A_block: List[List[float]],
                      B: List[List[float]], attempt_id: int = 0) -> dict:
    """Build an assign_block message for a worker (HTTP or WebSocket)."""
    return {
        "type":       "assign_block",
        "job_id":     job_id,
        "block_id":   block_id,
        "row_start":  row_start,
        "row_end":    row_end,
        "A_block":    A_block,
        "B":          B,
        "attempt_id": attempt_id,
        "timestamp":  time.time(),
    }


def make_block_result(job_id: str, block_id: str, worker_id: str,
                      partial_C: List[List[float]], metrics: dict,
                      attempt_id: int = 0) -> dict:
    return {
        "type":       "block_result",
        "job_id":     job_id,
        "block_id":   block_id,
        "worker_id":  worker_id,
        "partial_C":  partial_C,
        "metrics":    metrics,
        "attempt_id": attempt_id,
        "timestamp":  time.time(),
    }


def make_heartbeat(node_id: str, status: str) -> dict:
    return {
        "type":      "heartbeat",
        "node_id":   node_id,
        "status":    status,
        "timestamp": time.time(),
    }


def make_election(node_id: str, job_id: str) -> dict:
    return {
        "type":      "election",
        "node_id":   node_id,
        "job_id":    job_id,
        "timestamp": time.time(),
    }


def make_election_ok(from_id: str, to_id: str, job_id: str) -> dict:
    return {
        "type":      "election_ok",
        "from_node": from_id,
        "to_node":   to_id,
        "job_id":    job_id,
        "timestamp": time.time(),
    }


def make_coordinator_announce(node_id: str, job_id: str) -> dict:
    return {
        "type":      "coordinator_announce",
        "node_id":   node_id,
        "job_id":    job_id,
        "timestamp": time.time(),
    }


def make_state_sync(operation: str, data: dict) -> dict:
    return {
        "type":      "state_sync",
        "operation": operation,
        "data":      data,
        "timestamp": time.time(),
    }


def make_job_complete(job_id: str, result_matrix: List[List[float]],
                      duration_ms: float, worker_ids: List[str]) -> dict:
    return {
        "type":          "job_complete",
        "job_id":        job_id,
        "result_matrix": result_matrix,
        "duration_ms":   duration_ms,
        "workers_used":  worker_ids,
        "timestamp":     time.time(),
    }