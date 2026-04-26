"""
All shared data-structures used across coordinator, worker, server, and storage.
Using plain dataclasses (stdlib only) so phones-as-thin-proxies never need Pydantic.
FastAPI endpoints use dict payloads validated manually — keeps the runtime lean.
"""

from dataclasses import dataclass, field, asdict
from typing import List, Optional, Any, Dict
import time, uuid


# ── Node identity ─────────────────────────────────────────────────────────────

@dataclass
class NodeInfo:
    node_id:     str
    ip:          str
    port:        int
    join_time:   float = field(default_factory=time.time)
    last_seen:   float = field(default_factory=time.time)
    device_type: str   = "python"          # 'python' | 'browser'
    status:      str   = "idle"            # 'idle' | 'working' | 'coordinating'

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "NodeInfo":
        return NodeInfo(**d)


# ── Job lifecycle ─────────────────────────────────────────────────────────────

@dataclass
class JobRequest:
    """Payload the client POSTs to /submit"""
    matrix_A:     List[List[float]]
    matrix_B:     List[List[float]]
    submitter_id: str = ""          # filled by server from node_id


@dataclass
class JobRecord:
    job_id:         str
    submitter_id:   str
    status:         str             # pending | running | complete | failed
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

    def to_dict(self) -> dict:
        return asdict(self)


# ── Block lifecycle ───────────────────────────────────────────────────────────

@dataclass
class BlockRecord:
    block_id:       str
    job_id:         str
    status:         str             # pending | assigned | computing | done | failed
    row_start:      int
    row_end:        int
    worker_id:      Optional[str]  = None
    partial_result: Optional[Any]  = None  # List[List[float]] when done
    assigned_at:    Optional[float]= None
    completed_at:   Optional[float]= None
    compute_time_ms:Optional[float]= None
    mflops:         Optional[float]= None
    device_type:    Optional[str]  = None

    def to_dict(self) -> dict:
        return asdict(self)


# ── Wire messages (JSON bodies sent between nodes / browser) ──────────────────

def make_assign_block(job_id: str, block: BlockRecord,
                      A_block: List[List[float]],
                      B: List[List[float]]) -> dict:
    return {
        "type":      "assign_block",
        "job_id":    job_id,
        "block_id":  block.block_id,
        "row_start": block.row_start,
        "row_end":   block.row_end,
        "A_block":   A_block,
        "B":         B,
        "timestamp": time.time(),
    }


def make_block_result(job_id: str, block_id: str, worker_id: str,
                      partial_C: List[List[float]], metrics: dict) -> dict:
    return {
        "type":       "block_result",
        "job_id":     job_id,
        "block_id":   block_id,
        "worker_id":  worker_id,
        "partial_C":  partial_C,
        "metrics":    metrics,
        "timestamp":  time.time(),
    }


def make_heartbeat(node_id: str, status: str) -> dict:
    return {"type": "heartbeat", "node_id": node_id,
            "status": status, "timestamp": time.time()}


def make_election(node_id: str, job_id: str) -> dict:
    return {"type": "election", "node_id": node_id,
            "job_id": job_id, "timestamp": time.time()}


def make_election_ok(from_id: str, to_id: str, job_id: str) -> dict:
    return {"type": "election_ok", "from_node": from_id,
            "to_node": to_id, "job_id": job_id, "timestamp": time.time()}


def make_coordinator_announce(node_id: str, job_id: str) -> dict:
    return {"type": "new_coordinator", "node_id": node_id,
            "job_id": job_id, "timestamp": time.time()}


def make_state_sync(operation: str, data: dict) -> dict:
    return {"type": "state_sync", "operation": operation,
            "data": data, "timestamp": time.time()}


def make_job_complete(job_id: str, result_matrix: List[List[float]],
                      duration_ms: float, worker_ids: List[str]) -> dict:
    return {
        "type":           "job_complete",
        "job_id":         job_id,
        "result_matrix":  result_matrix,
        "duration_ms":    duration_ms,
        "workers_used":   worker_ids,
        "timestamp":      time.time(),
    }