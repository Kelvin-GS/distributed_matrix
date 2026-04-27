"""
Centralised configuration for every tuneable constant in the system.
Import from here — never hardcode values in other modules.
"""

import os

# ── Node ──────────────────────────────────────────────────────────────────────
NODE_PORT          = 8080
NODE_ID_FILE       = ".node_id"          # persists node_id across restarts

# ── mDNS / Discovery ─────────────────────────────────────────────────────────
SERVICE_TYPE       = "_matmul._tcp.local."

# ── Heartbeat ─────────────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL        = 1.0   # seconds between heartbeat broadcasts
HEARTBEAT_TIMEOUT         = 3.0   # missed seconds before node declared dead

# ── Block Assignment ──────────────────────────────────────────────────────────
BLOCK_TIMEOUT             = 30.0  # seconds before a block is reassigned
MAX_CONCURRENT_BLOCKS     = 8     # max blocks a single node processes at once

# ── Election (Bully) ──────────────────────────────────────────────────────────
ELECTION_TIMEOUT          = 5.0   # wait for OK before declaring self winner
COORDINATOR_ANNOUNCE_WAIT = 5.0   # wait for winner announcement after OK sent
COORD_HEALTH_INTERVAL     = 5.0   # how often to check coordinator liveness

# ── Persistence ───────────────────────────────────────────────────────────────
DB_PATH            = os.path.join(os.path.dirname(__file__), "matmul.db")
RESULT_TTL         = 7200   # 2 hours in seconds
CLEANUP_INTERVAL   = 1800   # cleanup expired records every 30 minutes

# ── Replication ───────────────────────────────────────────────────────────────
NUM_BACKUP_NODES   = 3      # SQLite state replicated to this many backup nodes
SYNC_RETRY_COUNT   = 2      # retries for backup replication

# ── Matrix ────────────────────────────────────────────────────────────────────
MAX_DIM            = 500    # maximum matrix dimension supported
MIN_DIM            = 2      # minimum matrix dimension supported

# ── Web ───────────────────────────────────────────────────────────────────────
WEB_DIR            = os.path.join(os.path.dirname(__file__), "web")


# ── Status Constants ──────────────────────────────────────────────────────────
# Using simple namespace classes instead of enum to keep JSON serialisation
# trivial (no .value needed). These are the ONLY valid status values.

class JobStatus:
    PENDING  = "pending"
    RUNNING  = "running"
    COMPLETE = "complete"
    FAILED   = "failed"

    ALL = {PENDING, RUNNING, COMPLETE, FAILED}

    # Allowed transitions: from_status → {to_statuses}
    TRANSITIONS = {
        PENDING:  {RUNNING, FAILED},
        RUNNING:  {COMPLETE, FAILED},
        COMPLETE: set(),            # terminal
        FAILED:   {RUNNING},        # allow retry
    }


class BlockStatus:
    PENDING  = "pending"
    ASSIGNED = "assigned"
    DONE     = "done"
    FAILED   = "failed"

    ALL = {PENDING, ASSIGNED, DONE, FAILED}

    TRANSITIONS = {
        PENDING:  {ASSIGNED},
        ASSIGNED: {DONE, FAILED},
        DONE:     set(),            # terminal
        FAILED:   {PENDING},        # reset for reassignment
    }


class NodeStatus:
    IDLE         = "idle"
    WORKING      = "working"
    COORDINATING = "coordinating"

    ALL = {IDLE, WORKING, COORDINATING}