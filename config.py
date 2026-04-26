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

# ── Election (Bully) ──────────────────────────────────────────────────────────
ELECTION_TIMEOUT          = 5.0   # wait for OK before declaring self winner
COORDINATOR_ANNOUNCE_WAIT = 5.0   # wait for winner announcement after OK sent

# ── Persistence ───────────────────────────────────────────────────────────────
DB_PATH            = os.path.join(os.path.dirname(__file__), "matmul.db")
RESULT_TTL         = 7200   # 2 hours in seconds
CLEANUP_INTERVAL   = 1800   # cleanup expired records every 30 minutes

# ── Replication ───────────────────────────────────────────────────────────────
NUM_BACKUP_NODES   = 3      # SQLite state replicated to this many backup nodes

# ── Matrix ────────────────────────────────────────────────────────────────────
MAX_DIM            = 500    # maximum matrix dimension supported
MIN_DIM            = 2      # minimum matrix dimension supported

# ── Web ───────────────────────────────────────────────────────────────────────
WEB_DIR            = os.path.join(os.path.dirname(__file__), "web")