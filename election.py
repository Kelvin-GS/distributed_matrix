"""
Bully Election Algorithm — per-job coordinator election.

When the coordinator for a job is detected as dead:
  1. Any node that notices fires an election for that job_id.
  2. It sends ELECTION to all known nodes with a higher node_id.
  3. If it receives no OK within ELECTION_TIMEOUT seconds → it wins.
  4. Winner broadcasts COORDINATOR_ANNOUNCE.
  5. New coordinator reads SQLite and resumes the job.

Because elections are per-job, multiple elections can run concurrently
(one per failed coordinator) without interfering.
"""

import asyncio
import logging
import time
from typing import Dict, Set, Callable, Awaitable

from config import ELECTION_TIMEOUT, COORDINATOR_ANNOUNCE_WAIT
from models import make_election, make_election_ok, make_coordinator_announce

log = logging.getLogger("election")


class ElectionManager:
    def __init__(self, node_id: str,
                 post_to_node: Callable[[str, int, str, dict], Awaitable[bool]],
                 get_active_nodes: Callable[[], list],
                 on_became_coordinator: Callable[[str], Awaitable[None]]):
        self.node_id              = node_id
        self._post                = post_to_node        # HTTP POST helper
        self._get_nodes           = get_active_nodes
        self._on_became_coord     = on_became_coordinator

        # job_id → set of node_ids that sent us OK
        self._ok_received:  Dict[str, Set[str]] = {}
        # job_id → True if we've already won / deferred
        self._election_done: Dict[str, bool]    = {}

    # ── Called when we detect coordinator failure ─────────────────────────────

    async def start_election(self, job_id: str) -> None:
        if self._election_done.get(job_id):
            return
        log.info("[Election][%s] Starting — I am %s", job_id[:8], self.node_id[:8])
        self._ok_received[job_id]  = set()
        self._election_done[job_id]= False

        nodes      = self._get_nodes()
        higher     = [n for n in nodes if n["node_id"] > self.node_id]
        msg        = make_election(self.node_id, job_id)

        # Broadcast ELECTION to all higher-id nodes
        tasks = [self._post(n["ip"], n["port"], "/election/start", msg)
                 for n in higher]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if not higher:
            # No one higher → win immediately
            await self._declare_victory(job_id)
            return

        # Wait for OK messages
        await asyncio.sleep(ELECTION_TIMEOUT)
        if not self._ok_received.get(job_id):
            await self._declare_victory(job_id)
        # else: someone higher responded → wait for their announcement

    # ── Called when we receive ELECTION from a lower node ────────────────────

    async def on_election_received(self, from_node_id: str,
                                   job_id: str) -> None:
        log.info("[Election][%s] ELECTION from %s",
                 job_id[:8], from_node_id[:8])
        # Send OK back to the lower node
        nodes = self._get_nodes()
        sender = next((n for n in nodes if n["node_id"] == from_node_id), None)
        if sender:
            ok_msg = make_election_ok(self.node_id, from_node_id, job_id)
            await self._post(sender["ip"], sender["port"],
                             "/election/ok", ok_msg)
        # Start our own election if we haven't yet
        if not self._election_done.get(job_id):
            asyncio.create_task(self.start_election(job_id))

    # ── Called when we receive OK from a higher node ─────────────────────────

    async def on_ok_received(self, from_node_id: str, job_id: str) -> None:
        log.info("[Election][%s] OK from %s", job_id[:8], from_node_id[:8])
        if job_id in self._ok_received:
            self._ok_received[job_id].add(from_node_id)
        # Wait for them to announce themselves as coordinator
        await asyncio.sleep(COORDINATOR_ANNOUNCE_WAIT)
        # If we still haven't heard a coordinator announcement, retry
        if not self._election_done.get(job_id):
            log.warning("[Election][%s] No announcement received; retrying",
                        job_id[:8])
            asyncio.create_task(self.start_election(job_id))

    # ── Called when another node announces itself as coordinator ──────────────

    def on_coordinator_announced(self, coordinator_id: str,
                                  job_id: str) -> None:
        log.info("[Election][%s] %s is new coordinator",
                 job_id[:8], coordinator_id[:8])
        self._election_done[job_id] = True

    # ── Internal: we won ──────────────────────────────────────────────────────

    async def _declare_victory(self, job_id: str) -> None:
        self._election_done[job_id] = True
        log.info("[Election][%s] I WON. Announcing.", job_id[:8])

        nodes = self._get_nodes()
        msg   = make_coordinator_announce(self.node_id, job_id)
        tasks = [self._post(n["ip"], n["port"],
                            "/coordinator/announce", msg)
                 for n in nodes]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Take over as coordinator for this job
        await self._on_became_coord(job_id)