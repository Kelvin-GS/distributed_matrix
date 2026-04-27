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

Memory management: election state is cleaned up after resolution.
Task safety: duplicate elections for the same job are prevented.
"""

import asyncio
import logging
import time
from typing import Dict, Set, Callable, Awaitable, Optional

from config import ELECTION_TIMEOUT, COORDINATOR_ANNOUNCE_WAIT
from models import make_election, make_election_ok, make_coordinator_announce

log = logging.getLogger("election")


class ElectionManager:
    def __init__(self, node_id: str,
                 post_to_node: Callable[[str, int, str, dict], Awaitable[bool]],
                 get_active_nodes: Callable[[], list],
                 on_became_coordinator: Callable[[str], Awaitable[None]]):
        self.node_id          = node_id
        self._post            = post_to_node
        self._get_nodes       = get_active_nodes
        self._on_became_coord = on_became_coordinator

        # Per-job election state
        self._ok_received:    Dict[str, Set[str]]            = {}
        self._election_done:  Dict[str, bool]                = {}
        self._active_tasks:   Dict[str, Optional[asyncio.Task]] = {}

    # ── Called when we detect coordinator failure ─────────────────────────────

    async def start_election(self, job_id: str) -> None:
        """Initiate a Bully election for the given job."""
        # Prevent duplicate concurrent elections for the same job
        if self._election_done.get(job_id):
            return
        if job_id in self._active_tasks and self._active_tasks[job_id] is not None:
            task = self._active_tasks[job_id]
            if not task.done():
                log.debug("[Election][%s] Election already in progress", job_id[:8])
                return

        log.info("[Election][%s] Starting — I am %s",
                 job_id[:8], self.node_id[:8])

        self._ok_received[job_id]   = set()
        self._election_done[job_id] = False

        nodes  = self._get_nodes()
        higher = [n for n in nodes if n["node_id"] > self.node_id]
        msg    = make_election(self.node_id, job_id)

        # Send ELECTION to all higher-ID nodes
        if higher:
            results = await asyncio.gather(
                *[self._post(n["ip"], n["port"], "/election/start", msg)
                  for n in higher],
                return_exceptions=True
            )
            for n, result in zip(higher, results):
                if isinstance(result, Exception):
                    log.debug("[Election] Failed to reach %s: %s",
                              n["node_id"][:8], result)

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
        nodes  = self._get_nodes()
        sender = next((n for n in nodes if n["node_id"] == from_node_id), None)
        if sender:
            ok_msg = make_election_ok(self.node_id, from_node_id, job_id)
            await self._post(sender["ip"], sender["port"],
                             "/election/ok", ok_msg)

        # Start our own election if we haven't yet
        if not self._election_done.get(job_id):
            task = asyncio.create_task(self.start_election(job_id))
            self._active_tasks[job_id] = task

    # ── Called when we receive OK from a higher node ─────────────────────────

    async def on_ok_received(self, from_node_id: str, job_id: str) -> None:
        log.info("[Election][%s] OK from %s", job_id[:8], from_node_id[:8])
        if job_id in self._ok_received:
            self._ok_received[job_id].add(from_node_id)

        # Wait for them to announce themselves as coordinator
        await asyncio.sleep(COORDINATOR_ANNOUNCE_WAIT)

        # If we still haven't heard a coordinator announcement, retry
        if not self._election_done.get(job_id):
            log.warning("[Election][%s] No announcement received; resetting and retrying",
                        job_id[:8])
            # Reset state before retry
            self._ok_received.pop(job_id, None)
            self._election_done.pop(job_id, None)
            self._active_tasks.pop(job_id, None)
            task = asyncio.create_task(self.start_election(job_id))
            self._active_tasks[job_id] = task

    # ── Called when another node announces itself as coordinator ──────────────

    def on_coordinator_announced(self, coordinator_id: str,
                                  job_id: str) -> None:
        log.info("[Election][%s] %s is new coordinator",
                 job_id[:8], coordinator_id[:8])
        self._election_done[job_id] = True
        # Clean up election state for this job
        self._cleanup_job(job_id)

    # ── Internal: we won ──────────────────────────────────────────────────────

    async def _declare_victory(self, job_id: str) -> None:
        self._election_done[job_id] = True
        log.info("[Election][%s] I WON. Announcing.", job_id[:8])

        nodes = self._get_nodes()
        msg   = make_coordinator_announce(self.node_id, job_id)

        # Broadcast to all nodes EXCEPT self
        targets = [n for n in nodes if n["node_id"] != self.node_id]
        if targets:
            results = await asyncio.gather(
                *[self._post(n["ip"], n["port"],
                             "/coordinator/announce", msg)
                  for n in targets],
                return_exceptions=True
            )
            for n, result in zip(targets, results):
                if isinstance(result, Exception):
                    log.warning("[Election] Failed to announce to %s: %s",
                                n["node_id"][:8], result)

        # Clean up election state
        self._cleanup_job(job_id)

        # Take over as coordinator for this job
        await self._on_became_coord(job_id)

    # ── Memory cleanup ────────────────────────────────────────────────────────

    def _cleanup_job(self, job_id: str) -> None:
        """Remove per-job election state to prevent memory leaks."""
        self._ok_received.pop(job_id, None)
        # Keep _election_done[job_id] = True briefly to prevent re-entry
        self._active_tasks.pop(job_id, None)

        # Schedule removal of _election_done after a cooldown
        async def _clear_done():
            await asyncio.sleep(ELECTION_TIMEOUT * 2)
            self._election_done.pop(job_id, None)

        asyncio.create_task(_clear_done())