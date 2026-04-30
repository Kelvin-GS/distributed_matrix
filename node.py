"""
Node — the top-level orchestrator.
Owns: storage, discovery, election manager, coordinator, heartbeat loop.
Everything else (server.py, worker.py) is wired through here.

Lifecycle:
  start()    → registers self, starts discovery, background loops, HTTP server
  shutdown() → closes session, stops discovery, cancels background tasks
"""

import asyncio
import logging
import os
import time
import uuid
from typing import Dict, List, Optional

import aiohttp
import uvicorn

from config import (NODE_PORT, NODE_ID_FILE, HEARTBEAT_INTERVAL,
                    HEARTBEAT_TIMEOUT, CLEANUP_INTERVAL,
                    COORD_HEALTH_INTERVAL, NodeStatus)
from discovery   import NodeDiscovery, get_local_ip
from election    import ElectionManager
from coordinator import Coordinator
from storage     import Storage
from models      import NodeInfo, make_heartbeat
from server      import create_app

log = logging.getLogger("node")


class Node:
    def __init__(self, port: int = NODE_PORT):
        self.port        = port
        self.node_id     = self._load_or_create_id(port)
        self.local_ip    = get_local_ip()
        self.storage     = Storage()
        self._status     = NodeStatus.IDLE

        # Registry of known peer nodes: node_id → NodeInfo
        self._registry:         Dict[str, NodeInfo] = {}
        # Registry of browser WebSocket connections: node_id → WebSocket
        self._browser_sockets:  Dict[str, any]      = {}
        # HTTP session (shared, created at startup)
        self._session:          Optional[aiohttp.ClientSession] = None
        # Background tasks (tracked for clean shutdown)
        self._tasks:            List[asyncio.Task] = []
        # Placeholder — wired by server.py after app creation
        self._broadcast_to_browsers = None

        # Sub-systems
        self.coordinator = Coordinator(
            node_id              = self.node_id,
            storage              = self.storage,
            post_to_node         = self._post,
            broadcast_to_browsers= self._broadcast_wrapper,
            get_active_nodes     = self.get_active_nodes,
            send_to_browser      = self._send_to_browser,
        )
        self.election = ElectionManager(
            node_id              = self.node_id,
            post_to_node         = self._post,
            get_active_nodes     = self.get_active_nodes,
            on_became_coordinator= self.coordinator.resume_job,
        )
        self.discovery = NodeDiscovery(
            node_id       = self.node_id,
            port          = self.port,           # actual port, not hardcoded
            on_node_found = self._on_node_found,
            on_node_lost  = self._on_node_lost,
        )

    # ── Node ID persistence ───────────────────────────────────────────────────

    def _load_or_create_id(self, port: int) -> str:
        """Each port gets its own identity file so multiple local instances
        never collide.  Falls back to the legacy .node_id if a port-specific
        file doesn't exist yet (first run after upgrade)."""
        id_file = f"{NODE_ID_FILE}.{port}"

        # Try port-specific file first
        if os.path.exists(id_file):
            with open(id_file) as f:
                nid = f.read().strip()
                if nid:
                    log.info("Loaded persistent node_id: %s", nid[:8])
                    return nid

        # Migrate from legacy .node_id if this is the default port
        # and the legacy file exists (smooth upgrade path)
        if port == NODE_PORT and os.path.exists(NODE_ID_FILE):
            with open(NODE_ID_FILE) as f:
                nid = f.read().strip()
                if nid:
                    # Copy to port-specific file
                    with open(id_file, "w") as pf:
                        pf.write(nid)
                    log.info("Migrated legacy node_id to %s: %s",
                             id_file, nid[:8])
                    return nid

        # Generate fresh ID
        nid = str(uuid.uuid4())
        with open(id_file, "w") as f:
            f.write(nid)
        log.info("Created new node_id: %s (file: %s)", nid[:8], id_file)
        return nid

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        log.info("Node %s starting on %s:%d", self.node_id[:8],
                 self.local_ip, self.port)
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        )

        try:
            # Register self in DB
            await self.storage.upsert_node({
                "node_id":    self.node_id,
                "ip":         self.local_ip,
                "port":       self.port,
                "join_time":  time.time(),
                "last_seen":  time.time(),
                "device_type":"python",
                "status":     NodeStatus.IDLE,
            })

            # Start background tasks (tracked for clean shutdown)
            await self.discovery.start()
        except Exception:
            # Ensure session is closed on early startup failure
            await self.shutdown()
            raise

        self._tasks.append(asyncio.create_task(
            self._guarded_loop("heartbeat", self._heartbeat_tick, HEARTBEAT_INTERVAL)))
        self._tasks.append(asyncio.create_task(
            self._guarded_loop("cleanup", self._cleanup_tick, CLEANUP_INTERVAL)))
        self._tasks.append(asyncio.create_task(
            self._guarded_loop("coord_health", self._coord_health_tick, COORD_HEALTH_INTERVAL)))
        asyncio.create_task(self._announce_to_peers())

        # Build and run FastAPI
        app = create_app(self)
        config = uvicorn.Config(app, host="0.0.0.0", port=self.port,
                                log_level="warning")
        server = uvicorn.Server(config)
        log.info("Web UI at http://%s:%d", self.local_ip, self.port)

        try:
            await server.serve()
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """Graceful shutdown: cancel tasks, close connections, stop discovery."""
        log.info("Node %s shutting down...", self.node_id[:8])
        # Cancel background tasks
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # Stop discovery
        await self.discovery.stop()

        # Close HTTP session
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

        log.info("Node %s shutdown complete", self.node_id[:8])

    # ── Guarded background loop ───────────────────────────────────────────────

    async def _guarded_loop(self, name: str, tick_fn, interval: float) -> None:
        """Run tick_fn every interval seconds. Log errors but keep running."""
        while True:
            try:
                await asyncio.sleep(interval)
                await tick_fn()
            except asyncio.CancelledError:
                log.debug("Background loop '%s' cancelled", name)
                return
            except Exception as e:
                log.error("Background loop '%s' error: %s", name, e)
                await asyncio.sleep(interval)  # backoff on error

    # ── Peer management ───────────────────────────────────────────────────────

    def _on_node_found(self, node_info: NodeInfo) -> None:
        self._registry[node_info.node_id] = node_info
        asyncio.create_task(self._greet_node(node_info))
        log.info("Node joined: %s @ %s:%d", node_info.node_id[:8],
                 node_info.ip, node_info.port)

    def _on_node_lost(self, node_id: str) -> None:
        self._registry.pop(node_id, None)
        log.info("Node left: %s", node_id[:8])

    def get_active_nodes(self) -> list:
        """
        Return all known nodes (Python peers + browser workers).
        Python peers come from mDNS discovery (_registry).
        Browser workers come from WebSocket connections (_browser_sockets).
        Both are merged into a single list for the coordinator.
        """
        now     = time.time()
        cutoff  = HEARTBEAT_TIMEOUT * 3
        result  = []
        seen_ids = set()

        # Python peers from mDNS registry
        for ni in self._registry.values():
            if now - ni.last_seen < cutoff:
                result.append({
                    "node_id":     ni.node_id,
                    "ip":          ni.ip,
                    "port":        ni.port,
                    "device_type": ni.device_type,
                    "last_seen":   ni.last_seen,
                })
                seen_ids.add(ni.node_id)

        # Browser/phone workers from WebSocket registry
        # These connect via WebSocket, not HTTP — port=0 signals this.
        # The coordinator MUST route work to these nodes via WebSocket,
        # NOT via HTTP POST (which would silently fail on port=0).
        for browser_id, ws in self._browser_sockets.items():
            if browser_id not in seen_ids:
                result.append({
                    "node_id":     browser_id,
                    "ip":          "ws",
                    "port":        0,
                    "device_type": "browser",
                    "last_seen":   now,
                })
                seen_ids.add(browser_id)

        return result

    async def _announce_to_peers(self) -> None:
        """Push our node info to all discovered peers via HTTP."""
        await asyncio.sleep(2)  # wait for mDNS to settle
        my_info = self._my_info()
        for ni in list(self._registry.values()):
            await self._post(ni.ip, ni.port, "/nodes/register", my_info)

    async def _greet_node(self, ni: NodeInfo) -> None:
        """Register with a newly discovered peer."""
        await self._post(ni.ip, ni.port, "/nodes/register", self._my_info())

    def _my_info(self) -> dict:
        return {
            "node_id":    self.node_id,
            "ip":         self.local_ip,
            "port":       self.port,
            "join_time":  time.time(),
            "last_seen":  time.time(),
            "device_type":"python",
            "status":     self._status,
        }

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeat_tick(self) -> None:
        """Single heartbeat tick — sends status to all peers."""
        # Determine actual status
        if self.coordinator._active_jobs:
            self._status = NodeStatus.COORDINATING
        else:
            self._status = NodeStatus.IDLE

        msg = make_heartbeat(self.node_id, self._status)
        for ni in list(self._registry.values()):
            asyncio.create_task(
                self._post(ni.ip, ni.port, "/heartbeat", msg)
            )

    async def on_heartbeat(self, data: dict) -> None:
        nid = data.get("node_id")
        if not nid:
            return
        if nid in self._registry:
            self._registry[nid].last_seen = time.time()
        else:
            # Accept heartbeats from nodes not yet in registry
            # (they may have registered via HTTP before mDNS discovered them)
            log.debug("Heartbeat from unknown node %s — ignoring", nid[:8])

    # ── Coordinator health monitor ────────────────────────────────────────────

    async def _coord_health_tick(self) -> None:
        """
        Check if coordinators of running jobs are still alive.
        If a coordinator is unreachable, trigger a Bully election.
        """
        running_jobs = await self.storage.get_running_jobs()
        for job in running_jobs:
            coord_id = job["coordinator_id"]
            if coord_id == self.node_id:
                continue  # we're the coordinator

            # Is the coordinator still in our active registry?
            coord_node = self._registry.get(coord_id)
            if coord_node:
                age = time.time() - coord_node.last_seen
                if age < HEARTBEAT_TIMEOUT * 5:
                    continue  # coordinator is alive

            # Coordinator is dead or unreachable — start election
            log.warning("[Health] Coordinator %s for job %s appears dead — "
                        "starting election", coord_id[:8], job["job_id"][:8])
            asyncio.create_task(
                self.election.start_election(job["job_id"])
            )

    # ── Cleanup ───────────────────────────────────────────────────────────────

    async def _cleanup_tick(self) -> None:
        """Single cleanup tick."""
        await self.storage.cleanup_expired()
        # Remove stale nodes from registry
        now   = time.time()
        stale = [nid for nid, ni in self._registry.items()
                 if now - ni.last_seen > HEARTBEAT_TIMEOUT * 5]
        for nid in stale:
            self._registry.pop(nid, None)
            log.info("Pruned stale node: %s", nid[:8])

    # ── HTTP helper ───────────────────────────────────────────────────────────

    async def _post(self, ip: str, port: int,
                    path: str, body: dict) -> bool:
        """POST JSON to a peer. Returns True on success, False on any failure."""
        if not port:     # browser nodes have port=0
            return False
        if not self._session or self._session.closed:
            return False
        url = f"http://{ip}:{port}{path}"
        try:
            async with self._session.post(url, json=body) as resp:
                return resp.status < 400
        except Exception as e:
            log.debug("POST %s failed: %s", url, e)
            return False

    # ── WebSocket send helper (for browser workers) ───────────────────────────

    async def _send_to_browser(self, node_id: str, msg: dict) -> bool:
        """Send a message to a browser worker via its WebSocket."""
        import json
        ws = self._browser_sockets.get(node_id)
        if not ws:
            return False
        try:
            await ws.send_text(json.dumps(msg))
            return True
        except Exception as e:
            log.debug("WS send to %s failed: %s", node_id[:8], e)
            self._browser_sockets.pop(node_id, None)
            return False

    # ── Broadcast wrapper for coordinator ─────────────────────────────────────

    async def _broadcast_wrapper(self, job_id: str, msg: dict) -> None:
        if self._broadcast_to_browsers:
            await self._broadcast_to_browsers(job_id, msg)