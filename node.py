"""
Node — the top-level orchestrator.
Owns: storage, discovery, election manager, coordinator, heartbeat loop.
Everything else (server.py, worker.py) is wired through here.
"""

import asyncio
import logging
import os
import time
import uuid
from typing import Dict, Optional

import aiohttp
import uvicorn

from config import (NODE_PORT, NODE_ID_FILE, HEARTBEAT_INTERVAL,
                    HEARTBEAT_TIMEOUT, CLEANUP_INTERVAL)
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
        self.node_id     = self._load_or_create_id()
        self.local_ip    = get_local_ip()
        self.storage     = Storage()

        # Registry of known peer nodes: node_id → NodeInfo
        self._registry:         Dict[str, NodeInfo] = {}
        # Registry of browser WebSocket connections: node_id → WebSocket
        self._browser_sockets:  Dict[str, any]      = {}
        # HTTP session (shared, created at startup)
        self._session:          Optional[aiohttp.ClientSession] = None
        # Placeholder — wired by server.py after app creation
        self._broadcast_to_browsers = None

        # Sub-systems
        self.coordinator = Coordinator(
            node_id             = self.node_id,
            storage             = self.storage,
            post_to_node        = self._post,
            broadcast_to_browsers=self._broadcast_wrapper,
            get_active_nodes    = self.get_active_nodes,
        )
        self.election = ElectionManager(
            node_id              = self.node_id,
            post_to_node        = self._post,
            get_active_nodes    = self.get_active_nodes,
            on_became_coordinator= self.coordinator.resume_job,
        )
        self.discovery = NodeDiscovery(
            node_id        = self.node_id,
            on_node_found  = self._on_node_found,
            on_node_lost   = self._on_node_lost,
        )

    # ── Node ID persistence ───────────────────────────────────────────────────

    def _load_or_create_id(self) -> str:
        if os.path.exists(NODE_ID_FILE):
            with open(NODE_ID_FILE) as f:
                nid = f.read().strip()
                if nid:
                    log.info("Loaded persistent node_id: %s", nid[:8])
                    return nid
        nid = str(uuid.uuid4())
        with open(NODE_ID_FILE, "w") as f:
            f.write(nid)
        log.info("Created new node_id: %s", nid[:8])
        return nid

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        log.info("Node %s starting on %s:%d", self.node_id[:8],
                 self.local_ip, self.port)
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        )

        # Register self in DB
        await self.storage.upsert_node({
            "node_id":    self.node_id,
            "ip":         self.local_ip,
            "port":       self.port,
            "join_time":  time.time(),
            "last_seen":  time.time(),
            "device_type":"python",
            "status":     "idle",
        })

        # Start background tasks
        await self.discovery.start()
        asyncio.create_task(self._heartbeat_loop())
        asyncio.create_task(self._cleanup_loop())
        asyncio.create_task(self._announce_to_peers())

        # Build and run FastAPI
        app = create_app(self)
        config = uvicorn.Config(app, host="0.0.0.0", port=self.port,
                                log_level="warning")
        server = uvicorn.Server(config)
        log.info("Web UI at http://%s:%d", self.local_ip, self.port)
        await server.serve()

    # ── Peer management ───────────────────────────────────────────────────────

    def _on_node_found(self, node_info: NodeInfo) -> None:
        self._registry[node_info.node_id] = node_info
        asyncio.create_task(self._greet_node(node_info))
        log.info("Node joined: %s @ %s", node_info.node_id[:8],
                 node_info.ip)

    def _on_node_lost(self, node_id: str) -> None:
        self._registry.pop(node_id, None)
        log.info("Node left: %s", node_id[:8])

    def get_active_nodes(self) -> list:
        """Return all known nodes (peers + browser workers from DB)."""
        now    = time.time()
        return [
            {
                "node_id":    ni.node_id,
                "ip":         ni.ip,
                "port":       ni.port,
                "device_type":ni.device_type,
                "last_seen":  ni.last_seen,
            }
            for ni in self._registry.values()
            if now - ni.last_seen < HEARTBEAT_TIMEOUT * 3
        ]

    async def _announce_to_peers(self) -> None:
        """Push our node info to all discovered peers via HTTP."""
        await asyncio.sleep(2)  # wait for mDNS to settle
        my_info = {
            "node_id":    self.node_id,
            "ip":         self.local_ip,
            "port":       self.port,
            "join_time":  time.time(),
            "last_seen":  time.time(),
            "device_type":"python",
            "status":     "idle",
        }
        for ni in list(self._registry.values()):
            await self._post(ni.ip, ni.port, "/nodes/register", my_info)

    async def _greet_node(self, ni: NodeInfo) -> None:
        """Register with a newly discovered peer."""
        my_info = {
            "node_id":    self.node_id,
            "ip":         self.local_ip,
            "port":       self.port,
            "join_time":  time.time(),
            "last_seen":  time.time(),
            "device_type":"python",
            "status":     "idle",
        }
        await self._post(ni.ip, ni.port, "/nodes/register", my_info)

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            msg = make_heartbeat(self.node_id, "idle")
            for ni in list(self._registry.values()):
                asyncio.create_task(
                    self._post(ni.ip, ni.port, "/heartbeat", msg)
                )

    async def on_heartbeat(self, data: dict) -> None:
        nid = data.get("node_id")
        if nid and nid in self._registry:
            self._registry[nid].last_seen = time.time()

    # ── Cleanup ───────────────────────────────────────────────────────────────

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL)
            await self.storage.cleanup_expired()
            # Remove stale nodes from registry
            now = time.time()
            stale = [nid for nid, ni in self._registry.items()
                     if now - ni.last_seen > HEARTBEAT_TIMEOUT * 5]
            for nid in stale:
                self._registry.pop(nid, None)
                log.info("Pruned stale node: %s", nid[:8])

    # ── HTTP helper ───────────────────────────────────────────────────────────

    async def _post(self, ip: str, port: int,
                    path: str, body: dict) -> bool:
        if not port:     # browser nodes have port=0
            return False
        url = f"http://{ip}:{port}{path}"
        try:
            async with self._session.post(url, json=body) as resp:
                return resp.status < 400
        except Exception as e:
            log.debug("POST %s failed: %s", url, e)
            return False

    # ── Broadcast wrapper for coordinator ─────────────────────────────────────

    async def _broadcast_wrapper(self, job_id: str, msg: dict) -> None:
        if self._broadcast_to_browsers:
            await self._broadcast_to_browsers(job_id, msg)