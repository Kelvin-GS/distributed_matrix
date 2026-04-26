"""
mDNS-based zero-configuration node discovery using the zeroconf library.
Every node announces itself on the LAN; every node listens for announcements.
No central registry, no manual IP entry.
"""

import asyncio
import json
import logging
import socket
import time
import uuid
from typing import Callable, Optional

from zeroconf import ServiceInfo, Zeroconf, ServiceBrowser, ServiceListener
from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser

from config import SERVICE_TYPE, NODE_PORT
from models import NodeInfo

log = logging.getLogger("discovery")


def get_local_ip() -> str:
    """Best-effort LAN IP (not 127.0.0.1)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


class NodeDiscovery:
    def __init__(self, node_id: str,
                 on_node_found: Callable[[NodeInfo], None],
                 on_node_lost:  Callable[[str], None]):
        self.node_id       = node_id
        self.local_ip      = get_local_ip()
        self.on_found      = on_node_found
        self.on_lost       = on_node_lost
        self._zc: Optional[AsyncZeroconf] = None
        self._browser      = None
        self._service_name = f"{node_id}.{SERVICE_TYPE}"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._zc = AsyncZeroconf()
        await self._register()
        await self._browse()
        log.info("Discovery started. Local IP: %s", self.local_ip)

    async def stop(self) -> None:
        if self._zc:
            await self._zc.async_unregister_all_services()
            await self._zc.async_close()

    # ── Register self ─────────────────────────────────────────────────────────

    async def _register(self) -> None:
        info = ServiceInfo(
            type_    = SERVICE_TYPE,
            name     = self._service_name,
            addresses= [socket.inet_aton(self.local_ip)],
            port     = NODE_PORT,
            properties={
                b"node_id":   self.node_id.encode(),
                b"join_time": str(time.time()).encode(),
            },
        )
        await self._zc.async_register_service(info)
        log.info("Registered mDNS service: %s", self._service_name)

    # ── Browse for peers ──────────────────────────────────────────────────────

    async def _browse(self) -> None:
        listener = _Listener(
            my_node_id = self.node_id,
            zc         = self._zc.zeroconf,
            on_found   = self.on_found,
            on_lost    = self.on_lost,
        )
        self._browser = AsyncServiceBrowser(
            self._zc.zeroconf, SERVICE_TYPE, listener
        )


class _Listener(ServiceListener):
    def __init__(self, my_node_id, zc, on_found, on_lost):
        self._my_id   = my_node_id
        self._zc      = zc
        self._found   = on_found
        self._lost    = on_lost

    def add_service(self, zc, type_, name):
        info = zc.get_service_info(type_, name)
        if not info:
            return
        props    = {k.decode(): v.decode() for k, v in info.properties.items()}
        node_id  = props.get("node_id", "")
        if node_id == self._my_id:
            return          # ignore self
        ip = socket.inet_ntoa(info.addresses[0])
        node = NodeInfo(
            node_id   = node_id,
            ip        = ip,
            port      = info.port,
            join_time = float(props.get("join_time", time.time())),
            last_seen = time.time(),
        )
        log.info("Discovered node: %s @ %s:%d", node_id[:8], ip, info.port)
        self._found(node)

    def remove_service(self, zc, type_, name):
        # Extract node_id from service name  (format: {node_id}._matmul._tcp.local.)
        node_id = name.replace(f".{type_}", "").strip(".")
        log.info("Node left: %s", node_id[:8])
        self._lost(node_id)

    def update_service(self, zc, type_, name):
        self.add_service(zc, type_, name)