"""
mDNS-based zero-configuration node discovery using the zeroconf library.
Every node announces itself on the LAN; every node listens for announcements.
No central registry, no manual IP entry.

Thread safety: zeroconf callbacks run in a background thread.
All mutations to the main asyncio loop are dispatched via
loop.call_soon_threadsafe() to prevent race conditions.
"""

import asyncio
import logging
import socket
import time
from typing import Callable, Optional, Dict

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
                 port: int,
                 on_node_found: Callable[[NodeInfo], None],
                 on_node_lost:  Callable[[str], None]):
        self.node_id       = node_id
        self.port          = port             # actual port, not hardcoded
        self.local_ip      = get_local_ip()
        self.on_found      = on_node_found
        self.on_lost       = on_node_lost
        self._zc: Optional[AsyncZeroconf] = None
        self._browser: Optional[AsyncServiceBrowser] = None
        self._service_name = f"{node_id}.{SERVICE_TYPE}"

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._zc = AsyncZeroconf()
        await self._register()
        await self._browse()
        log.info("Discovery started. Local IP: %s, port: %d",
                 self.local_ip, self.port)

    async def stop(self) -> None:
        if self._browser:
            self._browser.cancel()
            self._browser = None
        if self._zc:
            await self._zc.async_unregister_all_services()
            await self._zc.async_close()
            self._zc = None

    # ── Register self ─────────────────────────────────────────────────────────

    async def _register(self) -> None:
        info = ServiceInfo(
            type_    = SERVICE_TYPE,
            name     = self._service_name,
            addresses= [socket.inet_aton(self.local_ip)],
            port     = self.port,           # use actual port
            properties={
                b"node_id":   self.node_id.encode(),
                b"join_time": str(time.time()).encode(),
            },
        )
        await self._zc.async_register_service(info)
        log.info("Registered mDNS service: %s (port %d)",
                 self._service_name, self.port)

    # ── Browse for peers ──────────────────────────────────────────────────────

    async def _browse(self) -> None:
        loop = asyncio.get_running_loop()
        listener = _Listener(
            my_node_id = self.node_id,
            zc         = self._zc.zeroconf,
            on_found   = self.on_found,
            on_lost    = self.on_lost,
            loop       = loop,
        )
        self._browser = AsyncServiceBrowser(
            self._zc.zeroconf, SERVICE_TYPE, listener
        )


class _Listener(ServiceListener):
    """
    Receives zeroconf callbacks on a background thread.
    Dispatches node_found / node_lost to the asyncio event loop
    thread-safely via loop.call_soon_threadsafe().
    """
    def __init__(self, my_node_id, zc, on_found, on_lost, loop):
        self._my_id    = my_node_id
        self._zc       = zc
        self._found    = on_found
        self._lost     = on_lost
        self._loop     = loop
        # Robust mapping: service_name → node_id (avoids string parsing)
        self._name_map: Dict[str, str] = {}

    def _extract_ipv4(self, info: ServiceInfo) -> Optional[str]:
        """Safely extract the first IPv4 address, skipping IPv6."""
        for addr in info.addresses:
            if len(addr) == 4:   # IPv4 = 4 bytes
                return socket.inet_ntoa(addr)
        return None

    def add_service(self, zc, type_, name):
        info = zc.get_service_info(type_, name)
        if not info:
            return
        props   = {k.decode(): v.decode() for k, v in info.properties.items()}
        node_id = props.get("node_id", "")
        if not node_id or node_id == self._my_id:
            return          # ignore self or unknown

        ip = self._extract_ipv4(info)
        if not ip:
            log.warning("No IPv4 address for node %s — skipping", node_id[:8])
            return

        # Store mapping for robust removal
        self._name_map[name] = node_id

        node = NodeInfo(
            node_id   = node_id,
            ip        = ip,
            port      = info.port,
            join_time = float(props.get("join_time", time.time())),
            last_seen = time.time(),
        )
        log.info("Discovered node: %s @ %s:%d", node_id[:8], ip, info.port)
        # Thread-safe dispatch to asyncio loop
        self._loop.call_soon_threadsafe(self._found, node)

    def remove_service(self, zc, type_, name):
        # Use the name_map for robust ID lookup instead of string parsing
        node_id = self._name_map.pop(name, None)
        if not node_id:
            # Fallback: parse from service name
            node_id = name.replace(f".{type_}", "").strip(".")
        log.info("Node left: %s", node_id[:8])
        self._loop.call_soon_threadsafe(self._lost, node_id)

    def update_service(self, zc, type_, name):
        """Re-resolve and update the existing node info."""
        info = zc.get_service_info(type_, name)
        if not info:
            return
        props   = {k.decode(): v.decode() for k, v in info.properties.items()}
        node_id = props.get("node_id", "")
        if not node_id or node_id == self._my_id:
            return

        ip = self._extract_ipv4(info)
        if not ip:
            return

        self._name_map[name] = node_id
        node = NodeInfo(
            node_id   = node_id,
            ip        = ip,
            port      = info.port,
            join_time = float(props.get("join_time", time.time())),
            last_seen = time.time(),
        )
        log.debug("Updated node: %s @ %s:%d", node_id[:8], ip, info.port)
        self._loop.call_soon_threadsafe(self._found, node)