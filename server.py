"""
FastAPI server — the single HTTP + WebSocket interface for each node.
Serves: REST API  |  WebSocket (browser workers + UI)  |  Static web files
"""

import asyncio
import json
import logging
import os
import time
from typing import Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from config import WEB_DIR, NODE_PORT
from models import NodeInfo
from storage import Storage
from worker  import execute_block

log = logging.getLogger("server")


def create_app(node) -> FastAPI:
    """
    `node` is the Node instance (node.py). Injected here to avoid circular
    imports while keeping the server stateless about node internals.
    """
    app = FastAPI(title="DistMatMul Node")

    # ── Static web files ────────────────────────────────────────────────────
    if os.path.exists(WEB_DIR):
        app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

    # ── WebSocket connection registry (browser clients) ───────────────────
    # job_id → set of WebSocket connections subscribed to that job
    _job_sockets: Dict[str, Set[WebSocket]] = {}
    # node_id → set of WebSocket connections (general UI)
    _ui_sockets:  Set[WebSocket]            = set()

    # ─────────────────────────────────────────────────────────────────────────
    # HTTP ENDPOINTS
    # ─────────────────────────────────────────────────────────────────────────

    @app.get("/")
    async def index():
        return FileResponse(os.path.join(WEB_DIR, "index.html"))

    @app.get("/health")
    async def health():
        return {"node_id": node.node_id, "status": "ok",
                "timestamp": time.time()}

    @app.get("/nodes")
    async def list_nodes():
        nodes = node.get_active_nodes()
        return {"nodes": nodes, "self": node.node_id}

    @app.get("/coordinator/{job_id}")
    async def get_coordinator(job_id: str):
        """Browser asks who is currently coordinating job_id."""
        job = await node.storage.get_job(job_id)
        if not job:
            return JSONResponse({"error": "job not found"}, 404)
        nodes = node.get_active_nodes()
        coord = next((n for n in nodes
                      if n["node_id"] == job["coordinator_id"]), None)
        if not coord and job["coordinator_id"] == node.node_id:
            coord = {"node_id": node.node_id,
                     "ip": node.local_ip, "port": NODE_PORT}
        return {"coordinator": coord}

    # ── Job submission ────────────────────────────────────────────────────────

    @app.post("/submit")
    async def submit_job(request: Request):
        body = await request.json()
        A    = body.get("matrix_A")
        B    = body.get("matrix_B")
        if not A or not B:
            return JSONResponse({"error": "matrix_A and matrix_B required"},
                                status_code=400)
        try:
            job_id = await node.coordinator.submit_job(
                A, B, submitter_id=node.node_id
            )
            return {"job_id": job_id, "status": "running",
                    "coordinator": node.node_id}
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)

    @app.get("/jobs/{job_id}")
    async def job_status(job_id: str):
        job = await node.storage.get_job(job_id)
        if not job:
            return JSONResponse({"error": "not found"}, 404)
        result = await node.storage.get_result(job_id)
        return {
            "job_id":   job_id,
            "status":   job["status"],
            "result":   result,
            "coordinator": job["coordinator_id"],
        }

    # ── Worker endpoint (receives block from coordinator) ─────────────────────

    @app.post("/work")
    async def receive_work(request: Request):
        assignment = await request.json()
        # Run in background so we don't block the HTTP response
        asyncio.create_task(_process_block(assignment))
        return {"accepted": True, "block_id": assignment["block_id"]}

    async def _process_block(assignment: dict):
        result = await execute_block(assignment)
        # Report result back to coordinator
        job    = await node.storage.get_job(assignment["job_id"])
        if not job:
            return
        result["worker_id"] = node.node_id
        coord_id = job["coordinator_id"]

        if coord_id == node.node_id:
            # We ARE the coordinator — handle locally
            backups = job.get("backup_nodes", [])
            await node.coordinator.receive_result(result, backups)
        else:
            # POST result back to coordinator
            nodes    = node.get_active_nodes()
            coord    = next((n for n in nodes
                             if n["node_id"] == coord_id), None)
            if coord:
                await node._post(coord["ip"], coord["port"],
                                 "/result", result)

    # ── Coordinator result ingestion ──────────────────────────────────────────

    @app.post("/result")
    async def receive_result(request: Request):
        data = await request.json()
        job  = await node.storage.get_job(data["job_id"])
        if job:
            backups = job.get("backup_nodes", [])
            await node.coordinator.receive_result(data, backups)
        return {"received": True}

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    @app.post("/heartbeat")
    async def heartbeat(request: Request):
        data = await request.json()
        await node.on_heartbeat(data)
        return {"ok": True}

    # ── Node registration ──────────────────────────────────────────────────────

    @app.post("/nodes/register")
    async def register_node(request: Request):
        data    = await request.json()
        ni      = NodeInfo(**data)
        await node.storage.upsert_node(ni.to_dict())
        node._registry[ni.node_id] = ni
        return {"registered": True}

    # ── Election ──────────────────────────────────────────────────────────────

    @app.post("/election/start")
    async def election_start(request: Request):
        data = await request.json()
        asyncio.create_task(
            node.election.on_election_received(
                data["node_id"], data["job_id"]
            )
        )
        return {"ok": True}

    @app.post("/election/ok")
    async def election_ok(request: Request):
        data = await request.json()
        asyncio.create_task(
            node.election.on_ok_received(
                data["from_node"], data["job_id"]
            )
        )
        return {"ok": True}

    @app.post("/coordinator/announce")
    async def coordinator_announce(request: Request):
        data = await request.json()
        node.election.on_coordinator_announced(
            data["node_id"], data["job_id"]
        )
        if data["node_id"] == node.node_id:
            asyncio.create_task(
                node.coordinator.resume_job(data["job_id"])
            )
        return {"ok": True}

    # ── State sync (backup replication) ───────────────────────────────────────

    @app.post("/sync/state")
    async def sync_state(request: Request):
        data = await request.json()
        await node.storage.apply_sync(data["operation"], data["data"])
        return {"synced": True}

    # ─────────────────────────────────────────────────────────────────────────
    # WEBSOCKET — browser workers + real-time UI
    # ─────────────────────────────────────────────────────────────────────────

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await ws.accept()
        _ui_sockets.add(ws)
        browser_id = None
        log.info("[WS] Browser connected")

        try:
            while True:
                raw  = await ws.receive_text()
                msg  = json.loads(raw)
                mtype= msg.get("type")

                if mtype == "browser_register":
                    # Browser announces itself as a potential worker
                    browser_id = msg["node_id"]
                    node_info  = {
                        "node_id":    browser_id,
                        "ip":         ws.client.host,
                        "port":       0,            # browsers have no port
                        "device_type":"browser",
                        "join_time":  time.time(),
                        "last_seen":  time.time(),
                        "status":     "idle",
                    }
                    await node.storage.upsert_node(node_info)
                    node._browser_sockets[browser_id] = ws
                    await ws.send_text(json.dumps({
                        "type": "registered",
                        "node_id": browser_id,
                        "your_ip": ws.client.host,
                    }))
                    log.info("[WS] Browser worker registered: %s",
                             browser_id[:8])

                elif mtype == "block_result":
                    # Browser has finished computing its block
                    msg["worker_id"] = browser_id or "browser-unknown"
                    job = await node.storage.get_job(msg["job_id"])
                    if job:
                        backups = job.get("backup_nodes", [])
                        coord_id = job["coordinator_id"]
                        if coord_id == node.node_id:
                            await node.coordinator.receive_result(msg, backups)
                        else:
                            nodes = node.get_active_nodes()
                            coord = next((n for n in nodes
                                          if n["node_id"] == coord_id), None)
                            if coord:
                                await node._post(coord["ip"], coord["port"],
                                                 "/result", msg)

                elif mtype == "subscribe_job":
                    job_id = msg.get("job_id")
                    if job_id:
                        _job_sockets.setdefault(job_id, set()).add(ws)

                elif mtype == "heartbeat":
                    await ws.send_text(json.dumps(
                        {"type": "heartbeat_ack", "timestamp": time.time()}
                    ))

        except WebSocketDisconnect:
            log.info("[WS] Browser disconnected: %s",
                     (browser_id or "unknown")[:8])
            _ui_sockets.discard(ws)
            if browser_id:
                node._browser_sockets.pop(browser_id, None)
                await node.storage.remove_node(browser_id)

    # ── Helper: push job_complete to subscribed browsers ─────────────────────

    async def broadcast_to_job_browsers(job_id: str, msg: dict) -> None:
        text    = json.dumps(msg)
        dead    = set()
        targets = _job_sockets.get(job_id, set()) | _ui_sockets
        for ws in targets:
            try:
                await ws.send_text(text)
            except Exception:
                dead.add(ws)
        for ws in dead:
            _ui_sockets.discard(ws)

    # Attach helper to node so coordinator.py can call it
    node._broadcast_to_browsers = broadcast_to_job_browsers

    return app