"""
FastAPI server — the single HTTP + WebSocket interface for each node.
Serves: REST API  |  WebSocket (browser workers + UI)  |  Static web files

Hardening:
  - All inbound data is validated before use
  - Worker execution is failure-aware (errors reported back)
  - WebSocket lifecycle is properly managed (subscriptions cleaned on disconnect)
  - Coordinator routing is centralised via _route_to_coordinator()
  - Concurrency limited via MAX_CONCURRENT_BLOCKS semaphore
  - Election and sync endpoints are idempotent
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

from config import WEB_DIR, MAX_CONCURRENT_BLOCKS, BlockStatus
from models import NodeInfo
from storage import Storage
from worker  import execute_block, WorkerError

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

    # ── Registries ────────────────────────────────────────────────────────
    _job_sockets: Dict[str, Set[WebSocket]] = {}  # job_id → subscribers
    _ui_sockets:  Set[WebSocket]            = set()
    _block_sem = asyncio.Semaphore(MAX_CONCURRENT_BLOCKS)  # backpressure

    # ── Validation helpers ────────────────────────────────────────────────

    def _validate_keys(data: dict, required: list, context: str) -> str:
        """Return error message if any required key is missing, else ''."""
        missing = [k for k in required if k not in data]
        if missing:
            return f"{context}: missing fields {missing}"
        return ""

    async def _route_to_coordinator(data: dict, backups: list = None) -> bool:
        """
        Route a block result to the coordinator — centralised logic.
        If we ARE the coordinator, handle locally. Otherwise POST.
        """
        job = await node.storage.get_job(data["job_id"])
        if not job:
            return False

        coord_id = job["coordinator_id"]
        bp = backups if backups is not None else job.get("backup_nodes", [])

        if coord_id == node.node_id:
            await node.coordinator.receive_result(data, bp)
            return True

        nodes = node.get_active_nodes()
        coord = next((n for n in nodes if n["node_id"] == coord_id), None)
        if coord:
            return await node._post(coord["ip"], coord["port"], "/result", data)
        return False

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
        nodes_list = node.get_active_nodes()
        return {"nodes": nodes_list, "self": node.node_id}

    @app.get("/coordinator/{job_id}")
    async def get_coordinator(job_id: str):
        """Browser asks who is currently coordinating job_id."""
        job = await node.storage.get_job(job_id)
        if not job:
            return JSONResponse({"error": "job not found"}, 404)
        nodes_list = node.get_active_nodes()
        coord = next((n for n in nodes_list
                      if n["node_id"] == job["coordinator_id"]), None)
        if not coord and job["coordinator_id"] == node.node_id:
            coord = {"node_id": node.node_id,
                     "ip": node.local_ip, "port": node.port}
        return {"coordinator": coord}

    # ── Job submission ────────────────────────────────────────────────────────

    @app.post("/submit")
    async def submit_job(request: Request):
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON body"}, 400)

        A = body.get("matrix_A")
        B = body.get("matrix_B")
        if not A or not B:
            return JSONResponse({"error": "matrix_A and matrix_B required"},
                                status_code=400)
        if not isinstance(A, list) or not isinstance(B, list):
            return JSONResponse({"error": "Matrices must be arrays"}, 400)

        try:
            job_id = await node.coordinator.submit_job(
                A, B, submitter_id=node.node_id
            )
            return {"job_id": job_id, "status": "running",
                    "coordinator": node.node_id}
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        except Exception as e:
            log.error("Job submission failed: %s", e)
            return JSONResponse({"error": "Internal error"}, 500)

    @app.get("/jobs/{job_id}")
    async def job_status(job_id: str):
        job = await node.storage.get_job(job_id)
        if not job:
            return JSONResponse({"error": "not found"}, 404)
        result = await node.storage.get_result(job_id)
        return {
            "job_id":      job_id,
            "status":      job["status"],
            "result":      result,
            "coordinator": job["coordinator_id"],
        }

    # ── Worker endpoint (receives block from coordinator) ─────────────────────

    @app.post("/work")
    async def receive_work(request: Request):
        try:
            assignment = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, 400)

        err = _validate_keys(assignment,
                             ["job_id", "block_id", "A_block", "B"], "/work")
        if err:
            return JSONResponse({"error": err}, 400)

        # Backpressure: limit concurrent block computations
        if _block_sem.locked():
            log.warning("Rejecting block — at concurrency limit")
            return JSONResponse({"error": "Node busy"}, 503)

        asyncio.create_task(_process_block(assignment))
        return {"accepted": True, "block_id": assignment["block_id"]}

    async def _process_block(assignment: dict):
        """Compute a block and report result back to coordinator."""
        async with _block_sem:
            try:
                result = await execute_block(assignment)
                result["worker_id"] = node.node_id
                await _route_to_coordinator(result)
                log.info("[Server] Block %s computed and reported",
                         assignment["block_id"][:8])
            except WorkerError as e:
                log.error("[Server] Block %s failed: %s",
                          assignment["block_id"][:8], e)
                # Report failure back to coordinator
                await _route_to_coordinator({
                    "job_id":    assignment["job_id"],
                    "block_id":  assignment["block_id"],
                    "partial_C": [],
                    "metrics":   {"compute_time_ms": 0, "mflops": 0,
                                  "device_type": "python"},
                    "worker_id": node.node_id,
                    "failed":    True,
                })
            except Exception as e:
                log.exception("[Server] Unexpected error processing block: %s", e)

    # ── Coordinator result ingestion ──────────────────────────────────────────

    @app.post("/result")
    async def receive_result(request: Request):
        try:
            data = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, 400)

        err = _validate_keys(data,
                             ["job_id", "block_id", "partial_C", "metrics"],
                             "/result")
        if err:
            return JSONResponse({"error": err}, 400)

        job = await node.storage.get_job(data["job_id"])
        if job:
            backups = job.get("backup_nodes", [])
            await node.coordinator.receive_result(data, backups)
        return {"received": True}

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    @app.post("/heartbeat")
    async def heartbeat(request: Request):
        try:
            data = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, 400)
        await node.on_heartbeat(data)
        return {"ok": True}

    # ── Node registration ──────────────────────────────────────────────────────

    @app.post("/nodes/register")
    async def register_node(request: Request):
        try:
            data = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, 400)

        err = _validate_keys(data, ["node_id", "ip", "port"],
                             "/nodes/register")
        if err:
            return JSONResponse({"error": err}, 400)

        try:
            ni = NodeInfo.from_dict(data)
        except (ValueError, TypeError) as e:
            return JSONResponse({"error": str(e)}, 400)

        await node.storage.upsert_node(ni.to_dict())
        node._registry[ni.node_id] = ni
        return {"registered": True}

    # ── Election (idempotent) ─────────────────────────────────────────────────

    @app.post("/election/start")
    async def election_start(request: Request):
        try:
            data = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, 400)

        err = _validate_keys(data, ["node_id", "job_id"], "/election/start")
        if err:
            return JSONResponse({"error": err}, 400)

        asyncio.create_task(
            node.election.on_election_received(
                data["node_id"], data["job_id"]
            )
        )
        return {"ok": True}

    @app.post("/election/ok")
    async def election_ok(request: Request):
        try:
            data = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, 400)

        err = _validate_keys(data, ["from_node", "job_id"], "/election/ok")
        if err:
            return JSONResponse({"error": err}, 400)

        asyncio.create_task(
            node.election.on_ok_received(
                data["from_node"], data["job_id"]
            )
        )
        return {"ok": True}

    @app.post("/coordinator/announce")
    async def coordinator_announce(request: Request):
        try:
            data = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, 400)

        err = _validate_keys(data, ["node_id", "job_id"],
                             "/coordinator/announce")
        if err:
            return JSONResponse({"error": err}, 400)

        node.election.on_coordinator_announced(
            data["node_id"], data["job_id"]
        )
        if data["node_id"] == node.node_id:
            asyncio.create_task(
                node.coordinator.resume_job(data["job_id"])
            )
        return {"ok": True}

    # ── State sync (idempotent — safe for duplicate messages) ─────────────────

    @app.post("/sync/state")
    async def sync_state(request: Request):
        try:
            data = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, 400)

        err = _validate_keys(data, ["operation", "data"], "/sync/state")
        if err:
            return JSONResponse({"error": err}, 400)

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
                raw = await ws.receive_text()
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    await ws.send_text(json.dumps(
                        {"type": "error", "message": "Invalid JSON"}
                    ))
                    continue

                mtype = msg.get("type")

                if mtype == "browser_register":
                    # Validate required fields
                    if "node_id" not in msg:
                        continue
                    browser_id = msg["node_id"]
                    node_info  = {
                        "node_id":    browser_id,
                        "ip":         ws.client.host if ws.client else "unknown",
                        "port":       0,
                        "device_type":"browser",
                        "join_time":  time.time(),
                        "last_seen":  time.time(),
                        "status":     "idle",
                    }
                    await node.storage.upsert_node(node_info)
                    node._browser_sockets[browser_id] = ws
                    await ws.send_text(json.dumps({
                        "type":    "registered",
                        "node_id": browser_id,
                        "your_ip": ws.client.host if ws.client else "unknown",
                    }))
                    log.info("[WS] Browser worker registered: %s",
                             browser_id[:8])

                elif mtype == "block_result":
                    # Validate required fields
                    err = _validate_keys(msg,
                                         ["job_id", "block_id", "partial_C", "metrics"],
                                         "WS block_result")
                    if err:
                        log.warning("[WS] Invalid block_result: %s", err)
                        continue

                    msg["worker_id"] = browser_id or "browser-unknown"
                    await _route_to_coordinator(msg)

                elif mtype == "subscribe_job":
                    job_id = msg.get("job_id")
                    if job_id:
                        _job_sockets.setdefault(job_id, set()).add(ws)

                elif mtype == "heartbeat":
                    # Update browser liveness
                    if browser_id:
                        await node.storage.upsert_node({
                            "node_id": browser_id,
                            "ip": ws.client.host if ws.client else "unknown",
                            "port": 0,
                            "device_type": "browser",
                            "status": "idle",
                        })
                    await ws.send_text(json.dumps(
                        {"type": "heartbeat_ack", "timestamp": time.time()}
                    ))

        except WebSocketDisconnect:
            log.info("[WS] Browser disconnected: %s",
                     (browser_id or "unknown")[:8])
        except Exception as e:
            log.error("[WS] Unexpected error: %s", e)
        finally:
            # Clean up all references to this socket
            _ui_sockets.discard(ws)
            if browser_id:
                node._browser_sockets.pop(browser_id, None)
                await node.storage.remove_node(browser_id)
            # Clean up job subscriptions
            for job_id in list(_job_sockets.keys()):
                _job_sockets[job_id].discard(ws)
                if not _job_sockets[job_id]:
                    del _job_sockets[job_id]

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