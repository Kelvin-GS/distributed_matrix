/**
 * app.js — Main browser thread.
 *
 * Responsibilities:
 *  1. Connect to the node's WebSocket and register as a worker node
 *  2. Receive block assignments and dispatch them to the Web Worker thread
 *  3. Receive computed results from the Web Worker and send them back to coordinator
 *  4. Handle job submissions from the UI
 *  5. Display real-time metrics proving local computation
 *  6. Poll for results on reconnect (fault tolerance for disconnected clients)
 */

const App = (() => {
  let ws          = null;
  let workerId    = null;   // stable browser node ID (persisted in localStorage)
  let webWorker   = null;   // Web Worker thread
  let currentJobId= null;
  const logs      = [];

  // ── Connection ─────────────────────────────────────────────────────────────

  function connect() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(`${proto}//${location.host}/ws`);

    ws.onopen = () => {
      // Generate or restore a stable browser node ID
      workerId = localStorage.getItem("matmul_node_id");
      if (!workerId) {
        workerId = "browser-" + crypto.randomUUID();
        localStorage.setItem("matmul_node_id", workerId);
      }

      // Register with the Python node as a worker
      ws.send(JSON.stringify({
        type:        "browser_register",
        node_id:     workerId,
        user_agent:  navigator.userAgent,
        device_type: "browser",
      }));

      // Start the Web Worker compute thread
      if (!webWorker) {
        webWorker = new Worker("/static/worker.js");
        webWorker.onmessage = onWorkerResult;
        webWorker.onerror   = (e) => addLog("❌ Web Worker error: " + e.message);
      }

      // Keepalive heartbeat every 2 seconds
      setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "heartbeat" }));
        }
      }, 2000);

      updateStatus("🟢 Connected");
      addLog("✅ Connected as worker node " + workerId.slice(0, 12) + "...");
      updateMetric("node_id", workerId.slice(0, 12) + "...");
      updateMetric("device",  navigator.userAgent.slice(0, 40));
      updateMetric("status",  "Idle — ready");

      // If we had a job running before disconnect, try to fetch its result
      const lastJob = localStorage.getItem("matmul_last_job");
      if (lastJob) {
        addLog("🔄 Checking for pending result from job " + lastJob.slice(0, 8) + "...");
        pollResult(lastJob);
      }
    };

    ws.onmessage = (event) => {
      try {
        handleServerMessage(JSON.parse(event.data));
      } catch (e) {
        addLog("⚠️ Bad message from server: " + e.message);
      }
    };

    ws.onerror = (e) => addLog("⚠️ WebSocket error");

    ws.onclose = () => {
      updateStatus("🔴 Disconnected — reconnecting in 3s...");
      addLog("⚠️ Connection lost. Retrying...");
      setTimeout(connect, 3000);
    };
  }

  // ── Message dispatcher ──────────────────────────────────────────────────────

  function handleServerMessage(msg) {
    switch (msg.type) {

      case "registered":
        addLog("🔗 Registered as worker node on this network");
        break;

      case "assign_block":
        // We received a block of rows to compute
        addLog(`📦 Block ${msg.block_id.slice(-8)} assigned — rows ${msg.row_start}–${msg.row_end - 1}`);
        updateMetric("status",     "⚙️ Computing...");
        updateMetric("last_block", msg.block_id.slice(-8));
        updateMetric("job_id",     msg.job_id.slice(0, 8) + "...");

        // Dispatch to Web Worker (background CPU thread)
        webWorker.postMessage({
          type:      "assign_block",
          job_id:    msg.job_id,
          block_id:  msg.block_id,
          row_start: msg.row_start,
          row_end:   msg.row_end,
          A_block:   msg.A_block,
          B:         msg.B,
        });
        break;

      case "job_complete":
        addLog(`✅ Job ${msg.job_id.slice(0, 8)} complete — ${msg.duration_ms.toFixed(1)}ms total`);
        addLog(`👥 Workers used: ${(msg.workers_used || []).length}`);
        updateMetric("status", "✅ Idle — last job complete");
        currentJobId = msg.job_id;
        localStorage.setItem("matmul_last_job", msg.job_id);
        renderResult(msg.job_id, msg.result_matrix, msg.duration_ms, msg.workers_used || []);
        break;

      case "heartbeat_ack":
        // Silently acknowledge — no log spam
        break;

      default:
        addLog("📨 " + JSON.stringify(msg).slice(0, 100));
    }
  }

  // ── Web Worker result handler ───────────────────────────────────────────────

  function onWorkerResult(e) {
    const result = e.data;

    // Add user_agent (not available inside Worker)
    result.metrics.user_agent = navigator.userAgent;

    // Update metrics panel with proof of local computation
    updateMetric("compute_time", result.metrics.compute_time_ms + " ms");
    updateMetric("mflops",       result.metrics.mflops.toFixed(4));
    updateMetric("operations",   result.metrics.total_operations.toLocaleString());
    updateMetric("rows_done",    result.metrics.rows_processed);
    updateMetric("status",       "✅ Block done — idle");

    addLog(
      `✅ Block ${result.block_id.slice(-8)} computed locally — ` +
      `${result.metrics.compute_time_ms}ms, ${result.metrics.mflops} MFLOPS, ` +
      `${result.metrics.total_operations.toLocaleString()} ops`
    );

    // Send result back to coordinator via WebSocket
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type:      "block_result",
        job_id:    result.job_id,
        block_id:  result.block_id,
        partial_C: result.partial_C,
        metrics:   result.metrics,
      }));
    }
  }

  // ── Job submission ──────────────────────────────────────────────────────────

  async function submitJob(A, B) {
    addLog(`🚀 Submitting job: ${A.length}×${A[0].length} × ${B.length}×${B[0].length}`);
    updateMetric("status", "⏳ Job submitted...");

    const resp = await fetch("/submit", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ matrix_A: A, matrix_B: B }),
    });
    const data = await resp.json();

    if (data.job_id) {
      currentJobId = data.job_id;
      localStorage.setItem("matmul_last_job", data.job_id);
      addLog(`📋 Job ID: ${data.job_id}`);
      addLog(`🎯 Coordinator: ${data.coordinator.slice(0, 12)}...`);

      // Subscribe to live updates for this job
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "subscribe_job", job_id: data.job_id }));
      }
    } else {
      addLog("❌ Submission error: " + (data.error || "Unknown error"));
      updateMetric("status", "❌ Error");
    }
    return data;
  }

  // ── Result polling (for reconnected clients) ────────────────────────────────

  async function pollResult(jobId) {
    try {
      const resp = await fetch(`/jobs/${jobId}`);
      const data = await resp.json();
      if (data.status === "complete" && data.result) {
        addLog("📥 Result received for job " + jobId.slice(0, 8));
        renderResult(jobId, data.result, null, []);
      } else if (data.status === "running") {
        addLog("⏳ Job " + jobId.slice(0, 8) + " still running — subscribing...");
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "subscribe_job", job_id: jobId }));
        }
      }
      return data;
    } catch (e) {
      addLog("⚠️ Could not fetch job status: " + e.message);
    }
  }

  // ── Result display ──────────────────────────────────────────────────────────

  function renderResult(jobId, matrix, durationMs, workers) {
    const el = document.getElementById("result-area");
    if (!el) return;

    const rows = matrix.length;
    const cols = matrix[0] ? matrix[0].length : 0;

    let html = `
      <div class="result-card">
        <div class="result-header">
          <span class="result-title">Result Matrix C — ${rows}×${cols}</span>
          <span class="result-badge">Job ${jobId.slice(0, 8)}</span>
        </div>`;

    if (durationMs != null) {
      html += `<div class="result-meta">
        <span>⏱ ${durationMs.toFixed(1)}ms total</span>
        <span>👥 ${workers.length} worker(s)</span>
        <span>📐 ${rows}×${cols} result</span>
      </div>`;
    }

    if (rows <= 12 && cols <= 12) {
      // Show full matrix for small sizes
      html += `<div class="matrix-display">`;
      matrix.forEach(row => {
        html += `<div class="matrix-row">`;
        row.forEach(v => {
          html += `<span class="cell">${parseFloat(v.toFixed(4))}</span>`;
        });
        html += `</div>`;
      });
      html += `</div>`;
    } else {
      // Large matrix — show corner preview + download
      html += `<p class="muted">Matrix too large to display inline (${rows}×${cols}). Showing top-left 6×6:</p>`;
      html += `<div class="matrix-display">`;
      for (let i = 0; i < Math.min(6, rows); i++) {
        html += `<div class="matrix-row">`;
        for (let j = 0; j < Math.min(6, cols); j++) {
          html += `<span class="cell">${parseFloat(matrix[i][j].toFixed(2))}</span>`;
        }
        if (cols > 6) html += `<span class="cell muted">…</span>`;
        html += `</div>`;
      }
      if (rows > 6) html += `<div class="matrix-row"><span class="cell muted">…</span></div>`;
      html += `</div>`;
      html += `<button class="btn-download" onclick="App.downloadResult('${jobId}')">⬇ Download Full CSV</button>`;
    }

    html += `</div>`;
    el.innerHTML = html;
  }

  // ── CSV download ────────────────────────────────────────────────────────────

  async function downloadResult(jobId) {
    const resp = await fetch(`/jobs/${jobId || currentJobId}`);
    const data = await resp.json();
    if (!data.result) { addLog("⚠️ No result to download yet"); return; }

    const csv  = data.result.map(row => row.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href = url; a.download = `result_${(jobId || currentJobId).slice(0, 8)}.csv`;
    a.click(); URL.revokeObjectURL(url);
    addLog("⬇ CSV downloaded");
  }

  // ── UI helpers ──────────────────────────────────────────────────────────────

  function addLog(msg) {
    const ts = new Date().toLocaleTimeString();
    logs.push(`[${ts}] ${msg}`);
    if (logs.length > 200) logs.shift();
    const el = document.getElementById("log");
    if (el) { el.textContent = logs.slice(-60).join("\n"); el.scrollTop = el.scrollHeight; }
  }

  function updateStatus(s) {
    const el = document.getElementById("conn-status");
    if (el) el.textContent = s;
  }

  function updateMetric(key, val) {
    const el = document.getElementById("m-" + key);
    if (el) el.textContent = val;
  }

  // ── Boot ────────────────────────────────────────────────────────────────────

  document.addEventListener("DOMContentLoaded", connect);

  return { submitJob, pollResult, downloadResult, addLog };
})();
