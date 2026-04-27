/**
 * app.js — main browser thread.
 * Manages: node registration, job submission, block computation dispatch
 * to Web Worker, result reporting, real-time UI updates.
 */

const App = (() => {
  let ws = null;
  let workerId = null;
  let webWorker = null;
  let currentJobId = null;
  const logs = [];
  const metrics = {};

  // ── Connect ──────────────────────────────────────────────────────────────

  function connect() {
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(`${protocol}//${location.host}/ws`);

    ws.onopen = () => {
      // Generate a stable browser node ID
      workerId =
        localStorage.getItem("matmul_node_id") ||
        "browser-" + crypto.randomUUID();

      localStorage.setItem("matmul_node_id", workerId);

      ws.send(
        JSON.stringify({
          type: "browser_register",
          node_id: workerId,
          user_agent: navigator.userAgent,
          device_type: "browser",
        }),
      );

      // Start Web Worker for compute
      webWorker = new Worker("/static/worker.js");
      webWorker.onmessage = onWorkerResult;

      // Heartbeat every 2s
      setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "heartbeat" }));
        }
      }, 2000);

      addLog("✅ Connected to network as " + workerId.slice(0, 8) + "...");
      updateStatus("Connected");
    };

    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      handleServerMessage(msg);
    };

    ws.onclose = () => {
      updateStatus("Disconnected — reconnecting...");
      addLog("⚠️ Connection lost. Retrying in 3s...");
      setTimeout(connect, 3000);
    };
  }

  // ── Handle messages from server ─────────────────────────────────────────

  function handleServerMessage(msg) {
    switch (msg.type) {
      case "registered":
        addLog("🔗 Registered as worker node");
        break;

      case "assign_block":
        addLog(
          `📦 Received block ${msg.block_id.slice(-6)} — ` +
            `rows ${msg.row_start}–${msg.row_end - 1}`,
        );
        updateMetric("last_block", msg.block_id.slice(-6));
        updateMetric("status", "⚙️ Computing...");

        // Dispatch to Web Worker thread
        webWorker.postMessage({
          type: "assign_block",
          job_id: msg.job_id,
          block_id: msg.block_id,
          row_start: msg.row_start,
          row_end: msg.row_end,
          A_block: msg.A_block,
          B: msg.B,
        });
        break;

      case "job_complete":
        addLog(
          `✅ Job ${msg.job_id.slice(0, 8)} complete — ` +
            `${msg.duration_ms.toFixed(1)}ms`,
        );
        updateMetric("status", "✅ Idle");
        currentJobId = msg.job_id;
        renderResult(
          msg.job_id,
          msg.result_matrix,
          msg.duration_ms,
          msg.workers_used,
        );
        break;

      case "heartbeat_ack":
        break;

      default:
        addLog("📨 " + JSON.stringify(msg).slice(0, 80));
    }
  }

  // ── Web Worker result → send back to coordinator via WS ─────────────────

  function onWorkerResult(e) {
    const result = e.data;
    result.metrics.user_agent = navigator.userAgent;

    addLog(
      `✅ Block ${result.block_id.slice(-6)} done — ` +
        `${result.metrics.compute_time_ms}ms, ` +
        `${result.metrics.mflops} MFLOPS`,
    );

    updateMetric("compute_time", result.metrics.compute_time_ms + " ms");
    updateMetric("mflops", result.metrics.mflops);
    updateMetric("ops", result.metrics.total_operations.toLocaleString());
    updateMetric("rows", result.metrics.rows_processed);
    updateMetric("status", "✅ Block done — idle");

    ws.send(
      JSON.stringify({
        type: "block_result",
        job_id: result.job_id,
        block_id: result.block_id,
        partial_C: result.partial_C,
        metrics: result.metrics,
      }),
    );
  }

  // ── Job submission ───────────────────────────────────────────────────────

  async function submitJob(A, B) {
    const resp = await fetch("/submit", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ matrix_A: A, matrix_B: B }),
    });

    const data = await resp.json();

    if (data.job_id) {
      currentJobId = data.job_id;
      addLog(`🚀 Job submitted: ${data.job_id.slice(0, 8)}`);

      // Subscribe for live updates
      ws.send(
        JSON.stringify({
          type: "subscribe_job",
          job_id: data.job_id,
        }),
      );
    } else {
      addLog("❌ Error: " + (data.error || "Unknown"));
    }

    return data;
  }

  // ── Poll result for reconnected clients ──────────────────────────────────

  async function pollResult(jobId) {
    const resp = await fetch(`/jobs/${jobId}`);
    const data = await resp.json();

    if (data.status === "complete" && data.result) {
      renderResult(jobId, data.result, null, []);
    }

    return data;
  }

  // ── UI helpers ────────────────────────────────────────────────────────────

  function addLog(msg) {
    logs.push(`[${new Date().toLocaleTimeString()}] ${msg}`);
    const el = document.getElementById("log");
    if (el) el.textContent = logs.slice(-50).join("\n");
  }

  function updateStatus(s) {
    const el = document.getElementById("status");
    if (el) el.textContent = s;
  }

  function updateMetric(key, val) {
    metrics[key] = val;
    const el = document.getElementById("metric-" + key);
    if (el) el.textContent = val;
  }

  function renderResult(jobId, matrix, durationMs, workers) {
    const el = document.getElementById("result-area");
    if (!el) return;

    let html = `<div class="result-card">
      <h3>Job ${jobId.slice(0, 8)} — Complete</h3>`;

    if (durationMs) {
      html += `<p>Total time: <strong>${durationMs.toFixed(1)}ms</strong></p>`;
    }

    if (workers.length) {
      html += `<p>Workers: ${workers.length}</p>`;
    }

    html += `<div class="matrix-display">`;

    if (matrix.length <= 10) {
      // Show full matrix for small sizes
      matrix.forEach((row) => {
        html += `<div class="matrix-row">`;
        row.forEach((v) => {
          html += `<span class="cell">${v.toFixed(2)}</span>`;
        });
        html += `</div>`;
      });
    } else {
      html +=
        `<p>${matrix.length}×${matrix[0].length} matrix — ` +
        `too large to display inline.</p>`;
      html += `<button onclick="App.downloadResult()">⬇ Download CSV</button>`;
    }

    html += `</div></div>`;
    el.innerHTML = html;
  }

  function downloadResult() {
    fetch(`/jobs/${currentJobId}`)
      .then((r) => r.json())
      .then((data) => {
        if (!data.result) return;

        const csv = data.result.map((r) => r.join(",")).join("\n");
        const blob = new Blob([csv], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");

        a.href = url;
        a.download = `result_${currentJobId.slice(0, 8)}.csv`;
        a.click();
      });
  }

  return { connect, submitJob, pollResult, downloadResult, addLog };
})();

// Boot
document.addEventListener("DOMContentLoaded", () => {
  App.connect();

  // Restore last job on reconnect
  const lastJob = localStorage.getItem("matmul_last_job");
  if (lastJob) {
    App.pollResult(lastJob).then((d) => {
      if (d.status === "complete") {
        App.addLog("🔄 Restored result from previous session");
      }
    });
  }
});
