/**
 * worker.js — runs inside a browser Web Worker thread.
 * Receives a block assignment, computes matrix multiplication in JavaScript,
 * returns the result with detailed metrics.
 *
 * This runs on the phone's/browser own CPU — not the server's.
 *
 * The main thread posts messages here; this thread posts results back.
 */

self.onmessage = function (e) {
  const msg = e.data;

  if (msg.type === "assign_block") {
    const t0 = performance.now();
    const A_block = msg.A_block; // Array of row arrays
    const B = msg.B; // Full B matrix
    const rows = A_block.length;
    const k = B.length;
    const n = B[0].length;

    // Initialise result
    const C = new Array(rows);
    for (let i = 0; i < rows; i++) {
      C[i] = new Array(n).fill(0);
    }

    // Classic triple-loop matrix multiplication
    for (let i = 0; i < rows; i++) {
      for (let j = 0; j < n; j++) {
        let sum = 0;
        for (let p = 0; p < k; p++) {
          sum += A_block[i][p] * B[p][j];
        }
        C[i][j] = sum;
      }
    }

    const t1 = performance.now();
    const elapsedMs = t1 - t0;
    const operations = 2 * rows * k * n; // multiply-add pairs
    const mflops = operations / elapsedMs / 1e3;

    self.postMessage({
      type: "block_result",
      job_id: msg.job_id,
      block_id: msg.block_id,
      partial_C: C,
      metrics: {
        compute_time_ms: parseFloat(elapsedMs.toFixed(3)),
        rows_processed: rows,
        total_operations: operations,
        mflops: parseFloat(mflops.toFixed(4)),
        device_type: "browser",
        user_agent: "see-main-thread",
      },
    });
  }
};
