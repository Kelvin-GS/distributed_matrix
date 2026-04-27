/**
 * worker.js — Runs inside a browser Web Worker (background CPU thread).
 *
 * The main thread (app.js) posts an assign_block message here.
 * This thread performs the full triple-loop matrix multiplication
 * on the phone's or browser's own CPU, then posts the result back.
 *
 * No network access happens here — pure compute only.
 *
 * Optimisation: uses Float64Array for the result matrix and transfers
 * the underlying ArrayBuffer back to the main thread (zero-copy).
 */

self.onmessage = function (e) {
  const msg = e.data;

  if (msg.type === "assign_block") {
    // Validate inputs before computing
    if (!msg.A_block || !msg.B || !msg.A_block.length || !msg.B.length) {
      self.postMessage({
        type:  "error",
        error: "Invalid block data: A_block or B is missing or empty",
      });
      return;
    }

    const A_block = msg.A_block;
    const B       = msg.B;
    const rows    = A_block.length;
    const k       = B.length;
    const n       = B[0].length;

    // Flat Float64Array: rows × n stored in row-major order
    // Single contiguous buffer = better cache + zero-copy transfer
    const C_flat = new Float64Array(rows * n);

    // Classic O(r·k·n) triple-loop — same algorithm as worker.py
    const t0 = performance.now();

    for (let i = 0; i < rows; i++) {
      const A_row  = A_block[i];
      const offset = i * n;
      for (let p = 0; p < k; p++) {
        const a_ip  = A_row[p];
        const B_row = B[p];
        for (let j = 0; j < n; j++) {
          C_flat[offset + j] += a_ip * B_row[j];
        }
      }
    }

    const t1        = performance.now();
    const elapsedMs = t1 - t0;

    // Convert flat array to nested for JSON compatibility
    // (coordinator expects List[List[float]])
    const C_nested = [];
    for (let i = 0; i < rows; i++) {
      C_nested.push(Array.from(C_flat.subarray(i * n, (i + 1) * n)));
    }

    // Metrics
    const operations = 2 * rows * k * n;
    const mflops     = elapsedMs > 0 ? operations / elapsedMs / 1e3 : 0;

    self.postMessage({
      type:       "block_result",
      job_id:     msg.job_id,
      block_id:   msg.block_id,
      attempt_id: msg.attempt_id || 0,
      partial_C:  C_nested,
      metrics: {
        compute_time_ms:  parseFloat(elapsedMs.toFixed(3)),
        rows_processed:   rows,
        total_operations: operations,
        mflops:           parseFloat(mflops.toFixed(4)),
        device_type:      "browser",
      }
    });
  }
};
