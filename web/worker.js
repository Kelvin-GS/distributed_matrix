/**
 * worker.js — Runs inside a browser Web Worker (background CPU thread).
 *
 * The main thread (app.js) posts an assign_block message here.
 * This thread performs the full triple-loop matrix multiplication
 * on the phone's or browser's own CPU, then posts the result back.
 *
 * No network access happens here — pure compute only.
 */

self.onmessage = function (e) {
  const msg = e.data;

  if (msg.type === "assign_block") {
    const A_block = msg.A_block;   // Array of row arrays  (r × k)
    const B       = msg.B;         // Full matrix B         (k × n)
    const rows    = A_block.length;
    const k       = B.length;
    const n       = B[0].length;

    // Initialise result matrix C (r × n) with zeros
    const C = new Array(rows);
    for (let i = 0; i < rows; i++) {
      C[i] = new Float64Array(n);   // typed array: faster and numerically identical to Python floats
    }

    // Classic O(r·k·n) triple-loop — same algorithm as worker.py
    const t0 = performance.now();

    for (let i = 0; i < rows; i++) {
      const A_row = A_block[i];
      const C_row = C[i];
      for (let p = 0; p < k; p++) {
        const a_ip = A_row[p];
        const B_row = B[p];
        for (let j = 0; j < n; j++) {
          C_row[j] += a_ip * B_row[j];
        }
      }
    }

    const t1        = performance.now();
    const elapsedMs = t1 - t0;

    // Convert Float64Arrays back to plain arrays for JSON serialisation
    const C_plain = Array.from(C, row => Array.from(row));

    const operations = 2 * rows * k * n;     // multiply-add pairs
    const mflops     = operations / elapsedMs / 1e3;

    self.postMessage({
      type:      "block_result",
      job_id:    msg.job_id,
      block_id:  msg.block_id,
      partial_C: C_plain,
      metrics: {
        compute_time_ms:  parseFloat(elapsedMs.toFixed(3)),
        rows_processed:   rows,
        total_operations: operations,
        mflops:           parseFloat(mflops.toFixed(4)),
        device_type:      "browser",
        // user_agent is added by main thread (not accessible inside Worker)
      }
    });
  }
};
