'use strict';

// Stage 9 — where does the JS<->Rust boundary stop paying off?
// Run: npm run bench:rust  (requires sh rust/build.sh first)
// Compares byte-identical JS vs Rust uppercase Transforms at several chunk
// sizes, plus raw call-overhead and compute-kernel probes.
//
// Methodology: time-bounded warmup, median of interleaved samples with the
// backend order alternated per sample (order recorded in `order`), GC
// between samples when available (script runs node with --expose-gc).
// Single-box numbers — directional, not constants.
const { Readable, Writable, pipeline } = require('../src');
const { loadNative, jsUppercase, jsChecksum, jsHeavy } = require('../rust');

const gc = typeof global.gc === 'function' ? global.gc : () => {};
const TOTALS = { 64: 2 * 1024 * 1024, 4096: 8 * 1024 * 1024, 65536: 16 * 1024 * 1024, 1048576: 16 * 1024 * 1024 };

function median(xs) {
  const s = xs.slice().sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
}

/** Warmup bounded by wall time, so expensive kernels don't stall the bench. */
function warmupCalls(fn, arg, budgetMs = 500) {
  const t = Date.now();
  let n = 0;
  do {
    fn(arg);
    n += 1;
  } while (Date.now() - t < budgetMs && n < 100000);
}

async function runOnce(fn, chunkBytes, totalBytes) {
  let produced = 0;
  const buf = Buffer.alloc(chunkBytes, 'a');
  const src = new Readable({
    highWaterMark: 64 * 1024,
    read() {
      let ok = true;
      while (ok && produced < totalBytes) {
        produced += chunkBytes;
        ok = this.push(buf);
      }
      if (produced >= totalBytes) this.push(null);
    },
  });
  const { Transform } = require('../src');
  const mid = new Transform({
    transform(c, push, done) {
      push(fn(c));
      done();
    },
  });
  const dst = new Writable({
    highWaterMark: 64 * 1024,
    write(_c, cb) {
      cb();
    },
  });
  const t = Date.now();
  await pipeline(src, mid, dst);
  return totalBytes / 1024 / 1024 / ((Date.now() - t) / 1000);
}

/** Throughput cell: warmup both backends, then 3 interleaved samples. */
async function pipelineCell(chunkBytes, jsFn, rustFn) {
  const totalBytes = TOTALS[chunkBytes];
  const warm = Math.max(chunkBytes, totalBytes / 4);
  await runOnce(jsFn, chunkBytes, warm);
  await runOnce(rustFn, chunkBytes, warm);
  const js = [];
  const rust = [];
  const order = [];
  for (let i = 0; i < 3; i++) {
    const first = i % 2 === 0 ? 'js' : 'rust';
    order.push(first);
    gc();
    if (first === 'js') {
      js.push(await runOnce(jsFn, chunkBytes, totalBytes));
      gc();
      rust.push(await runOnce(rustFn, chunkBytes, totalBytes));
    } else {
      rust.push(await runOnce(rustFn, chunkBytes, totalBytes));
      gc();
      js.push(await runOnce(jsFn, chunkBytes, totalBytes));
    }
    gc();
  }
  return { js: median(js), rust: median(rust), order: order.join(',') };
}

function timeCalls(fn, arg, n) {
  const t = process.hrtime.bigint();
  for (let j = 0; j < n; j++) fn(arg);
  return Number(process.hrtime.bigint() - t) / n;
}

/** Raw call probe: warmup both, then 5 interleaved rounds. */
function rawCell(label, jsFn, rustFn, arg, n) {
  warmupCalls(jsFn, arg);
  warmupCalls(rustFn, arg);
  const js = [];
  const rust = [];
  const order = [];
  for (let r = 0; r < 5; r++) {
    const first = r % 2 === 0 ? 'js' : 'rust';
    order.push(first);
    if (first === 'js') {
      js.push(timeCalls(jsFn, arg, n));
      rust.push(timeCalls(rustFn, arg, n));
    } else {
      rust.push(timeCalls(rustFn, arg, n));
      js.push(timeCalls(jsFn, arg, n));
    }
  }
  return { label, js: median(js), rust: median(rust), order: order.join(',') };
}

async function main() {
  const native = loadNative();
  if (!native) {
    console.error('rust/native.node not built — run sh rust/build.sh first');
    process.exitCode = 1;
    return;
  }
  const tiny = Buffer.alloc(64, 'a');
  console.log('raw call overhead: checksum(64B) x 20000');
  console.table([rawCell('checksum', jsChecksum, native.checksum, tiny, 20000)]);

  console.log('pipeline throughput: uppercase, instant sink');
  const rows = [];
  for (const chunkBytes of [64, 4096, 65536, 1048576]) {
    const cell = await pipelineCell(chunkBytes, jsUppercase, native.uppercase);
    rows.push({ chunkBytes, jsMbPerSec: cell.js, rustMbPerSec: cell.rust, order: cell.order });
  }
  console.table(rows);

  console.log('compute-bound kernel: heavy(64KB) x rounds');
  const buf64k = Buffer.alloc(65536, 'a');
  const krows = [];
  for (const rounds of [1, 64]) {
    const cell = rawCell(
      `heavy x${rounds}`,
      (b) => jsHeavy(b, rounds),
      (b) => native.heavy(b, rounds),
      buf64k,
      20
    );
    krows.push({
      label: cell.label,
      jsNsPerCall: cell.js,
      rustNsPerCall: cell.rust,
      order: cell.order,
    });
  }
  console.table(krows);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
