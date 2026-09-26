'use strict';

// Stage 9 — where does the JS<->Rust boundary stop paying off?
// Run: npm run bench:rust  (requires sh rust/build.sh first)
// Compares byte-identical JS vs Rust uppercase Transforms at several chunk
// sizes, plus a raw call-overhead probe with a minimal Rust->JS payload.
//
// Methodology: warmup, then median of 3 alternating runs per cell with a
// GC between cells when available (run node with --expose-gc). Single-box
// numbers — directional, not constants.
const { Readable, Writable, pipeline } = require('../src');
const { loadNative, jsUppercase, jsChecksum, jsHeavy } = require('../rust');

const gc = typeof global.gc === 'function' ? global.gc : () => {};
const TOTALS = { 64: 2 * 1024 * 1024, 4096: 8 * 1024 * 1024, 65536: 16 * 1024 * 1024, 1048576: 16 * 1024 * 1024 };

function median(xs) {
  const s = xs.slice().sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
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

async function pipelineThroughput(label, fn, chunkBytes) {
  const totalBytes = TOTALS[chunkBytes];
  await runOnce(fn, chunkBytes, Math.max(chunkBytes, totalBytes / 4)); // warmup
  const samples = [];
  for (let i = 0; i < 3; i++) {
    gc();
    samples.push(await runOnce(fn, chunkBytes, totalBytes));
    gc();
  }
  return { label, chunkBytes, mbPerSec: median(samples) };
}

function rawCallOverhead(label, fn, arg, n) {
  for (let i = 0; i < 10000; i++) fn(arg); // warmup
  const samples = [];
  for (let i = 0; i < 5; i++) {
    const t = process.hrtime.bigint();
    for (let j = 0; j < n; j++) fn(arg);
    samples.push(Number(process.hrtime.bigint() - t) / n);
  }
  return { label, nsPerCall: median(samples) };
}

async function main() {
  const native = loadNative();
  if (!native) {
    console.error('rust/native.node not built — run sh rust/build.sh first');
    process.exitCode = 1;
    return;
  }
  const tiny = Buffer.alloc(64, 'a');
  console.log('raw call overhead: checksum(64B) x 20000, median of 5');
  console.table([
    rawCallOverhead('js', jsChecksum, tiny, 20000),
    rawCallOverhead('rust', native.checksum, tiny, 20000),
  ]);

  console.log('pipeline throughput: uppercase, instant sink, median of 3');
  const rows = [];
  for (const chunkBytes of [64, 4096, 65536, 1048576]) {
    const js = await pipelineThroughput('js', jsUppercase, chunkBytes);
    const rust = await pipelineThroughput('rust', native.uppercase, chunkBytes);
    rows.push({ ...js, vs: '' });
    rows.push({ ...rust, vs: (rust.mbPerSec / js.mbPerSec).toFixed(2) + 'x' });
  }
  console.table(rows);

  console.log('compute-bound kernel: heavy(64KB) x rounds, median of 5');
  const buf64k = Buffer.alloc(65536, 'a');
  const krows = [];
  for (const rounds of [1, 64]) {
    const js = rawCallOverhead(`js x${rounds}`, (b) => jsHeavy(b, rounds), buf64k, 20);
    const rust = rawCallOverhead(`rust x${rounds}`, (b) => native.heavy(b, rounds), buf64k, 20);
    krows.push({ ...js, vs: '' });
    krows.push({ ...rust, vs: (js.nsPerCall / rust.nsPerCall).toFixed(2) + 'x' });
  }
  console.table(krows);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
