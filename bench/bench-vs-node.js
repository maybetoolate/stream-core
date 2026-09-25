'use strict';

// Stage 8 — benchmark ours vs Node's native streams on identical workloads.
// Run: npm run bench
// Methodology: same chunk size, same totals, same consumer delay, same
// process. Heap sampled during the run (no --expose-gc forcing).
const NodeStream = require('node:stream');
const { Readable, Transform, Writable, pipeline } = require('../src');

const CHUNK = 16 * 1024;

function heapMB() {
  return process.memoryUsage().heapUsed / 1024 / 1024;
}

function startPeakSampler() {
  let peak = 0;
  const timer = setInterval(() => {
    const h = heapMB();
    if (h > peak) peak = h;
  }, 10);
  return { stop: () => clearInterval(timer), peak: () => peak };
}

function makeOurs({ totalBytes, consumerDelayMs }) {
  let produced = 0;
  const buf = Buffer.alloc(CHUNK);
  const src = new Readable({
    highWaterMark: 64 * 1024,
    read() {
      let ok = true;
      while (ok && produced < totalBytes) {
        produced += CHUNK;
        ok = this.push(buf);
      }
      if (produced >= totalBytes) this.push(null);
    },
  });
  const mid = new Transform({
    transform(c, push, done) {
      push(c);
      done();
    },
  });
  let drains = 0;
  const dst = new Writable({
    highWaterMark: 64 * 1024,
    write(c, cb) {
      if (consumerDelayMs > 0) setTimeout(cb, consumerDelayMs);
      else cb();
    },
  });
  dst.on('drain', () => {
    drains += 1;
  });
  return { src, mid, dst, extra: () => ({ drains }) };
}

function makeNode({ totalBytes, consumerDelayMs }) {
  let produced = 0;
  const buf = Buffer.alloc(CHUNK);
  const src = new NodeStream.Readable({
    highWaterMark: 64 * 1024,
    read() {
      let ok = true;
      while (ok && produced < totalBytes) {
        produced += CHUNK;
        ok = this.push(buf);
      }
      if (produced >= totalBytes) this.push(null);
    },
  });
  const mid = new NodeStream.Transform({
    transform(c, _enc, done) {
      done(null, c);
    },
  });
  let drains = 0;
  const dst = new NodeStream.Writable({
    highWaterMark: 64 * 1024,
    write(c, _enc, cb) {
      if (consumerDelayMs > 0) setTimeout(cb, consumerDelayMs);
      else cb();
    },
  });
  dst.on('drain', () => {
    drains += 1;
  });
  return { src, mid, dst, extra: () => ({ drains }) };
}

async function throughput(label, make, totalBytes) {
  const { src, mid, dst, extra } = make({ totalBytes, consumerDelayMs: 0 });
  const run = src instanceof Readable
    ? pipeline(src, mid, dst)
    : NodeStream.promises.pipeline(src, mid, dst);
  const sampler = startPeakSampler();
  const base = heapMB();
  const t = Date.now();
  await run;
  const elapsedMs = Date.now() - t;
  sampler.stop();
  const mb = totalBytes / 1024 / 1024;
  // Note: peakHeapMB is omitted here. Under fully synchronous load our
  // nextTick/microtask-driven pipeline starves the sampler's 10ms timer,
  // so the peak reads 0 (same for Node at this speed). heapDeltaMB stands in.
  return {
    label,
    mbPerSec: mb / (elapsedMs / 1000),
    elapsedMs,
    heapDeltaMB: heapMB() - base,
    ...extra(),
    pauses: src.getStats ? src.getStats().pauseCount : 'n/a',
  };
}

async function backpressure(label, make, totalBytes) {
  const { src, mid, dst, extra } = make({ totalBytes, consumerDelayMs: 2 });
  const run = src instanceof Readable
    ? pipeline(src, mid, dst)
    : NodeStream.promises.pipeline(src, mid, dst);
  const sampler = startPeakSampler();
  const t = Date.now();
  await run;
  const elapsedMs = Date.now() - t;
  sampler.stop();
  return {
    label,
    elapsedMs,
    peakHeapMB: sampler.peak(),
    ...extra(),
    pauses: src.getStats ? src.getStats().pauseCount : 'n/a',
  };
}

async function latency(label, ours) {
  const times = [];
  const N = 300;
  const msg = Buffer.from('ping');
  if (ours) {
    for (let i = 0; i < N; i++) {
      const t = process.hrtime.bigint();
      await new Promise((resolve, reject) => {
        const src = new Readable({
          objectMode: true,
          read() {
            this.push(msg);
            this.push(null);
          },
        });
        const mid = new Transform({
          objectMode: true,
          transform(c, push, done) {
            push(c);
            done();
          },
        });
        const seen = [];
        const dst = new Writable({
          objectMode: true,
          write(c, cb) {
            seen.push(c);
            cb();
          },
        });
        pipeline(src, mid, dst).then(resolve, reject);
      });
      times.push(Number(process.hrtime.bigint() - t) / 1e6);
    }
  } else {
    for (let i = 0; i < N; i++) {
      const t = process.hrtime.bigint();
      await new Promise((resolve, reject) => {
        const src = NodeStream.Readable({
          objectMode: true,
          read() {
            this.push(msg);
            this.push(null);
          },
        });
        const mid = new NodeStream.Transform({
          objectMode: true,
          transform(c, _enc, done) {
            done(null, c);
          },
        });
        const dst = new NodeStream.Writable({
          objectMode: true,
          write(_c, _e, cb) {
            cb();
          },
        });
        NodeStream.promises.pipeline(src, mid, dst).then(resolve, reject);
      });
      times.push(Number(process.hrtime.bigint() - t) / 1e6);
    }
  }
  times.sort((a, b) => a - b);
  return { label, meanMs: times.reduce((a, b) => a + b, 0) / N, p50Ms: times[Math.floor(N / 2)] };
}

async function main() {
  console.log('throughput: 32 MB through R->T->W, instant producer/consumer');
  const tOurs = await throughput('ours', makeOurs, 32 * 1024 * 1024);
  const tNode = await throughput('node', makeNode, 32 * 1024 * 1024);
  console.table([tOurs, tNode]);

  console.log('backpressure: 4 MB through R->T->W, 2ms/chunk consumer');
  const bOurs = await backpressure('ours', makeOurs, 4 * 1024 * 1024);
  const bNode = await backpressure('node', makeNode, 4 * 1024 * 1024);
  console.table([bOurs, bNode]);

  console.log('latency: 1-chunk objectMode pipeline, 300 iterations');
  const lOurs = await latency('ours', true);
  const lNode = await latency('node', false);
  console.table([lOurs, lNode]);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}

module.exports = { throughput, backpressure, latency };
