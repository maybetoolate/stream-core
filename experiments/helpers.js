'use strict';

// Stage 7 — experiment helpers. Everything is measured through the public
// API (getStats()), the same way a user would instrument a pipeline.
const { Readable, Writable } = require('../src');

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Producer capped at bytesPerSec (token bucket over wall-clock time).
 * _read is only invoked on demand, so the cap composes with backpressure:
 * a slow consumer still pauses this source via push() === false.
 */
function makePacedProducer({ chunkBytes = 16 * 1024, bytesPerSec, totalBytes, highWaterMark = 64 * 1024 }) {
  let produced = 0;
  const start = Date.now();
  return new Readable({
    highWaterMark,
    async read() {
      while (produced < totalBytes) {
        const elapsed = (Date.now() - start) / 1000;
        if (produced < bytesPerSec * elapsed) break;
        const deficit = produced + chunkBytes - bytesPerSec * elapsed;
        await sleep(Math.max(1, Math.min(50, (deficit / bytesPerSec) * 1000)));
      }
      if (produced >= totalBytes) {
        this.push(null);
        return;
      }
      const n = Math.min(chunkBytes, totalBytes - produced);
      produced += n;
      this.push(Buffer.alloc(n));
    },
  });
}

/**
 * Consumer that takes chunk.length/bytesPerSec per chunk, with optional
 * periodic random stalls and an optional kill switch at killAfterBytes.
 */
function makePacedSink({
  bytesPerSec,
  highWaterMark = 64 * 1024,
  stallEveryChunks = 0,
  stallMs = 0,
  killAfterBytes = Infinity,
}) {
  let written = 0;
  let chunks = 0;
  const sink = new Writable({
    highWaterMark,
    write(chunk, cb) {
      chunks += 1;
      const base = (chunk.length / bytesPerSec) * 1000;
      const stall = stallEveryChunks > 0 && chunks % stallEveryChunks === 0
        ? stallMs / 2 + Math.random() * stallMs // mean ~= stallMs
        : 0;
      setTimeout(() => {
        written += chunk.length;
        if (written >= killAfterBytes) cb(new Error('consumer killed halfway'));
        else cb();
      }, base + stall);
    },
  });
  sink.totalWritten = () => written;
  return sink;
}

/** Peak buffered bytes per stream, sampled on an interval. */
function startSampler(streams, intervalMs = 25) {
  const peaks = Object.fromEntries(Object.keys(streams).map((k) => [k, 0]));
  const timer = setInterval(() => {
    for (const [k, s] of Object.entries(streams)) {
      const st = s.getStats();
      const b = st.bufferedBytes ?? (st.readableBuffered || 0) + (st.writableBuffered || 0);
      if (b > peaks[k]) peaks[k] = b;
    }
  }, intervalMs);
  return { peaks, stop: () => clearInterval(timer) };
}

function bufferedOf(s) {
  const st = s.getStats();
  return st.bufferedBytes ?? (st.readableBuffered || 0) + (st.writableBuffered || 0);
}

function report(name, { src, dst, peaks, elapsedMs, extra = {} }) {
  const s = src.getStats();
  const d = dst.getStats();
  console.log(`\n### ${name}`);
  for (const [k, v] of Object.entries(extra)) console.log(`- ${k}: ${v}`);
  console.log(`- elapsed: ${(elapsedMs / 1000).toFixed(2)}s`);
  console.log(`- bytes in/out: ${s.bytesProduced} / ${d.bytesWritten}`);
  console.log(
    `- throughput: ${((d.bytesWritten / 1024 / 1024 / (elapsedMs / 1000)) || 0).toFixed(2)} MB/s`
  );
  console.log(`- src pauses/resumes: ${s.pauseCount}/${s.resumeCount}`);
  console.log(`- dst backpressure hits (write→false): ${d.falseCount}, drains: ${d.drainCount}`);
  console.log(`- peak buffered bytes (sampled): src=${peaks.src} dst=${peaks.dst}`);
}

module.exports = { sleep, makePacedProducer, makePacedSink, startSampler, bufferedOf, report };
