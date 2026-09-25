'use strict';

// Stage 7 — backpressure experiments. Run: npm run experiments
// Each experiment answers "what happens?" with measured numbers.
const { pipeline } = require('../src');
const { makePacedProducer, makePacedSink, startSampler, report } = require('./helpers');

const MB = 1024 * 1024;

async function expA() {
  // Producer 1 MB/s, consumer 10 MB/s: the consumer starves.
  const src = makePacedProducer({ bytesPerSec: 1 * MB, totalBytes: 2 * MB });
  const dst = makePacedSink({ bytesPerSec: 10 * MB });
  const { peaks, stop } = startSampler({ src, dst });
  const t = Date.now();
  await pipeline(src, dst);
  const elapsedMs = Date.now() - t;
  stop();
  report('A — slow producer (1 MB/s) vs fast consumer (10 MB/s)', {
    src, dst, peaks, elapsedMs,
    extra: { config: 'producer is the bottleneck' },
  });
  console.log('- lesson: consumer idles (dst buffered ~0, no backpressure); elapsed ~= total/1MB/s.');
}

async function expB() {
  // Producer 100 MB/s, consumer 1 MB/s: backpressure must bound memory.
  const src = makePacedProducer({ bytesPerSec: 100 * MB, totalBytes: 2 * MB });
  const dst = makePacedSink({ bytesPerSec: 1 * MB, highWaterMark: 64 * 1024 });
  const { peaks, stop } = startSampler({ src, dst });
  const t = Date.now();
  await pipeline(src, dst);
  const elapsedMs = Date.now() - t;
  stop();
  report('B — fast producer (100 MB/s) vs slow consumer (1 MB/s)', {
    src, dst, peaks, elapsedMs,
    extra: { config: 'backpressure must bound memory' },
  });
  console.log('- lesson: src pauses/resumes > 0 and peak buffer stays ~HWM; elapsed ~= total/1MB/s.');
}

async function expC() {
  // Consumer randomly stalls ~500ms: pauses must spike, buffer stay bounded.
  const src = makePacedProducer({ bytesPerSec: 20 * MB, totalBytes: 1 * MB });
  const dst = makePacedSink({
    bytesPerSec: 10 * MB,
    highWaterMark: 64 * 1024,
    stallEveryChunks: 8,
    stallMs: 500,
  });
  const { peaks, stop } = startSampler({ src, dst });
  const t = Date.now();
  await pipeline(src, dst);
  const elapsedMs = Date.now() - t;
  stop();
  report('C — consumer randomly stalls (~500ms every 8 chunks)', {
    src, dst, peaks, elapsedMs,
    extra: { config: 'bursty consumer' },
  });
  console.log('- lesson: throughput drops below 10 MB/s; pauses track stalls; buffer stays bounded.');
}

async function expD() {
  // Consumer dies halfway: the producer must react and everything closes.
  const total = 2 * MB;
  const src = makePacedProducer({ bytesPerSec: 20 * MB, totalBytes: total });
  const dst = makePacedSink({ bytesPerSec: 10 * MB, killAfterBytes: total / 2 });
  const t = Date.now();
  const err = await pipeline(src, dst).then(
    () => null,
    (e) => e
  );
  const elapsedMs = Date.now() - t;
  await new Promise((r) => setTimeout(r, 50));
  console.log('\n### D — consumer killed halfway');
  console.log(`- pipeline rejected: ${err ? err.message : '(no error!)'}`);
  console.log(`- elapsed: ${(elapsedMs / 1000).toFixed(2)}s`);
  console.log(`- bytes written before death: ${dst.totalWritten()} of ${total}`);
  console.log(`- src destroyed: ${src.getStats().destroyed}, dst destroyed: ${dst.getStats().destroyed}`);
  console.log('- lesson: kill propagates; producer is destroyed, not left pushing forever.');
  if (!err) process.exitCode = 1;
}

async function main() {
  console.log('stream-core backpressure experiments');
  await expA();
  await expB();
  await expC();
  await expD();
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}

module.exports = { expA, expB, expC, expD };
