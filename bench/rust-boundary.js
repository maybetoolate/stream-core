'use strict';

// Stage 9 — where does the JS<->Rust boundary stop paying off?
// Run: npm run bench:rust  (requires sh rust/build.sh first)
// Compares byte-identical JS vs Rust uppercase Transforms at several chunk
// sizes, plus a raw call-overhead probe with a minimal Rust->JS payload.
const { Readable, Writable, pipeline } = require('../src');
const { loadNative, jsUppercase, jsChecksum } = require('../rust');

async function pipelineThroughput(label, fn, chunkBytes, totalBytes) {
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
  const s = (Date.now() - t) / 1000;
  return { label, chunkBytes, mbPerSec: totalBytes / 1024 / 1024 / s };
}

function rawCallOverhead(label, fn, arg, n) {
  const t = process.hrtime.bigint();
  for (let i = 0; i < n; i++) fn(arg);
  const ns = Number(process.hrtime.bigint() - t);
  return { label, nsPerCall: ns / n };
}

async function main() {
  const native = loadNative();
  if (!native) {
    console.error('rust/native.node not built — run sh rust/build.sh first');
    process.exitCode = 1;
    return;
  }
  const tiny = Buffer.alloc(64, 'a');
  console.log('raw call overhead: checksum(64B) x 100000');
  console.table([
    rawCallOverhead('js', jsChecksum, tiny, 100000),
    rawCallOverhead('rust', native.checksum, tiny, 100000),
  ]);

  console.log('pipeline throughput: 8 MB uppercase, instant sink');
  const rows = [];
  for (const chunkBytes of [64, 4096, 65536, 1048576]) {
    const js = await pipelineThroughput('js', jsUppercase, chunkBytes, 8 * 1024 * 1024);
    const rust = await pipelineThroughput('rust', native.uppercase, chunkBytes, 8 * 1024 * 1024);
    rows.push({ ...js, vs: '' });
    rows.push({ ...rust, vs: (rust.mbPerSec / js.mbPerSec).toFixed(2) + 'x' });
  }
  console.table(rows);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
