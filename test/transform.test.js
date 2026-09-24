'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { Readable } = require('../src/readable');
const { Transform } = require('../src/transform');
const { Writable } = require('../src/writable');

function collect(writes) {
  return new Writable({
    objectMode: true,
    write(c, cb) { writes.push(c); cb(); },
  });
}

test('uppercase transform end-to-end', async () => {
  const src = new Readable({ objectMode: true });
  const upper = new Transform({
    objectMode: true,
    transform(chunk, push, done) {
      push(String(chunk).toUpperCase());
      done();
    },
  });
  const out = [];
  const dst = collect(out);
  src.push('a'); src.push('b'); src.push(null);
  src.pipe(upper).pipe(dst);
  await new Promise((res) => dst.once('finish', res));
  assert.deepStrictEqual(out, ['A', 'B']);
});

test('line splitter + chunk aggregator behavior', async () => {
  const splitter = new Transform({
    objectMode: true,
    transform(chunk, push, done) {
      this._rest = (this._rest || '') + String(chunk);
      const parts = this._rest.split('\n');
      this._rest = parts.pop();
      for (const p of parts) push(p);
      done();
    },
    flush(push, done) {
      if (this._rest) push(this._rest);
      done();
    },
  });
  const src = new Readable({ objectMode: true });
  const out = [];
  src.pipe(splitter).pipe(collect(out));
  src.push('a\nb\npar');
  src.push('tial\nc\n');
  src.push(null);
  await new Promise((res, rej) => {
    splitter.once('end', res);
    splitter.once('error', rej);
  });
  assert.deepStrictEqual(out, ['a', 'b', 'partial', 'c']);
});

test('downstream slow applies backpressure upstream through transform', async () => {
  const src = new Readable({ objectMode: true, highWaterMark: 4 });
  let srcReads = 0;
  src._read = function () {
    srcReads += 1;
    if (srcReads > 20) {
      this.push(null);
      return;
    }
    this.push(srcReads);
  };
  const t = new Transform({
    objectMode: true,
    readableHighWaterMark: 2,
    writableHighWaterMark: 2,
    transform(c, push, done) { push(c); done(); },
  });
  const dst = new Writable({
    objectMode: true,
    highWaterMark: 2,
    write(_c, cb) { setTimeout(cb, 10); },
  });
  src.pipe(t).pipe(dst);
  await new Promise((res) => dst.once('finish', res));
  const st = t.getStats();
  assert.ok(st.falseCount > 0 || st.pauseCount > 0, 'expected backpressure signals');
  assert.strictEqual(st.chunksIn, st.chunksOut);
});

test('errors propagate and shutdown propagates end', async () => {
  const bad = new Transform({
    objectMode: true,
    transform(_c, _push, done) { done(new Error('bad chunk')); },
  });
  const src = new Readable({ objectMode: true });
  const errs = [];
  bad.on('error', (e) => errs.push(e));
  src.push('x');
  const p = new Promise((res) => bad.once('error', res));
  src.pipe(bad);
  src.push(null);
  const err = await p;
  assert.match(err.message, /bad chunk/);

  // shutdown propagation
  const src2 = new Readable({ objectMode: true });
  const pass = new Transform({ objectMode: true });
  const seen2 = [];
  pass.on('data', (c) => seen2.push(c));
  const ended = new Promise((res) => pass.once('end', res));
  src2.pipe(pass);
  src2.push('y');
  src2.push(null);
  await ended;
});
