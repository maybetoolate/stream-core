'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { Readable, Transform, Writable, pipeline } = require('../src');

test('for-await over Readable collects all chunks then ends', async () => {
  const src = new Readable({
    objectMode: true,
    read() {
      this._n = (this._n || 0) + 1;
      if (this._n > 5) this.push(null);
      else this.push(this._n);
    },
  });
  const out = [];
  for await (const c of src) out.push(c);
  assert.deepStrictEqual(out, [1, 2, 3, 4, 5]);
});

test('for-await pulls on demand (consumer decides)', async () => {
  let calls = 0;
  const src = new Readable({
    objectMode: true,
    highWaterMark: 2,
    read() {
      calls += 1;
      this._n = (this._n || 0) + 1;
      if (this._n > 4) this.push(null);
      else this.push(this._n);
    },
  });
  const out = [];
  for await (const c of src) {
    out.push(c);
    await new Promise((r) => setTimeout(r, 5));
  }
  assert.deepStrictEqual(out, [1, 2, 3, 4]);
  assert.ok(calls >= 1 && calls <= 6, `expected demand-driven reads, got ${calls}`);
});

test('for-await over Transform output side', async () => {
  const t = new Transform({
    objectMode: true,
    transform(c, push, done) { push(c * 10); done(); },
  });
  t.write(1);
  t.write(2);
  t.end();
  const out = [];
  for await (const c of t) out.push(c);
  assert.deepStrictEqual(out, [10, 20]);
});

test('for-await over pipe tail (Readable -> Transform chain)', async () => {
  const src = new Readable({
    objectMode: true,
    read() {
      this._n = (this._n || 0) + 1;
      if (this._n > 3) this.push(null);
      else this.push(this._n);
    },
  });
  const dbl = new Transform({
    objectMode: true,
    transform(c, push, done) { push(c * 2); done(); },
  });
  src.pipe(dbl);
  const out = [];
  for await (const c of dbl) out.push(c);
  assert.deepStrictEqual(out, [2, 4, 6]);
});

test('for-await throws on stream error', async () => {
  const bad = new Readable({
    objectMode: true,
    read() {
      this._n = (this._n || 0) + 1;
      if (this._n === 2) this.destroy(new Error('iter boom'));
      else this.push(this._n);
    },
  });
  bad.on('error', () => {}); // avoid unhandled; iterator still throws
  await assert.rejects(
    (async () => {
      for await (const c of bad) { /* consume */ }
    })(),
    /iter boom/
  );
});

test('early break cleans up pending listeners (no leak)', async () => {
  const src = new Readable({ objectMode: true, read() {} });
  const it = src[Symbol.asyncIterator]();
  const pending = it.next();
  await new Promise((r) => setTimeout(r, 10));
  assert.strictEqual(src.listenerCount('readable'), 1);
  await it.return();
  const res = await pending;
  assert.strictEqual(res.done, true);
  await new Promise((r) => setTimeout(r, 10));
  assert.strictEqual(src.listenerCount('readable'), 0);
  assert.strictEqual(src.listenerCount('end'), 0);
  src.destroy();
});

test('break out of for-await leaves no listeners', async () => {
  const src = new Readable({
    objectMode: true,
    read() {
      this._n = (this._n || 0) + 1;
      if (this._n > 100) this.push(null);
      else this.push(this._n);
    },
  });
  for await (const c of src) {
    assert.strictEqual(c, 1);
    break;
  }
  await new Promise((r) => setTimeout(r, 20));
  assert.strictEqual(src.listenerCount('readable'), 0);
  src.destroy();
});
