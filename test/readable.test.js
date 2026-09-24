'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { Readable } = require('../src/readable');

test('pull: _read only called on demand (consumer decides)', async () => {
  let calls = 0;
  const r = new Readable({
    highWaterMark: 3,
    objectMode: true,
    read() {
      calls += 1;
      if (calls <= 2) {
        this.push({ n: calls });
        this.push({ n: calls + 10 });
      } else {
        this.push(null);
      }
    },
  });
  assert.strictEqual(calls, 0);
  r.resume();
  r.pause();
  await new Promise((res) => setTimeout(res, 20));
  assert.ok(calls >= 1, 'demand triggers _read');
  const s = r.getStats();
  assert.ok(s.chunksProduced >= 1);
});

test('push returns false when full (backpressure signal)', () => {
  const r = new Readable({ highWaterMark: 2, objectMode: true });
  assert.strictEqual(r.push({ a: 1 }), true);
  assert.strictEqual(r.push({ a: 2 }), false); // at HWM
});

test('manual read() consumes queue and emits end after null', async () => {
  const r = new Readable({ objectMode: true });
  r.push({ x: 1 });
  r.push(null);
  assert.deepStrictEqual(r.read(), { x: 1 });
  assert.strictEqual(r.read(), null);
  await new Promise((res) => r.once('end', res));
});

test('flowing mode delivers data events and end', async () => {
  const r = new Readable({ objectMode: true });
  const got = [];
  r.push(1); r.push(2); r.push(null);
  r.on('data', (c) => got.push(c));
  await new Promise((res) => r.once('end', res));
  assert.deepStrictEqual(got, [1, 2]);
});

test('destroy(err) emits error and close', async () => {
  const r = new Readable({ objectMode: true });
  const err = new Error('boom');
  const seen = {};
  r.on('error', (e) => { seen.error = e; });
  r.on('close', () => { seen.close = true; });
  r.destroy(err);
  await new Promise((res) => setTimeout(res, 10));
  assert.strictEqual(seen.error, err);
  assert.strictEqual(seen.close, true);
});
