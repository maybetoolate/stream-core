'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { Writable } = require('../src/writable');

test('write returns false when full, drain when emptied', async () => {
  const seen = [];
  const w = new Writable({
    highWaterMark: 2,
    objectMode: true,
    write(chunk, cb) {
      setTimeout(() => { seen.push(chunk); cb(); }, 5);
    },
  });
  assert.strictEqual(w.write(1), true);
  assert.strictEqual(w.write(2), false); // buffered hits HWM
  const drained = new Promise((res) => w.once('drain', res));
  await drained;
  assert.deepStrictEqual(seen, [1, 2]);
});

test('slow consumer: bounded buffer does not grow indefinitely', async () => {
  const w = new Writable({
    highWaterMark: 4,
    objectMode: true,
    write(_c, cb) { setTimeout(cb, 2); },
  });
  let falses = 0;
  for (let i = 0; i < 50; i++) {
    if (!w.write(i)) {
      falses += 1;
      await new Promise((res) => w.once('drain', res));
    }
  }
  w.end();
  await new Promise((res) => w.once('finish', res));
  assert.ok(falses > 0, 'backpressure engaged');
  assert.strictEqual(w.getStats().chunksWritten, 50);
});

test('end() flushes then emits finish', async () => {
  const got = [];
  const w = new Writable({ objectMode: true, write(c, cb) { got.push(c); cb(); } });
  w.write('a');
  w.end('b');
  await new Promise((res) => w.once('finish', res));
  assert.deepStrictEqual(got, ['a', 'b']);
});

test('write error destroys and emits error', async () => {
  const w = new Writable({
    objectMode: true,
    write(_c, cb) { cb(new Error('disk full')); },
  });
  const err = await new Promise((res) => {
    w.once('error', res);
    w.write('x');
  });
  assert.match(err.message, /disk full/);
});

test('write after end emits error', async () => {
  const w = new Writable({ objectMode: true, write(_c, cb) { cb(); } });
  w.end();
  await new Promise((res) => w.once('finish', res));
  const err = await new Promise((res) => {
    w.once('error', res);
    w.write('late');
  });
  assert.match(err.message, /after end/);
});
