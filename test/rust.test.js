'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { loadNative, jsUppercase, jsChecksum, jsHeavy, createUppercase } = require('../rust');
const { Readable, Writable, pipeline } = require('../src');

const native = loadNative();
// Native cases skip gracefully when the module was never built.
const needsNative = (name, fn) =>
  test(name, native ? {} : { skip: 'rust/native.node not built (sh rust/build.sh)' }, fn);

test('js fallback uppercases ascii only', () => {
  assert.ok(jsUppercase(Buffer.from('aBcZ09é')).equals(Buffer.from('ABCZ09é')));
  assert.strictEqual(jsUppercase(Buffer.alloc(0)).length, 0);
});

needsNative('native uppercase/checksum/heavy match js on binary input', () => {
  const input = Buffer.from(Array.from({ length: 256 }, (_, i) => i));
  assert.ok(native.uppercase(input).equals(jsUppercase(input)));
  assert.strictEqual(native.checksum(input), jsChecksum(input));
  for (const rounds of [1, 64]) {
    assert.strictEqual(native.heavy(input, rounds), jsHeavy(input, rounds));
  }
});

test('pipeline through js uppercase backend', async () => {
  const src = new Readable({ read() { this.push(Buffer.from('hello')); this.push(null); } });
  const out = [];
  const dst = new Writable({ write(c, cb) { out.push(c.toString()); cb(); } });
  await pipeline(src, createUppercase({ backend: 'js' }), dst);
  assert.deepStrictEqual(out, ['HELLO']);
});

needsNative('pipeline through rust uppercase backend', async () => {
  const src = new Readable({ read() { this.push(Buffer.from('hello')); this.push(null); } });
  const out = [];
  const dst = new Writable({ write(c, cb) { out.push(c.toString()); cb(); } });
  await pipeline(src, createUppercase({ backend: 'rust' }), dst);
  assert.deepStrictEqual(out, ['HELLO']);
});

test('rust backend throws helpfully when unbuilt', () => {
  if (native) return; // covered by the backend test above
  assert.throws(() => createUppercase({ backend: 'rust' }), /not built/);
});
