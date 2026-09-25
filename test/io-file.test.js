'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { FileReadable, FileWritable } = require('../io/file');
const { Transform, pipeline } = require('../src');

function tmp(name) {
  return path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'stream-core-')), name);
}

test('file -> transform -> file roundtrip', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'stream-core-'));
  const inp = path.join(dir, 'in.txt');
  const outp = path.join(dir, 'out.txt');
  fs.writeFileSync(inp, 'hello file io\nsecond line\n');
  const src = new FileReadable(inp, { highWaterMark: 8 });
  const up = new Transform({
    transform(c, push, done) {
      push(Buffer.from(c.toString().toUpperCase()));
      done();
    },
  });
  await pipeline(src, up, new FileWritable(outp));
  assert.strictEqual(fs.readFileSync(outp, 'utf8'), 'HELLO FILE IO\nSECOND LINE\n');
});

test('for-await over FileReadable', async () => {
  const p = tmp('a.txt');
  fs.writeFileSync(p, 'abc123');
  const r = new FileReadable(p, { highWaterMark: 2 });
  let s = '';
  for await (const c of r) s += c.toString();
  assert.strictEqual(s, 'abc123');
});

test('missing file destroys with error', async () => {
  const r = new FileReadable('/tmp/opencode/does-not-exist-stream-core.txt');
  r.on('error', () => {});
  await assert.rejects(
    (async () => {
      for await (const c of r) { /* drain */ }
    })(),
    /ENOENT/
  );
});
