'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { Readable, Writable, Transform, pipeline } = require('../src');

test('A too fast -> B full -> A pauses -> B drains -> A resumes', async () => {
  const src = new Readable({
    objectMode: true,
    highWaterMark: 4,
    read() {
      let ok = true;
      while (ok) {
        this._n = (this._n || 0) + 1;
        if (this._n > 30) {
          this.push(null);
          break;
        }
        ok = this.push(this._n);
      }
    },
  });
  const slow = new Transform({
    objectMode: true,
    readableHighWaterMark: 2,
    writableHighWaterMark: 2,
    transform(c, push, done) {
      setTimeout(() => { push(c); done(); }, 2);
    },
  });
  const out = [];
  const dst = new Writable({
    objectMode: true,
    highWaterMark: 2,
    write(c, cb) { out.push(c); setTimeout(cb, 2); },
  });

  await pipeline(src, slow, dst);
  assert.strictEqual(out.length, 30);
  assert.deepStrictEqual(out, Array.from({ length: 30 }, (_, i) => i + 1));
  const ss = src.getStats();
  const ts = slow.getStats();
  assert.ok(ss.pauseCount > 0 || ss.resumeCount > 0 || ts.pauseCount > 0,
    `expected pause/resume, got src=${JSON.stringify(ss)} t=${JSON.stringify(ts)}`);
});

test('chained .pipe() returns dest for A.pipe(B).pipe(C)', async () => {
  const src = new Readable({ objectMode: true });
  const a = new Transform({ objectMode: true, transform(c, p, d) { p(c + 1); d(); } });
  const b = new Transform({ objectMode: true, transform(c, p, d) { p(c * 10); d(); } });
  const out = [];
  const dst = new Writable({ objectMode: true, write(c, cb) { out.push(c); cb(); } });
  const ret = src.pipe(a).pipe(b).pipe(dst);
  assert.strictEqual(ret, dst);
  src.push(1); src.push(2); src.push(null);
  await new Promise((res) => dst.once('finish', res));
  assert.deepStrictEqual(out, [20, 30]);
});

test('A fails -> B, C react, resources close', async () => {
  const src = new Readable({
    objectMode: true,
    read() {
      this._n = (this._n || 0) + 1;
      if (this._n === 3) {
        this.destroy(new Error('source boom'));
        return;
      }
      this.push(this._n);
    },
  });
  const mid = new Transform({ objectMode: true });
  const out = [];
  const dst = new Writable({ objectMode: true, write(c, cb) { out.push(c); cb(); } });

  const errors = [];
  for (const s of [src, mid, dst]) s.on('error', (e) => errors.push(e));

  await assert.rejects(pipeline(src, mid, dst), /source boom/);
  await new Promise((res) => setTimeout(res, 20));
  assert.ok(errors.length >= 1, 'errors surfaced');
  assert.ok(mid._destroyed || mid.getStats().destroyed, 'mid closed');
});

test('consumer error with no user listeners still rejects (no uncaught throw)', async () => {
  const src = new Readable({
    objectMode: true,
    read() {
      this._n = (this._n || 0) + 1;
      if (this._n > 100) this.push(null);
      else this.push(this._n);
    },
  });
  const dst = new Writable({
    objectMode: true,
    write(c, cb) {
      if (c === 3) cb(new Error('consumer died'));
      else cb();
    },
  });
  await assert.rejects(pipeline(src, dst), /consumer died/);
  await new Promise((res) => setTimeout(res, 30));
  assert.ok(src._destroyed, 'producer destroyed, not left pushing');
});

test('pipe source error destroys listener-less dest without uncaught throw', async () => {
  const src = new Readable({
    objectMode: true,
    read() {
      this.destroy(new Error('src boom'));
    },
  });
  src.on('error', () => {}); // observe source; dest stays listener-less
  const dst = new Writable({ objectMode: true, write(c, cb) { cb(); } });
  src.pipe(dst);
  await new Promise((res) => setTimeout(res, 30));
  assert.ok(dst._destroyed);
});
