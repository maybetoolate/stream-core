'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { EventEmitter } = require('events');
const { StdinReadable, StdoutWritable } = require('../io/stdio');
const { Readable, pipeline } = require('../src');

function fakeSource() {
  const e = new EventEmitter();
  e.paused = false;
  e.pause = () => {
    e.paused = true;
  };
  e.resume = () => {
    e.paused = false;
  };
  return e;
}

test('StdinReadable pushes source data with backpressure', async () => {
  const fake = fakeSource();
  const src = new StdinReadable(fake, { highWaterMark: 4 });
  for (let i = 0; i < 10; i++) fake.emit('data', Buffer.from('x'));
  assert.strictEqual(fake.paused, true);
  const first = src.read();
  assert.ok(first);
  src.destroy();
});

test('pipeline -> StdoutWritable captures writes', async () => {
  const written = [];
  const dest = {
    write(c, cb) {
      written.push(c.toString());
      cb();
    },
  };
  const out = new StdoutWritable(dest);
  const src = new Readable({
    objectMode: true,
    read() {
      this._n = (this._n || 0) + 1;
      if (this._n > 2) this.push(null);
      else this.push('x' + this._n);
    },
  });
  await pipeline(src, out);
  assert.deepStrictEqual(written, ['x1', 'x2']);
});

test('StdoutWritable error destroys', async () => {
  const dest = {
    write(_c, cb) {
      cb(new Error('stdout gone'));
    },
  };
  const out = new StdoutWritable(dest);
  out.on('error', () => {});
  out.write('hi');
  await new Promise((r) => setTimeout(r, 20));
  assert.ok(out._destroyed);
});
