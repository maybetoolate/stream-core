'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const net = require('net');
const { SocketReadable, SocketWritable } = require('../io/socket');
const { Readable, pipeline } = require('../src');

test('SocketReadable collects server bytes via for-await', async () => {
  const server = net.createServer((sock) => sock.end('hello socket'));
  await new Promise((r) => server.listen(0, r));
  const sock = net.connect(server.address().port);
  const reader = new SocketReadable(sock);
  let s = '';
  for await (const c of reader) s += c.toString();
  assert.strictEqual(s, 'hello socket');
  server.close();
});

test('pipeline -> SocketWritable delivers all bytes', async () => {
  const received = [];
  const server = net.createServer((sock) => {
    sock.on('data', (c) => received.push(c.toString()));
  });
  await new Promise((r) => server.listen(0, r));
  const sock = net.connect(server.address().port);
  await new Promise((r) => sock.on('connect', r));
  const writer = new SocketWritable(sock);
  const src = new Readable({
    objectMode: true,
    read() {
      this._n = (this._n || 0) + 1;
      if (this._n > 3) this.push(null);
      else this.push('msg' + this._n);
    },
  });
  await pipeline(src, writer);
  await new Promise((r) => setTimeout(r, 50));
  assert.strictEqual(received.join(''), 'msg1msg2msg3');
  server.close();
});

test('socket error destroys the reader', async () => {
  const server = net.createServer(() => {});
  await new Promise((r) => server.listen(0, r));
  const sock = net.connect(server.address().port);
  await new Promise((r) => sock.on('connect', r));
  const reader = new SocketReadable(sock);
  reader.on('error', () => {});
  sock.destroy(new Error('conn reset'));
  await new Promise((r) => setTimeout(r, 50));
  assert.ok(reader._destroyed);
  server.close();
});
