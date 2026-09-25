'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { EventEmitter } = require('../src/emitter');

test('on/emit delivers args and returns true; no listener returns false', () => {
  const e = new EventEmitter();
  const got = [];
  e.on('data', (a, b) => got.push([a, b]));
  assert.strictEqual(e.emit('data', 1, 2), true);
  assert.strictEqual(e.emit('missing'), false);
  assert.deepStrictEqual(got, [[1, 2]]);
});

test('once fires once and is removable via the original listener', () => {
  const e = new EventEmitter();
  let n = 0;
  const fn = () => {
    n += 1;
  };
  e.once('x', fn);
  e.emit('x');
  e.emit('x');
  assert.strictEqual(n, 1);
  const e2 = new EventEmitter();
  let m = 0;
  const fn2 = () => {
    m += 1;
  };
  e2.once('x', fn2);
  e2.removeListener('x', fn2);
  e2.emit('x');
  assert.strictEqual(m, 0);
});

test('prependListener runs first', () => {
  const e = new EventEmitter();
  const order = [];
  e.on('x', () => order.push('late'));
  e.prependListener('x', () => order.push('early'));
  e.emit('x');
  assert.deepStrictEqual(order, ['early', 'late']);
});

test('removeListener/off/removeAllListeners/listenerCount', () => {
  const e = new EventEmitter();
  const a = () => {};
  const b = () => {};
  e.on('x', a);
  e.on('x', b);
  assert.strictEqual(e.listenerCount('x'), 2);
  e.removeListener('x', a);
  assert.strictEqual(e.listenerCount('x'), 1);
  e.off('x', b);
  assert.strictEqual(e.listenerCount('x'), 0);
  e.on('x', a);
  e.on('y', b);
  e.removeAllListeners('x');
  assert.strictEqual(e.listenerCount('x'), 0);
  assert.strictEqual(e.listenerCount('y'), 1);
  e.removeAllListeners();
  assert.strictEqual(e.listenerCount('y'), 0);
});

test('listeners() returns a copy', () => {
  const e = new EventEmitter();
  const a = () => {};
  e.on('x', a);
  const copy = e.listeners('x');
  assert.deepStrictEqual(copy, [a]);
  copy.push(() => {});
  assert.strictEqual(e.listenerCount('x'), 1);
});

test("emit('error') without listener throws; with listener does not", () => {
  const e = new EventEmitter();
  assert.throws(() => e.emit('error', new Error('boom')), /boom/);
  const e2 = new EventEmitter();
  let seen = null;
  e2.on('error', (err) => {
    seen = err;
  });
  assert.strictEqual(e2.emit('error', new Error('caught')), true);
  assert.match(String(seen && seen.message), /caught/);
});

test('emit iterates a snapshot (removal mid-emit is safe)', () => {
  const e = new EventEmitter();
  const order = [];
  const first = () => {
    order.push('first');
    e.removeListener('x', second);
  };
  const second = () => order.push('second');
  e.on('x', first);
  e.on('x', second);
  e.emit('x');
  assert.deepStrictEqual(order, ['first', 'second']);
  order.length = 0;
  e.emit('x');
  assert.deepStrictEqual(order, ['first']);
});

test('removeListener removes only the most recent duplicate (Node parity)', () => {
  const e = new EventEmitter();
  const fn = () => {};
  e.on('x', fn);
  e.on('x', fn);
  e.removeListener('x', fn);
  assert.strictEqual(e.listenerCount('x'), 1);
  e.removeListener('x', fn);
  assert.strictEqual(e.listenerCount('x'), 0);
});

test('listeners() returns the original callback for once() registrations', () => {
  const e = new EventEmitter();
  const fn = () => {};
  e.once('x', fn);
  assert.strictEqual(e.listeners('x')[0], fn);
});

test('methods chain and reject non-function listeners', () => {
  const e = new EventEmitter();
  assert.strictEqual(e.on('x', () => {}), e);
  assert.strictEqual(e.addListener('y', () => {}), e);
  assert.strictEqual(e.prependListener('z', () => {}), e);
  assert.throws(() => e.on('x', 'nope'), TypeError);
  assert.throws(() => e.once('x', null), TypeError);
});
