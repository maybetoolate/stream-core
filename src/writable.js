'use strict';
const { EventEmitter } = require('events');

function chunkSizeOf(chunk, objectMode) {
  if (objectMode) return 1;
  if (Buffer.isBuffer(chunk)) return chunk.length;
  if (typeof chunk === 'string') return Buffer.byteLength(chunk);
  if (chunk instanceof Uint8Array) return chunk.byteLength;
  return 1;
}

function normalizeChunk(chunk, objectMode) {
  if (objectMode) return chunk;
  if (Buffer.isBuffer(chunk)) return chunk;
  if (typeof chunk === 'string') return Buffer.from(chunk);
  if (chunk instanceof Uint8Array) return Buffer.from(chunk);
  return chunk;
}

/**
 * Stage 2 — Writable.
 *
 * Bounded internal buffer + async writes. write() returns false
 * when buffered >= highWaterMark ("Stop. I'm full."). 'drain' is
 * emitted when the buffer drops back below the mark ("resume").
 */
class Writable extends EventEmitter {
  constructor(options = {}) {
    super();
    this.objectMode = !!options.objectMode;
    this.highWaterMark =
      options.highWaterMark ?? (this.objectMode ? 16 : 16 * 1024);
    this._queue = []; // [{chunk, size, cb}]
    this._buffered = 0;
    this._writing = false;
    this._ended = false; // end() called
    this._finished = false;
    this._destroyed = false;
    this._needDrain = false;
    this._endCb = null;
    this._storedError = null;

    this._stats = {
      chunksWritten: 0,
      bytesWritten: 0,
      drainCount: 0,
      falseCount: 0,
    };

    if (typeof options.write === 'function') {
      this._write = options.write.bind(this);
    }
    if (typeof options.final === 'function') {
      this._final = options.final.bind(this);
    }
  }

  _write(_chunk, cb) {
    cb();
  }

  _final(cb) {
    cb();
  }

  write(chunk, cb) {
    if (this._destroyed) {
      const err = new Error('write after destroy');
      if (cb) process.nextTick(() => cb(err));
      else process.nextTick(() => this.emit('error', err));
      return false;
    }
    if (this._ended) {
      const err = new Error('write after end');
      if (cb) process.nextTick(() => cb(err));
      else process.nextTick(() => this.emit('error', err));
      return false;
    }
    if (chunk === null || chunk === undefined) {
      const err = new Error('chunk must not be null/undefined (use end())');
      if (cb) process.nextTick(() => cb(err));
      else process.nextTick(() => this.emit('error', err));
      return false;
    }
    const norm = normalizeChunk(chunk, this.objectMode);
    const size = chunkSizeOf(norm, this.objectMode);
    this._queue.push({ chunk: norm, size, cb });
    this._buffered += size;
    this._pump();
    if (this._buffered >= this.highWaterMark) {
      this._needDrain = true;
      this._stats.falseCount += 1;
      return false;
    }
    return true;
  }

  end(chunk, cb) {
    if (typeof chunk === 'function') {
      cb = chunk;
      chunk = undefined;
    }
    if (chunk !== undefined && chunk !== null) {
      this.write(chunk);
    }
    if (this._ended) {
      if (cb) {
        if (this._finished) process.nextTick(() => cb());
        else this.once('finish', cb);
      }
      return this;
    }
    this._ended = true;
    if (cb) this._endCb = cb;
    this._pump();
    return this;
  }

  _pump() {
    if (this._writing || this._destroyed) return;
    const item = this._queue.shift();
    if (!item) {
      if (this._needDrain && this._buffered < this.highWaterMark) {
        this._needDrain = false;
        this._stats.drainCount += 1;
        process.nextTick(() => this.emit('drain'));
      }
      if (this._ended && !this._finished) {
        this._writing = true;
        this._final((err) => {
          this._writing = false;
          if (err) {
            this.destroy(err);
            if (this._endCb) {
              const f = this._endCb;
              this._endCb = null;
              f(err);
            }
            return;
          }
          this._finished = true;
          process.nextTick(() => {
            this.emit('finish');
            this.emit('close');
          });
          if (this._endCb) {
            const f = this._endCb;
            this._endCb = null;
            process.nextTick(() => f());
          }
        });
      }
      return;
    }
    this._writing = true;
    let called = false;
    const done = (err) => {
      if (called) return;
      called = true;
      this._writing = false;
      if (err) {
        this._buffered -= item.size;
        if (this._buffered < 0) this._buffered = 0;
        if (item.cb) item.cb(err);
        this.destroy(err);
        return;
      }
      this._buffered -= item.size;
      if (this._buffered < 0) this._buffered = 0;
      this._stats.chunksWritten += 1;
      this._stats.bytesWritten += item.size;
      if (item.cb) item.cb();
      // Drain check before next chunk so producer resumes promptly.
      if (this._needDrain && this._buffered < this.highWaterMark && this._queue.length === 0) {
        this._needDrain = false;
        this._stats.drainCount += 1;
        this.emit('drain');
      }
      this._pump();
    };
    try {
      const ret = this._write(item.chunk, done);
      if (ret && typeof ret.then === 'function') {
        ret.then(() => done(), done);
      }
    } catch (err) {
      done(err);
    }
  }

  destroy(err) {
    if (this._destroyed) return this;
    this._destroyed = true;
    this._queue = [];
    this._buffered = 0;
    if (err) this._storedError = err;
    if (err) process.nextTick(() => this.emit('error', err));
    process.nextTick(() => this.emit('close'));
    return this;
  }

  getStats() {
    return {
      bufferedBytes: this._buffered,
      queueDepth: this._queue.length,
      chunksWritten: this._stats.chunksWritten,
      bytesWritten: this._stats.bytesWritten,
      drainCount: this._stats.drainCount,
      falseCount: this._stats.falseCount,
      highWaterMark: this.highWaterMark,
      writing: this._writing,
      ended: this._ended,
      finished: this._finished,
      destroyed: this._destroyed,
    };
  }
}

module.exports = { Writable };
