'use strict';

// NOTE: never require('stream') here. EventEmitter only.
const { EventEmitter } = require('events');

function chunkSizeOf(chunk, objectMode) {
  if (objectMode) return 1;
  if (Buffer.isBuffer(chunk)) return chunk.length;
  if (typeof chunk === 'string') return Buffer.byteLength(chunk);
  if (chunk instanceof Uint8Array) return chunk.byteLength;
  if (chunk != null && typeof chunk.byteLength === 'number') return chunk.byteLength;
  return 1;
}

function normalizeChunk(chunk, objectMode) {
  if (chunk === null || chunk === undefined) return chunk;
  if (objectMode) return chunk;
  if (Buffer.isBuffer(chunk)) return chunk;
  if (typeof chunk === 'string') return Buffer.from(chunk);
  if (chunk instanceof Uint8Array) return Buffer.from(chunk);
  return chunk;
}

/**
 * Stage 1 — Readable.
 *
 * Answer to "Who decides when the next chunk is produced?":
 * The CONSUMER. _read() is only invoked when the internal queue
 * drops below highWaterMark (demand signal). push() returns false
 * when the queue is full (backpressure signal to the producer).
 */
class Readable extends EventEmitter {
  constructor(options = {}) {
    super();
    this.objectMode = !!options.objectMode;
    this.highWaterMark =
      options.highWaterMark ??
      (this.objectMode ? 16 : 16 * 1024);
    this._buf = [];
    this._bufLen = 0;
    this._ended = false; // push(null) seen
    this._endEmitted = false;
    this._flowing = false;
    this._reading = false;
    this._destroyed = false;
    this._storedError = null;
    this._pipes = new Set();

    this._stats = {
      chunksProduced: 0,
      bytesProduced: 0,
      chunksConsumed: 0,
      bytesConsumed: 0,
      pauseCount: 0,
      resumeCount: 0,
    };

    if (typeof options.read === 'function') {
      this._read = options.read.bind(this);
    }
    // Defer initial read until next tick so constructor can finish
    // and listeners can attach. Only fill if nobody is flowing yet?
    // We fill on demand: resume()/read()/pipe() trigger _maybeRead.
  }

  // Default: do nothing. Subclass or `read` option overrides.
  _read(_size) {}

  push(chunk) {
    if (this._destroyed) return false;
    if (chunk === null) {
      if (this._ended) {
        process.nextTick(() =>
          this.emit('error', new Error('push(null) after EOF'))
        );
        return false;
      }
      this._ended = true;
      if (this._buf.length === 0) {
        this._emitEndSoon();
      } else if (this._flowing) {
        this._flowSoon();
      } else {
        process.nextTick(() => this.emit('readable'));
      }
      return false;
    }
    if (this._ended) {
      process.nextTick(() =>
        this.emit('error', new Error('push() after EOF'))
      );
      return false;
    }
    const norm = normalizeChunk(chunk, this.objectMode);
    const size = chunkSizeOf(norm, this.objectMode);
    const wasEmpty = this._buf.length === 0;
    this._buf.push(norm);
    this._bufLen += size;
    this._stats.chunksProduced += 1;
    this._stats.bytesProduced += size;

    if (wasEmpty) process.nextTick(() => this.emit('readable'));
    if (this._flowing) this._flowSoon();
    return this._bufLen < this.highWaterMark;
  }

  /**
   * Manual pull. Returns one chunk or null if none available now.
   * Returning null does NOT mean EOF — check for 'end' event.
   */
  read() {
    if (this._destroyed) return null;
    if (this._buf.length === 0) {
      if (this._ended && !this._endEmitted) this._emitEndSoon();
      else if (!this._ended) this._maybeRead();
      return null;
    }
    const chunk = this._buf.shift();
    const size = chunkSizeOf(chunk, this.objectMode);
    this._bufLen -= size;
    if (this._bufLen < 0) this._bufLen = 0;
    this._stats.chunksConsumed += 1;
    this._stats.bytesConsumed += size;
    if (!this._ended && this._bufLen < this.highWaterMark) {
      this._maybeRead();
    }
    if (this._buf.length === 0 && this._ended && !this._endEmitted) {
      this._emitEndSoon();
    }
    return chunk;
  }

  pause() {
    if (this._flowing) {
      this._flowing = false;
      this._stats.pauseCount += 1;
      this.emit('pause');
    }
    return this;
  }

  resume() {
    if (this._destroyed) return this;
    if (!this._flowing) {
      this._flowing = true;
      this._stats.resumeCount += 1;
      this.emit('resume');
      this._flowSoon();
      this._maybeRead();
    }
    return this;
  }

  isPaused() {
    return !this._flowing;
  }

  // Node-compatible: adding a 'data' listener starts flowing.
  on(event, listener) {
    super.on(event, listener);
    if (event === 'data' && !this._flowing && !this._destroyed) this.resume();
    return this;
  }

  addListener(event, listener) {
    return this.on(event, listener);
  }

  prependListener(event, listener) {
    super.prependListener(event, listener);
    if (event === 'data' && !this._flowing && !this._destroyed) this.resume();
    return this;
  }

  // --- internal flow machinery ---

  _maybeRead() {
    if (this._destroyed || this._ended || this._reading) return;
    if (this._bufLen >= this.highWaterMark) return;
    queueMicrotask(() => this._callRead());
  }

  _callRead() {
    if (this._destroyed || this._ended || this._reading) return;
    if (this._bufLen >= this.highWaterMark) return;
    this._reading = true;
    let size = this.highWaterMark - this._bufLen;
    if (size <= 0) size = this.highWaterMark;
    try {
      const ret = this._read(size);
      if (ret && typeof ret.then === 'function') {
        ret.then(
          () => {
            this._reading = false;
            if (this._buf.length === 0 && !this._ended) {
              // _read resolved without pushing: avoid hot loop,
              // wait for next demand signal.
            }
            if (this._flowing) this._flowSoon();
          },
          (err) => {
            this._reading = false;
            this.destroy(err);
          }
        );
      } else {
        this._reading = false;
      }
    } catch (err) {
      this._reading = false;
      this.destroy(err);
    }
  }

  _flowSoon() {
    process.nextTick(() => this._flow());
  }

  _flow() {
    if (!this._flowing || this._destroyed) return;
    while (this._buf.length > 0 && this._flowing && !this._destroyed) {
      const chunk = this._buf.shift();
      const size = chunkSizeOf(chunk, this.objectMode);
      this._bufLen -= size;
      if (this._bufLen < 0) this._bufLen = 0;
      this._stats.chunksConsumed += 1;
      this._stats.bytesConsumed += size;
      this.emit('data', chunk);
    }
    if (this._destroyed) return;
    if (this._buf.length === 0 && this._ended) {
      this._emitEndSoon();
      return;
    }
    if (!this._ended) this._maybeRead();
  }

  _emitEndSoon() {
    if (this._endEmitted || this._destroyed) return;
    // Only emit end once queue fully drained.
    if (this._buf.length !== 0) return;
    this._endEmitted = true;
    process.nextTick(() => {
      this.emit('end');
      this.emit('close');
    });
  }

  // --- pipe (Stage 4, minimal version usable from Stage 1) ---

  pipe(dest, options = {}) {
    if (!dest || typeof dest.write !== 'function' || typeof dest.end !== 'function') {
      throw new Error('pipe() destination must implement write()/end()');
    }
    const doEnd = options.end !== false;
    this._pipes.add(dest);

    const onData = (chunk) => {
      let ok;
      try {
        ok = dest.write(chunk);
      } catch (err) {
        this.unpipe(dest);
        return;
      }
      if (ok === false) this.pause();
    };
    const onDrain = () => this.resume();
    const onEnd = () => {
      cleanup();
      if (doEnd) {
        try { dest.end(); } catch (_) {}
      }
    };
    const onSrcError = (err) => {
      cleanup();
      try { dest.destroy(err); } catch (_) {}
    };
    const onDestError = () => {
      cleanup();
      try { this.unpipe(dest); this.pause(); } catch (_) {}
    };
    const onDestClose = () => {
      cleanup();
    };
    const cleanup = () => {
      this.removeListener('data', onData);
      dest.removeListener('drain', onDrain);
      this.removeListener('end', onEnd);
      this.removeListener('error', onSrcError);
      dest.removeListener('error', onDestError);
      dest.removeListener('close', onDestClose);
      this._pipes.delete(dest);
    };
    dest._pipeCleanup = dest._pipeCleanup || new Map();
    dest._pipeCleanup.set(this, cleanup);

    this.on('data', onData);
    dest.on('drain', onDrain);
    this.once('end', onEnd);
    this.once('error', onSrcError);
    dest.once('error', onDestError);
    dest.once('close', onDestClose);

    this.resume();
    dest.emit('pipe', this);
    return dest;
  }

  unpipe(dest) {
    if (!dest) {
      for (const d of [...this._pipes]) this.unpipe(d);
      return this;
    }
    const cleanup = dest._pipeCleanup && dest._pipeCleanup.get(this);
    if (cleanup) cleanup();
    else this._pipes.delete(dest);
    dest.emit('unpipe', this);
    if (this._pipes.size === 0) this.pause();
    return this;
  }

  destroy(err) {
    if (this._destroyed) {
      if (err) process.nextTick(() => this.emit('error', err));
      return this;
    }
    this._destroyed = true;
    this._flowing = false;
    this._buf = [];
    this._bufLen = 0;
    if (err) this._storedError = err;
    if (err) process.nextTick(() => this.emit('error', err));
    process.nextTick(() => this.emit('close'));
    // Detach pipes
    for (const d of [...this._pipes]) {
      try { this.unpipe(d); } catch (_) {}
    }
    return this;
  }

  getStats() {
    return {
      bufferedBytes: this._bufLen,
      queueDepth: this._buf.length,
      chunksProduced: this._stats.chunksProduced,
      chunksConsumed: this._stats.chunksConsumed,
      bytesProduced: this._stats.bytesProduced,
      bytesConsumed: this._stats.bytesConsumed,
      pauseCount: this._stats.pauseCount,
      resumeCount: this._stats.resumeCount,
      highWaterMark: this.highWaterMark,
      flowing: this._flowing,
      ended: this._ended,
      destroyed: this._destroyed,
    };
  }

  // Stage 5 — async iteration (event-driven streams <-> for-await).
  // Pull-based: each next() calls read() (demand signal). Early exit
  // via return() removes pending listeners so no leak.
  [Symbol.asyncIterator]() {
    this.pause();
    const self = this;
    let finished = false;
    let cancelWait = null;
    const waitForData = () =>
      new Promise((resolve, reject) => {
        const onReadable = () => { cleanup(); resolve(); };
        const onEnd = () => { cleanup(); resolve(); };
        const onClose = () => { cleanup(); resolve(); };
        const onError = (e) => { cleanup(); reject(e); };
        const cleanup = () => {
          self.removeListener('readable', onReadable);
          self.removeListener('end', onEnd);
          self.removeListener('close', onClose);
          self.removeListener('error', onError);
          if (cancelWait === doCancel) cancelWait = null;
        };
        const doCancel = () => { cleanup(); resolve(); };
        cancelWait = doCancel;
        self.once('readable', onReadable);
        self.once('end', onEnd);
        self.once('close', onClose);
        self.once('error', onError);
      });
    const it = {
      async next() {
        while (true) {
          if (finished) return { value: undefined, done: true };
          const chunk = self.read();
          if (chunk !== null) return { value: chunk, done: false };
          if (self._destroyed) {
            finished = true;
            if (self._storedError) throw self._storedError;
            return { value: undefined, done: true };
          }
          if (self._ended && self._buf.length === 0) {
            finished = true;
            return { value: undefined, done: true };
          }
          try {
            await waitForData();
          } catch (e) {
            finished = true;
            throw e;
          }
        }
      },
      async return() {
        finished = true;
        if (cancelWait) cancelWait();
        return { value: undefined, done: true };
      },
      async throw(err) {
        finished = true;
        if (cancelWait) cancelWait();
        throw err;
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    };
    return it;
  }
}

module.exports = { Readable };
