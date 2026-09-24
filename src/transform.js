'use strict';
const { EventEmitter } = require('events');

function sizeOf(chunk, objectMode) {
  if (objectMode) return 1;
  if (Buffer.isBuffer(chunk)) return chunk.length;
  if (typeof chunk === 'string') return Buffer.byteLength(chunk);
  if (chunk instanceof Uint8Array) return chunk.byteLength;
  return 1;
}

function normalize(chunk, objectMode) {
  if (chunk === null || chunk === undefined) return chunk;
  if (objectMode) return chunk;
  if (Buffer.isBuffer(chunk)) return chunk;
  if (typeof chunk === 'string') return Buffer.from(chunk);
  if (chunk instanceof Uint8Array) return Buffer.from(chunk);
  return chunk;
}

/**
 * Stage 3 — Transform.
 *
 * Decisions:
 * - input accepted unless writable queue OR readable queue is full
 * - output produced only via push() inside _transform/_flush
 * - downstream slowness (readable full) pauses input processing
 *   AND applies backpressure upstream via write()===false
 * - errors destroy both sides; shutdown (end->flush->push(null))
 *   propagates downstream as 'end'.
 */
class Transform extends EventEmitter {
  constructor(options = {}) {
    super();
    this.objectMode = !!options.objectMode;
    // Allow separate watermarks; fall back to shared highWaterMark.
    this.readableHighWaterMark =
      options.readableHighWaterMark ??
      options.highWaterMark ??
      (this.objectMode ? 16 : 16 * 1024);
    this.writableHighWaterMark =
      options.writableHighWaterMark ??
      options.highWaterMark ??
      (this.objectMode ? 16 : 16 * 1024);

    // Readable side
    this._rbuf = [];
    this._rlen = 0;
    this._readEnded = false;
    this._endEmitted = false;
    this._flowing = false;
    // Writable side
    this._wqueue = []; // {chunk,size,cb}
    this._wBuffered = 0;
    this._wEnded = false;
    this._wFinished = false; // input fully consumed + flushed
    this._processing = false;
    this._flushCalled = false;
    this._destroyed = false;
    this._storedError = null;
    this._needDrain = false;
    this._pipes = new Set();
    this._endCb = null;

    this._stats = {
      chunksIn: 0, bytesIn: 0,
      chunksOut: 0, bytesOut: 0,
      pauseCount: 0, resumeCount: 0,
      drainCount: 0, falseCount: 0,
    };

    if (typeof options.transform === 'function') {
      this._transform = options.transform.bind(this);
    }
    if (typeof options.flush === 'function') {
      this._flush = options.flush.bind(this);
    }
  }

  // ---- user hooks ----
  _transform(chunk, push, done) {
    push(chunk);
    done();
  }

  _flush(push, done) {
    done();
  }

  // ---- readable side (source API) ----

  push(chunk) {
    if (this._destroyed) return false;
    if (chunk === null) {
      if (this._readEnded) {
        process.nextTick(() => this.emit('error', new Error('push(null) after EOF')));
        return false;
      }
      this._readEnded = true;
      if (this._rbuf.length === 0) this._emitEndSoon();
      else if (this._flowing) this._flowSoon();
      else process.nextTick(() => this.emit('readable'));
      return false;
    }
    if (this._readEnded) {
      process.nextTick(() => this.emit('error', new Error('push() after EOF')));
      return false;
    }
    const norm = normalize(chunk, this.objectMode);
    const size = sizeOf(norm, this.objectMode);
    const wasEmpty = this._rbuf.length === 0;
    this._rbuf.push(norm);
    this._rlen += size;
    this._stats.chunksOut += 1;
    this._stats.bytesOut += size;
    if (wasEmpty) process.nextTick(() => this.emit('readable'));
    if (this._flowing) this._flowSoon();
    return this._rlen < this.readableHighWaterMark;
  }

  read() {
    if (this._destroyed) return null;
    if (this._rbuf.length === 0) {
      if (this._readEnded && !this._endEmitted) this._emitEndSoon();
      else this._pumpTransform(); // demand: try to produce more
      return null;
    }
    const chunk = this._rbuf.shift();
    this._rlen -= sizeOf(chunk, this.objectMode);
    if (this._rlen < 0) this._rlen = 0;
    // Freed readable capacity: resume input processing + maybe drain upstream.
    this._pumpTransform();
    this._maybeEmitDrain();
    if (this._rbuf.length === 0 && this._readEnded && !this._endEmitted) {
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
      this._pumpTransform();
    }
    return this;
  }

  isPaused() {
    return !this._flowing;
  }

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

  _flowSoon() {
    process.nextTick(() => this._flow());
  }

  _flow() {
    if (!this._flowing || this._destroyed) return;
    while (this._rbuf.length > 0 && this._flowing && !this._destroyed) {
      const chunk = this._rbuf.shift();
      this._rlen -= sizeOf(chunk, this.objectMode);
      if (this._rlen < 0) this._rlen = 0;
      this.emit('data', chunk);
    }
    if (this._destroyed) return;
    if (this._rbuf.length === 0 && this._readEnded) {
      this._emitEndSoon();
      return;
    }
    // Demand more output.
    this._pumpTransform();
    this._maybeEmitDrain();
  }

  _emitEndSoon() {
    if (this._endEmitted || this._destroyed) return;
    if (this._rbuf.length !== 0) return;
    this._endEmitted = true;
    process.nextTick(() => {
      this.emit('end');
      this.emit('close');
    });
  }

  // ---- writable side (dest API) ----

  write(chunk, cb) {
    if (this._destroyed) {
      const err = new Error('write after destroy');
      if (cb) process.nextTick(() => cb(err));
      else process.nextTick(() => this.emit('error', err));
      return false;
    }
    if (this._wEnded) {
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
    const norm = normalize(chunk, this.objectMode);
    const size = sizeOf(norm, this.objectMode);
    this._wqueue.push({ chunk: norm, size, cb });
    this._wBuffered += size;
    this._stats.chunksIn += 1;
    this._stats.bytesIn += size;
    this._pumpTransform();
    if (this._wBuffered >= this.writableHighWaterMark ||
        this._rlen >= this.readableHighWaterMark) {
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
    if (chunk !== undefined && chunk !== null) this.write(chunk);
    if (this._wEnded) {
      if (cb) {
        if (this._wFinished && this._endEmitted) process.nextTick(() => cb());
        else this.once('end', cb);
      }
      return this;
    }
    this._wEnded = true;
    if (cb) this._endCb = cb;
    this._pumpTransform();
    return this;
  }

  // ---- core pump: serial input -> _transform -> readable buffer ----

  _readableFull() {
    return this._rlen >= this.readableHighWaterMark;
  }

  _pumpTransform() {
    if (this._processing || this._destroyed || this._wFinished) return;
    // Downstream slow: wait until readable side drains.
    if (this._readableFull()) return;
    const item = this._wqueue.shift();
    if (!item) {
      if (this._wEnded && !this._flushCalled) {
        this._flushCalled = true;
        this._processing = true;
        this._runFlush();
      } else {
        this._maybeEmitDrain();
      }
      return;
    }
    this._processing = true;
    const push = (out) => {
      if (out !== undefined && out !== null) this.push(out);
    };
    const done = (err) => {
      if (this._destroyed) return;
      this._processing = false;
      this._wBuffered -= item.size;
      if (this._wBuffered < 0) this._wBuffered = 0;
      if (err) {
        if (item.cb) item.cb(err);
        this.destroy(err);
        return;
      }
      if (item.cb) item.cb();
      this._maybeEmitDrain();
      this._pumpTransform();
    };
    try {
      const fn = this._transform;
      let ret;
      if (fn.length >= 3) {
        ret = fn(item.chunk, push, done);
      } else {
        ret = fn(item.chunk, push);
        if (ret && typeof ret.then === 'function') {
          ret.then(() => done(), done);
        } else if (ret !== undefined && fn.length < 3) {
          // Sync return value is treated as output (unless undefined).
          if (ret !== null) push(ret);
          done();
        } else if (fn.length < 3) {
          // (chunk, push) sync without return: assume sync done.
          // If user forgot done, we still advance to avoid stall,
          // but async callbacks via push still work for passthrough.
          // To support async without done, return a promise instead.
          done();
        }
      }
      if (ret && typeof ret.then === 'function' && fn.length >= 3) {
        // Callback style also returned a promise; ignore (done via callback).
      }
    } catch (err) {
      done(err);
    }
  }

  _runFlush() {
    const push = (out) => {
      if (out !== undefined && out !== null) this.push(out);
    };
    const done = (err) => {
      this._processing = false;
      if (err) {
        this.destroy(err);
        return;
      }
      this._wFinished = true;
      this.push(null); // propagate shutdown downstream
      this._maybeEmitDrain();
      if (this._endCb) {
        const f = this._endCb;
        this._endCb = null;
        this.once('end', f);
      }
      this.emit('finish');
    };
    try {
      const fn = this._flush;
      if (fn.length >= 2) {
        const ret = fn(push, done);
        if (ret && typeof ret.then === 'function') ret.then(() => done(), done);
      } else {
        const ret = fn(push);
        if (ret && typeof ret.then === 'function') ret.then(() => done(), done);
        else done();
      }
    } catch (err) {
      done(err);
    }
  }

  _maybeEmitDrain() {
    if (
      this._needDrain &&
      this._wBuffered < this.writableHighWaterMark &&
      !this._readableFull() &&
      !this._destroyed
    ) {
      this._needDrain = false;
      this.emit('drain');
    }
  }

  // ---- pipe (source API, shared with Readable) ----

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
      } catch (_) {
        this.unpipe(dest);
        return;
      }
      if (ok === false) this.pause();
    };
    const onDrain = () => {
      this.resume();
      this._pumpTransform();
    };
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
    const onDestClose = () => cleanup();
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
    if (this._destroyed) return this;
    this._destroyed = true;
    this._flowing = false;
    this._rbuf = [];
    this._rlen = 0;
    this._wqueue = [];
    this._wBuffered = 0;
    if (err) this._storedError = err;
    if (err) process.nextTick(() => this.emit('error', err));
    process.nextTick(() => this.emit('close'));
    for (const d of [...this._pipes]) {
      try { this.unpipe(d); } catch (_) {}
    }
    return this;
  }

  getStats() {
    return {
      readableBuffered: this._rlen,
      readableQueue: this._rbuf.length,
      writableBuffered: this._wBuffered,
      writableQueue: this._wqueue.length,
      chunksIn: this._stats.chunksIn,
      bytesIn: this._stats.bytesIn,
      chunksOut: this._stats.chunksOut,
      bytesOut: this._stats.bytesOut,
      pauseCount: this._stats.pauseCount,
      resumeCount: this._stats.resumeCount,
      drainCount: this._stats.drainCount,
      falseCount: this._stats.falseCount,
      flowing: this._flowing,
      inputEnded: this._wEnded,
      outputEnded: this._readEnded,
      destroyed: this._destroyed,
    };
  }

  // Stage 5 — async iteration over the readable side.
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
          if (self._readEnded && self._rbuf.length === 0) {
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

module.exports = { Transform };
