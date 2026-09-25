'use strict';

// Stage 6 — stdin/stdout adapters. Sources/destinations are injectable
// (defaulting to process.stdin/process.stdout) so tests can pass fakes.
// No 'stream' module is required here.
const { Readable } = require('../src/readable');
const { Writable } = require('../src/writable');

/**
 * StdinReadable: pushes source 'data' into our queue; pauses the
 * source when full, resumes on _read (demand signal).
 */
class StdinReadable extends Readable {
  constructor(source, options = {}) {
    if (source && typeof source.on !== 'function') {
      options = source;
      source = undefined;
    }
    super({ highWaterMark: options.highWaterMark ?? 64 * 1024 });
    this.source = source || process.stdin;
    this._srcPaused = false;

    this._onData = (chunk) => {
      const ok = this.push(chunk);
      if (ok === false && !this._srcPaused) {
        this._srcPaused = true;
        try {
          this.source.pause();
        } catch (_) {}
      }
    };
    this._onEnd = () => this.push(null);
    this._onError = (err) => this.destroy(err);

    this.source.on('data', this._onData);
    this.source.on('end', this._onEnd);
    this.source.once('error', this._onError);
  }

  _read() {
    if (this._destroyed || this._ended) return;
    if (this._srcPaused) {
      this._srcPaused = false;
      try {
        this.source.resume();
      } catch (err) {
        this.destroy(err);
      }
    }
  }

  destroy(err) {
    if (!this._destroyed) {
      try {
        this.source.removeListener('data', this._onData);
        this.source.removeListener('end', this._onEnd);
        this.source.removeListener('error', this._onError);
      } catch (_) {}
    }
    super.destroy(err);
    return this;
  }
}

/**
 * StdoutWritable: bounded-buffer sink over a dest with write(chunk, cb).
 */
class StdoutWritable extends Writable {
  constructor(dest, options = {}) {
    if (dest && typeof dest.write !== 'function') {
      options = dest;
      dest = undefined;
    }
    super({ highWaterMark: options.highWaterMark ?? 64 * 1024 });
    this.dest = dest || process.stdout;
    this._onError =
      typeof this.dest.once === 'function'
        ? (err) => this.destroy(err)
        : null;
    if (this._onError) this.dest.once('error', this._onError);
  }

  _write(chunk, cb) {
    let ret;
    try {
      ret = this.dest.write(chunk, (err) => cb(err || undefined));
    } catch (err) {
      cb(err);
      return;
    }
    if (ret === false && typeof this.dest.once === 'function') {
      // Correctness already comes from our bounded queue + write cb;
      // kernel buffering is the dest's own concern.
    }
  }

  destroy(err) {
    if (!this._destroyed && this._onError && typeof this.dest.removeListener === 'function') {
      try {
        this.dest.removeListener('error', this._onError);
      } catch (_) {}
    }
    super.destroy(err);
    return this;
  }
}

module.exports = { StdinReadable, StdoutWritable };
