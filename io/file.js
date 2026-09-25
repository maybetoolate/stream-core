'use strict';

// Stage 6 — filesystem source/sink. No 'stream' module required here,
// only fs + the pull-driven Readable/Writable core.
const fs = require('fs');
const { Readable } = require('../src/readable');
const { Writable } = require('../src/writable');

/**
 * FileReadable: demand-driven file source.
 * The consumer decides when the next chunk is produced: _read(size)
 * is only invoked when the internal queue drops below highWaterMark.
 */
class FileReadable extends Readable {
  constructor(path, options = {}) {
    super({ highWaterMark: options.highWaterMark ?? 64 * 1024 });
    this.path = path;
    this._handle = null;
    this._pos = 0;
    this._opening = null;
  }

  async _open() {
    if (this._handle) return this._handle;
    if (!this._opening) {
      this._opening = fs.promises.open(this.path, 'r').then(
        (h) => {
          this._handle = h;
          this._opening = null;
          return h;
        },
        (err) => {
          this._opening = null;
          throw err;
        }
      );
    }
    return this._opening;
  }

  async _read(size) {
    let handle;
    try {
      handle = await this._open();
    } catch (err) {
      this.destroy(err);
      return;
    }
    const buf = Buffer.alloc(Math.max(1, size || this.highWaterMark));
    let res;
    try {
      res = await handle.read(buf, 0, buf.length, this._pos);
    } catch (err) {
      this.destroy(err);
      return;
    }
    if (res.bytesRead === 0) {
      await this._closeQuiet();
      this.push(null);
      return;
    }
    this._pos += res.bytesRead;
    this.push(buf.subarray(0, res.bytesRead));
  }

  async _closeQuiet() {
    const h = this._handle;
    this._handle = null;
    if (h) {
      try {
        await h.close();
      } catch (_) {}
    }
  }

  destroy(err) {
    if (this._destroyed) {
      if (err) super.destroy(err);
      return this;
    }
    super.destroy(err);
    this._closeQuiet();
    return this;
  }
}

/**
 * FileWritable: bounded-buffer file sink.
 * write() returns false when buffered >= highWaterMark ("Stop. I'm full."),
 * 'drain' fires when capacity frees up.
 */
class FileWritable extends Writable {
  constructor(path, options = {}) {
    super({ highWaterMark: options.highWaterMark ?? 64 * 1024 });
    this.path = path;
    this._flags = options.flags ?? 'w';
    this._mode = options.mode;
    this._handle = null;
    this._opening = null;
  }

  async _open() {
    if (this._handle) return this._handle;
    if (!this._opening) {
      const opts = { flags: this._flags };
      if (this._mode !== undefined) opts.mode = this._mode;
      this._opening = fs.promises.open(this.path, opts.flags, opts.mode).then(
        (h) => {
          this._handle = h;
          this._opening = null;
          return h;
        },
        (err) => {
          this._opening = null;
          throw err;
        }
      );
    }
    return this._opening;
  }

  _write(chunk, cb) {
    this._open().then(
      (handle) =>
        handle.write(chunk).then(
          () => cb(),
          (err) => cb(err)
        ),
      (err) => cb(err)
    );
  }

  _final(cb) {
    const h = this._handle;
    this._handle = null;
    if (!h) {
      // Never opened (empty input): still create/truncate the file.
      this._open().then(
        (handle) => {
          this._handle = null;
          handle.close().then(
            () => cb(),
            () => cb()
          );
        },
        (err) => cb(err)
      );
      return;
    }
    h.close().then(
      () => cb(),
      (err) => cb(err)
    );
  }

  destroy(err) {
    const h = this._handle;
    this._handle = null;
    if (h) h.close().catch(() => {});
    super.destroy(err);
    return this;
  }
}

module.exports = { FileReadable, FileWritable };
