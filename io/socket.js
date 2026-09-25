'use strict';

// Stage 6 — TCP socket bridges. The net.Socket is used only as an OS
// endpoint (event emitter + write/end/pause/resume). No 'stream'
// module is required here.
const { Readable } = require('../src/readable');
const { Writable } = require('../src/writable');

/**
 * SocketReadable: demand-driven wrapper over a net.Socket.
 * Socket 'data' is pushed into our internal queue; when the queue is
 * full (push() === false) the socket pauses ("Stop. I'm full."), and
 * _read() resumes it when the consumer wants more.
 */
class SocketReadable extends Readable {
  constructor(socket, options = {}) {
    super({ highWaterMark: options.highWaterMark ?? 64 * 1024 });
    if (!socket || typeof socket.on !== 'function') {
      throw new Error('SocketReadable requires a net.Socket-like object');
    }
    this.socket = socket;
    this._destroySocket = options.destroySocket ?? true;
    this._socketPaused = false;

    this._onData = (chunk) => {
      const ok = this.push(chunk);
      if (ok === false && !this._socketPaused) {
        this._socketPaused = true;
        try {
          this.socket.pause();
        } catch (_) {}
      }
    };
    this._onEnd = () => this.push(null);
    this._onError = (err) => this.destroy(err);
    this._onClose = () => {
      if (!this._ended && !this._destroyed) this.push(null);
    };

    socket.on('data', this._onData);
    socket.on('end', this._onEnd);
    socket.once('error', this._onError);
    socket.once('close', this._onClose);
    try {
      socket.pause();
      this._socketPaused = true;
    } catch (_) {}
  }

  _read() {
    if (this._destroyed || this._ended) return;
    if (this._socketPaused) {
      this._socketPaused = false;
      try {
        this.socket.resume();
      } catch (err) {
        this.destroy(err);
      }
    }
  }

  _detach() {
    const s = this.socket;
    if (!s) return;
    try {
      s.removeListener('data', this._onData);
      s.removeListener('end', this._onEnd);
      s.removeListener('error', this._onError);
      s.removeListener('close', this._onClose);
    } catch (_) {}
  }

  destroy(err) {
    if (this._destroyed) {
      if (err) super.destroy(err);
      return this;
    }
    this._detach();
    if (this._destroySocket) {
      try {
        if (err) this.socket.destroy(err);
        else this.socket.destroy();
      } catch (_) {}
    } else if (!this._socketPaused) {
      try {
        this.socket.resume();
      } catch (_) {}
    }
    super.destroy(err);
    return this;
  }
}

/**
 * SocketWritable: bounded-buffer sink over a net.Socket.
 * Backpressure to the pipeline comes from our own queue (write()
 * === false when buffered >= highWaterMark); each _write flushes
 * one chunk via socket.write(chunk, cb).
 */
class SocketWritable extends Writable {
  constructor(socket, options = {}) {
    super({ highWaterMark: options.highWaterMark ?? 64 * 1024 });
    if (!socket || typeof socket.write !== 'function') {
      throw new Error('SocketWritable requires a net.Socket-like object');
    }
    this.socket = socket;
    this._destroySocket = options.destroySocket ?? true;
    this._onError = (err) => this.destroy(err);
    this._onClose = () => {
      if (!this._finished && !this._destroyed) {
        this.destroy(this._storedError || new Error('socket closed prematurely'));
      }
    };
    socket.once('error', this._onError);
    socket.once('close', this._onClose);
  }

  _write(chunk, cb) {
    const s = this.socket;
    if (this._destroyed) {
      cb(new Error('write after destroy'));
      return;
    }
    if (s.destroyed) {
      cb(new Error('write after socket destroy'));
      return;
    }
    let syncErr = null;
    let ret;
    try {
      ret = s.write(chunk, (err) => cb(err || undefined));
    } catch (err) {
      syncErr = err;
    }
    if (syncErr) cb(syncErr);
    else if (ret === false) {
      // Kernel buffer full: the write callback still fires on flush,
      // so no extra drain wait is needed for correctness.
    }
  }

  _final(cb) {
    const s = this.socket;
    if (s.destroyed) {
      cb();
      return;
    }
    try {
      s.end(() => cb());
    } catch (err) {
      cb(err);
    }
  }

  _detach() {
    try {
      this.socket.removeListener('error', this._onError);
      this.socket.removeListener('close', this._onClose);
    } catch (_) {}
  }

  destroy(err) {
    if (this._destroyed) return this;
    this._detach();
    if (this._destroySocket) {
      try {
        if (err) this.socket.destroy(err);
      } catch (_) {}
    }
    super.destroy(err);
    return this;
  }
}

module.exports = { SocketReadable, SocketWritable };
