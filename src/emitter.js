'use strict';

/**
 * Minimal EventEmitter. Zero dependencies — no `require()` calls at all.
 *
 * Implements the subset of Node's `events` module that stream-core uses:
 * on / addListener / prependListener / once / removeListener / off /
 * removeAllListeners / listeners / listenerCount / emit.
 *
 * Semantics match Node where it matters here:
 * - emit() iterates a snapshot, so listeners added/removed during emit
 *   don't affect the in-progress emission (pipe() unpipes mid-'data').
 * - once() stores wrapper.listener = original, so
 *   removeListener(event, original) removes a once-wrapped listener
 *   (async iteration cleanup relies on this).
 * - emit('error', err) with no 'error' listener throws err, like Node.
 *
 * Deliberately omitted: newListener/removeListener events,
 * max-listeners warnings, captureRejections, async iteration helpers.
 */
class EventEmitter {
  constructor() {
    this._events = Object.create(null);
  }

  _add(event, listener, prepend) {
    if (typeof listener !== 'function') {
      throw new TypeError('listener must be a function');
    }
    const list = this._events[event];
    if (list === undefined) {
      this._events[event] = [listener];
    } else if (prepend) {
      list.unshift(listener);
    } else {
      list.push(listener);
    }
    return this;
  }

  on(event, listener) {
    return this._add(event, listener, false);
  }

  addListener(event, listener) {
    return this.on(event, listener);
  }

  prependListener(event, listener) {
    return this._add(event, listener, true);
  }

  once(event, listener) {
    if (typeof listener !== 'function') {
      throw new TypeError('listener must be a function');
    }
    const self = this;
    function wrapper(...args) {
      self.removeListener(event, wrapper);
      listener.apply(self, args);
    }
    wrapper.listener = listener;
    return this.on(event, wrapper);
  }

  removeListener(event, listener) {
    const list = this._events[event];
    if (list !== undefined) {
      const kept = list.filter(
        (fn) => fn !== listener && fn.listener !== listener
      );
      if (kept.length === 0) {
        delete this._events[event];
      } else {
        this._events[event] = kept;
      }
    }
    return this;
  }

  off(event, listener) {
    return this.removeListener(event, listener);
  }

  removeAllListeners(event) {
    if (event === undefined) {
      this._events = Object.create(null);
    } else {
      delete this._events[event];
    }
    return this;
  }

  listeners(event) {
    const list = this._events[event];
    return list === undefined ? [] : list.slice();
  }

  listenerCount(event) {
    const list = this._events[event];
    return list === undefined ? 0 : list.length;
  }

  emit(event, ...args) {
    const list = this._events[event];
    if (list === undefined || list.length === 0) {
      if (event === 'error') {
        const err = args[0];
        if (err instanceof Error) throw err;
        throw new Error(
          'Unhandled error event: ' + (err === undefined ? '(no message)' : String(err))
        );
      }
      return false;
    }
    for (const fn of list.slice()) {
      fn.apply(this, args);
    }
    return true;
  }
}

module.exports = { EventEmitter };
