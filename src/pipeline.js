'use strict';

/**
 * Stage 4 — pipeline().
 *
 * A.pipe(B).pipe(C) looks trivial but must handle:
 * - A too fast -> B full -> A pauses -> B drains -> A resumes
 *   (pause/resume is implemented in Readable/Transform.pipe;
 *   pipeline() only ensures the chain is torn down correctly)
 * - A fails -> B, C react -> resources close (destroy all, no leaks)
 */
function pipeline(...args) {
  let cb = null;
  if (args.length > 0 && typeof args[args.length - 1] === 'function') {
    cb = args.pop();
  }
  const streams = args;
  if (streams.length < 2) {
    const err = new Error('pipeline requires at least 2 streams');
    if (cb) {
      process.nextTick(() => cb(err));
      return null;
    }
    return Promise.reject(err);
  }

  const promise = new Promise((resolve, reject) => {
    let finished = false;
    const last = streams[streams.length - 1];

    const done = (err) => {
      if (finished) return;
      finished = true;
      cleanup();
      if (err) {
        destroyAll(err);
        reject(err);
      } else {
        resolve();
      }
    };

    const destroyAll = (err) => {
      for (const s of streams) {
        try {
          if (s && typeof s.destroy === 'function' && !s._destroyed) {
            // Avoid re-emitting the original error from every stage.
            if (s._storedError === err) continue;
            // Secondary destroys emit 'error' on next tick; the pipeline's
            // own handlers are already cleaned up, so swallow them here.
            // The original error already reached the caller via done(err).
            if (typeof s.once === 'function') s.once('error', () => {});
            s.destroy(err);
          }
        } catch (_) {}
      }
    };

    const onError = (err) => done(err || new Error('pipeline failed'));

    const cleanups = [];
    // Wire errors first so no failure is missed.
    for (const s of streams) {
      if (s && typeof s.once === 'function') {
        s.once('error', onError);
        cleanups.push(() => {
          try { s.removeListener('error', onError); } catch (_) {}
        });
      }
    }

    const cleanup = () => {
      for (const fn of cleanups) fn();
      try { last.removeListener('finish', onLastDone); } catch (_) {}
      try { last.removeListener('end', onLastDone); } catch (_) {}
      try { last.removeListener('close', onLastCloseWithoutDone); } catch (_) {}
    };

    const onLastDone = () => done();
    // If the last stage closes without finish/end (e.g. destroy()),
    // treat as failure unless already done.
    const onLastCloseWithoutDone = () => {
      if (!finished) done(last._storedError || new Error('pipeline closed prematurely'));
    };

    if (typeof last.once === 'function') {
      // Writable last stage -> 'finish'; Readable/Transform tail -> 'end'.
      last.once('finish', onLastDone);
      last.once('end', onLastDone);
      last.once('close', onLastCloseWithoutDone);
    }

    try {
      // Chain with end propagation: src 'end' -> dest.end().
      let prev = streams[0];
      for (let i = 1; i < streams.length; i++) {
        prev = prev.pipe(streams[i]);
      }
    } catch (err) {
      done(err);
    }
  });

  if (cb) {
    promise.then(
      () => cb(),
      (err) => cb(err)
    );
    return undefined;
  }
  return promise;
}

module.exports = { pipeline };
