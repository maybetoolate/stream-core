'use strict';

// Stage 9 — Rust escape hatch with a byte-identical pure-JS fallback.
// The native module is optional: if it was never built (sh rust/build.sh),
// everything falls back to JS and tests skip the native cases.
const { Transform } = require('../src/transform');

function loadNative() {
  try {
    return require('./native.node');
  } catch (_) {
    return null;
  }
}

function jsUppercase(input) {
  const v = Buffer.from(input);
  for (let i = 0; i < v.length; i++) {
    const b = v[i];
    if (b >= 97 && b <= 122) v[i] = b - 32;
  }
  return v;
}

function jsChecksum(input) {
  const v = Buffer.isBuffer(input) ? input : Buffer.from(input);
  let h = 0x811c9dc5;
  for (let i = 0; i < v.length; i++) {
    h ^= v[i];
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function jsHeavy(input, rounds) {
  const v = Buffer.isBuffer(input) ? input : Buffer.from(input);
  let h = 0x811c9dc5;
  for (let r = 0; r < rounds; r++) {
    for (let i = 0; i < v.length; i++) {
      h ^= v[i];
      h = Math.imul(h, 0x01000193);
    }
  }
  return h >>> 0;
}

/**
 * Uppercase Transform. backend 'auto' (default) uses Rust when built,
 * 'rust' throws if unbuilt, 'js' always uses the fallback.
 */
function createUppercase({ backend = 'auto' } = {}) {
  const native = backend === 'js' ? null : loadNative();
  if (backend === 'rust' && !native) {
    throw new Error('rust backend requested but rust/native.node is not built (run sh rust/build.sh)');
  }
  const fn = native ? native.uppercase : jsUppercase;
  return new Transform({
    transform(chunk, push, done) {
      push(fn(chunk));
      done();
    },
  });
}

module.exports = { loadNative, jsUppercase, jsChecksum, jsHeavy, createUppercase };
