'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

function stripped(root, file) {
  // Strip // comments on the whole source so multiline expressions
  // (e.g. require(\n'node:events'\n)) are still matched as one unit.
  return fs
    .readFileSync(path.join(root, file), 'utf8')
    .replace(/\/\/.*$/gm, '');
}

function lineOf(src, index) {
  return src.slice(0, index).split('\n').length;
}

test('src/ is dependency-free: only relative require() calls', () => {
  const root = path.join(__dirname, '..', 'src');
  const bad = [];
  for (const f of fs.readdirSync(root)) {
    if (!f.endsWith('.js')) continue;
    const src = stripped(root, f);
    // Allowed: require('./x') / require('../x') — our own modules.
    // Forbidden: bare specifiers (events, stream, fs, ...) and npm packages.
    for (const m of src.matchAll(/require\(\s*['"]([^'"]+)['"]/g)) {
      if (!m[1].startsWith('.')) bad.push(`src/${f}:${lineOf(src, m.index)}: ${m[0]}`);
    }
  }
  assert.strictEqual(bad.join('\n'), '', `non-relative require() in src/:\n${bad.join('\n')}`);
});

test("io/ never uses the events/stream abstractions (skipped if io/ absent)", () => {
  const root = path.join(__dirname, '..', 'io');
  if (!fs.existsSync(root)) return;
  const bad = [];
  for (const f of fs.readdirSync(root)) {
    if (!f.endsWith('.js')) continue;
    const src = stripped(root, f);
    for (const m of src.matchAll(
      /require\(\s*['"](node:)?(events|stream)['"]|from\s+['"](node:)?(events|stream)['"]/g
    )) {
      bad.push(`io/${f}:${lineOf(src, m.index)}: ${m[0]}`);
    }
  }
  assert.strictEqual(bad.join('\n'), '', `abstraction used in io/:\n${bad.join('\n')}`);
});
