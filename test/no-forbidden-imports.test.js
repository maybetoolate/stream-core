'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

function codeLines(root, file) {
  return fs
    .readFileSync(path.join(root, file), 'utf8')
    .split('\n')
    .map((line) => line.replace(/\/\/.*$/, ''));
}

test('src/ is dependency-free: only relative require() calls', () => {
  const root = path.join(__dirname, '..', 'src');
  const bad = [];
  for (const f of fs.readdirSync(root)) {
    if (!f.endsWith('.js')) continue;
    codeLines(root, f).forEach((code, i) => {
      // Allowed: require('./x') / require('../x') — our own modules.
      // Forbidden: bare specifiers (events, stream, fs, ...) and npm packages.
      const m = code.match(/require\(\s*['"]([^'"]+)['"]/);
      if (m && !m[1].startsWith('.')) bad.push(`src/${f}:${i + 1}: ${code.trim()}`);
    });
  }
  assert.strictEqual(bad.join('\n'), '', `non-relative require() in src/:\n${bad.join('\n')}`);
});

test("io/ never uses the events/stream abstractions (skipped if io/ absent)", () => {
  const root = path.join(__dirname, '..', 'io');
  if (!fs.existsSync(root)) return;
  const bad = [];
  for (const f of fs.readdirSync(root)) {
    if (!f.endsWith('.js')) continue;
    codeLines(root, f).forEach((code, i) => {
      if (
        /require\(\s*['"](node:)?(events|stream)['"]/.test(code) ||
        /from\s+['"](node:)?(events|stream)['"]/.test(code)
      ) {
        bad.push(`io/${f}:${i + 1}: ${code.trim()}`);
      }
    });
  }
  assert.strictEqual(bad.join('\n'), '', `abstraction used in io/:\n${bad.join('\n')}`);
});
