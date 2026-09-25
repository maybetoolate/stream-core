'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

test("no require('stream') in src/ or io/ (comment lines ignored)", () => {
  const dirs = ['src', 'io'];
  const bad = [];
  for (const dir of dirs) {
    const root = path.join(__dirname, '..', dir);
    for (const f of fs.readdirSync(root)) {
      if (!f.endsWith('.js')) continue;
      const lines = fs.readFileSync(path.join(root, f), 'utf8').split('\n');
      lines.forEach((line, i) => {
        const code = line.replace(/\/\/.*$/, '');
        if (
          /require\(\s*['"](node:)?stream['"]/.test(code) ||
          /from\s+['"](node:)?stream['"]/.test(code)
        ) {
          bad.push(`${dir}/${f}:${i + 1}: ${line.trim()}`);
        }
      });
    }
  }
  assert.strictEqual(bad.join('\n'), '', `stream module used:\n${bad.join('\n')}`);
});
