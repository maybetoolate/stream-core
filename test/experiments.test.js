'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { pipeline } = require('../src');
const { makePacedProducer, makePacedSink } = require('../experiments/helpers');

test('paced producer delivers exact bytes through a fast sink', async () => {
  const total = 128 * 1024;
  const src = makePacedProducer({ bytesPerSec: 32 * 1024 * 1024, totalBytes: total });
  const dst = makePacedSink({ bytesPerSec: 32 * 1024 * 1024 });
  await pipeline(src, dst);
  assert.strictEqual(dst.totalWritten(), total);
});

test('kill switch rejects the pipeline and destroys the producer', async () => {
  const total = 256 * 1024;
  const src = makePacedProducer({ bytesPerSec: 32 * 1024 * 1024, totalBytes: total });
  const dst = makePacedSink({ bytesPerSec: 32 * 1024 * 1024, killAfterBytes: total / 2 });
  await assert.rejects(pipeline(src, dst), /consumer killed halfway/);
  await new Promise((r) => setTimeout(r, 30));
  assert.ok(src.getStats().destroyed, 'producer destroyed after consumer death');
  assert.ok(dst.totalWritten() < total, 'stopped short of total');
});
