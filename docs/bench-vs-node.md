# Benchmark vs Node (Stage 8)

Harness: `bench/bench-vs-node.js` (`npm run bench`). Identical workloads on
both sides: 16 KB chunks, 64 KB watermarks, same consumer delays, same
process. Single-run numbers from 2026-09-25 (Node v22.22.1) — treat as
orders of magnitude, not constants.

## Results

Throughput — 32 MB through R→T→W, instant producer and consumer:

| impl | MB/s | heap Δ |
| ---- | ---- | ------ |
| ours | ~294 | 0.59 MB |
| node | ~1455 | 0.64 MB |

Backpressure — 4 MB, 2 ms/chunk consumer:

| impl | wall | peak heap | drains | pauses |
| ---- | ---- | --------- | ------ | ------ |
| ours | 919 ms | 5.8 MB | 64 | 256 |
| node | 1165 ms | 5.5 MB | 63 | n/a |

Latency — 1-chunk objectMode pipeline, 300 iterations:

| impl | mean | p50 |
| ---- | ---- | --- |
| ours | 0.49 ms | 0.32 ms |
| node | 0.49 ms | 0.38 ms |

## Reading

1. **Throughput is ~5× worse, memory behavior is equivalent.** Both bound
   memory to ~HWM and finish with the same heap delta. The backpressure
   *design* is validated; the per-chunk *cost* is not.
2. **Prime suspect: pause/resume churn.** Ours logged 2048 pauses for 2048
   chunks (throughput test) and 256 for 256 (backpressure test) — roughly
   one pause/resume cycle *per chunk*, each an emit plus a `nextTick` hop.
   `Transform.write()` returns false whenever either side hits HWM, and
   `pipe()` pauses immediately on every `false`. Node coalesces: it resumes
   at HWM crossings with batched state checks instead of per-chunk signals.
3. **Hop count.** Each chunk crosses several `nextTick`/`queueMicrotask`
   boundaries per stage (`_flowSoon`, `_maybeRead`, pump callbacks). Node
   moves more work synchronously inside a single tick.
4. **Latency is tied** because at one chunk the fixed setup cost dominates
   and both do equivalent work — consistent with a per-chunk overhead story.
5. **Methodology caveat that is also a finding:** under fully synchronous
   load, our `nextTick`-driven pipeline starved the harness's 10 ms sampler
   (peak read 0). A pipeline that never yields to the timers phase delays
   timers; keep this in mind for stall/heartbeat logic.

## Next (not done here)

- Coalesce pause/resume: pause on false→true *transitions* only, resume
  below a low-water mark instead of on every drain.
- Move more of `_flow`/`_pump` synchronously; measure `nextTick` vs
  `setImmediate` for drain paths.
- Re-run this harness after each change; it exists for exactly that.
