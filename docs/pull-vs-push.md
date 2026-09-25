# Pull vs push: who decides when the next chunk is produced?

Short answer: **the consumer**. `Readable` only calls `_read()` when its
internal queue drops below `highWaterMark` (demand signal). `push()` returns
`false` when the queue is full (backpressure signal to the producer).

## The design

```text
producer ──push()──▶ [ bounded queue ] ──read()/data──▶ consumer
                    ▲                    │
                    │   demand: queue    │
                    │   below HWM ──▶ _read()
                    │
              push() === false means "Stop. I'm full."
              'drain' / freed capacity means "resume."
```

Concretely (`src/readable.js`):

- `_maybeRead()` is invoked from `read()`, `_flow()`, and `resume()` — i.e.
  only when consumption freed capacity. It no-ops when the queue is at or
  above `highWaterMark`.
- `push()` appends and returns `this._bufLen < this.highWaterMark`.
- `pipe()` translates the two signals across stages: `dest.write()` false
  pauses the source, `drain` resumes it.

## Problems this design had to solve

1. **Unbounded growth.** Without the `false` signal, a 100 MB/s producer
   feeding a 10 MB/s consumer grows memory without limit (see
   `experiments/run-all.js`, experiment B: 32 pauses, peak buffer ~HWM).
2. **Stall on empty `_read`.** An async `_read` that resolves without
   pushing must *not* spin; it waits for the next demand signal.
3. **Teardown.** Error paths must destroy downstream stages *and* leave no
   dangling `error` listeners behind (`src/pipeline.js`, `destroy()`).
4. **Two consumption models.** `for-await` pulls via `read()` while `data`
   listeners flow — mixing them races, so iterators `pause()` on entry.

## Comparison against Node

Node's `Readable` answers the same question the same way: `read(n)` /
`_read(size)` is demand-driven, `push()` returns `false` past
`highWaterMark`, `'readable'` vs flowing mode mirrors our manual/`data`
split. Differences worth knowing:

- Node resumes (`drain`) as soon as the buffer drops *below* HWM; our
  `Writable` waits until the queue is *fully empty* (`src/writable.js`) —
  simpler, but lumpy (stop-and-go) throughput.
- Node's ` readableFlowing`, `readableLength`, `readableHighWaterMark`
  instrumentation is built in; ours is `getStats()` plus sampling.
- Node's pipeline teardown routes secondary errors to the callback; we
  attach swallow-handlers before secondary destroys for the same effect.
