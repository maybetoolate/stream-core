# stream-core

Minimal stream library built **without Node's `stream` module internally**.

```text
Readable → Transform → Transform → Writable
```

## Who decides when the next chunk is produced?

The **consumer**. `Readable` only calls `_read()` when its internal
queue drops below `highWaterMark`. `push()` returns `false` when full.
See `docs/pull-vs-push.md`.

## Quick start

```js
const { Readable, Transform, Writable, pipeline } = require('./src');

const src = new Readable({
  highWaterMark: 16 * 1024,
  read() {
    for (let i = 0; i < 10 && this.push(Buffer.from('hi ')); i++) {}
    this.push(null); // EOF
  }
});

const upper = new Transform({
  transform(chunk, push, done) {
    push(Buffer.from(chunk.toString().toUpperCase()));
    done();
  }
});

const dst = new Writable({
  write(chunk, cb) { process.stdout.write(chunk); cb(); }
});

src.pipe(upper).pipe(dst);
```

## Real I/O (`io/`)

```js
const { FileReadable, FileWritable } = require('./io/file');
const { SocketReadable, SocketWritable } = require('./io/socket');
const { StdoutWritable } = require('./io/stdio');
const { Transform, pipeline } = require('./src');

// file -> transform -> file
await pipeline(
  new FileReadable('in.txt'),
  new Transform({ transform(c, push, done) { push(Buffer.from(c.toString().toUpperCase())); done(); } }),
  new FileWritable('out.txt')
);

// TCP socket -> parser -> socket
const { SocketReadable } = require('./io/socket');
for await (const chunk of new SocketReadable(socket)) {
  // parse chunk
}
```

`io/` never requires Node's `stream` module (`test/no-forbidden-imports.test.js`
enforces it). Sockets/stdin are only used as OS endpoints: `data` is
pushed into our queue, a full queue pauses the endpoint, `_read` resumes it.

## Instrument everything (`experiments/`)

Every class exposes `getStats()` (buffered bytes, queue depth, chunks/bytes,
pause/resume and drain/false counts). Run:

```text
npm run experiments
```

- **A** — producer 1 MB/s, consumer 10 MB/s: consumer starves, no backpressure.
- **B** — producer 100 MB/s, consumer 1 MB/s: pauses/resumes bound memory ~HWM.
- **C** — consumer randomly stalls ~500 ms: pauses track stalls, buffer bounded.
- **D** — consumer killed halfway: pipeline rejects, producer destroyed.

## Rules

- `src/` is dependency-free: only relative `require()`s (own `emitter.js`,
  no `events`, no `stream`). `io/` may use `fs`/`net` as OS endpoints but
  never `events`/`stream`. `test/no-forbidden-imports.test.js` enforces it.
- Never push directly to `main`. Open a PR from a feature branch.
- Small commits, one concept each.
