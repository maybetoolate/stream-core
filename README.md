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

## Rules

- Never `require('stream')` inside `src/` or `io/`. CI enforces it.
- Never push directly to `main`. Open a PR from a feature branch.
- Small commits, one concept each.
