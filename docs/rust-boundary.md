# Rust escape hatch (Stage 9)

`rust/` is a N-API cdylib (`uppercase`, `checksum`, `heavy`) behind
`rust/index.js`, which keeps a byte-identical pure-JS fallback and loads the
native module only if it was built. Build: `sh rust/build.sh`
(`npm run build:rust`). The artifact (`rust/native.node`) is gitignored;
tests skip native cases when it is absent.

## Measured (this box, median of 3–5, warmup, `--expose-gc`)

Raw call, `checksum(64B)`: JS ~111 ns/call, Rust ~445 ns/call.
The complete Rust checksum call is ~330 ns/call slower on this box. That
covers checksum work *and* JS↔Rust boundary costs together — it does not
isolate the boundary alone, but it is the right guidance for small ops:
a few hundred nanoseconds of premium per call, every call.

Uppercase pipeline, instant sink (backend order alternated per sample):

| chunk | JS MB/s | Rust MB/s | ratio |
| ----- | ------- | --------- | ----- |
| 64 B | 11 | 5.3 | 0.48x |
| 4 KB | 118 | 121 | 1.03x |
| 64 KB | 296 | 356 | 1.20x |
| 1 MB | 500 | 842 | 1.68x |

Compute-bound kernel, `heavy(64KB)` FNV × rounds:

| rounds | JS | Rust | ratio |
| ------ | -- | ---- | ----- |
| 1 | 82 µs | 77 µs | 1.06x |
| 64 | 5.0 ms | 4.9 ms | 1.03x |

Caveat on the table: mid/large-chunk ratios move ±50% run to run on this
box (thermals, GC) — only the direction is claimed. Stable across all runs:
Rust loses at 64 B, the heavy kernel is parity, and the raw-call premium
holds.

## Reading

1. **The spec's prediction holds**: tiny ops through Rust are slower
   (0.36x at 64 B). The ~300 ns/call premium dominates small chunks.
2. **Stronger than predicted**: even a 64-round byte loop shows *no* Rust
   win. V8 ties `rustc -O3` on simple ALU loops, so the hatch pays only for
   kernels where Rust is *structurally* faster — not merely "a loop".
3. **Fix the framework first.** Stage 8 found ~5× per-chunk overhead
   (pause/resume churn). For cheap transforms that overhead dwarfs language
   choice; reaching for Rust before fixing it optimizes the wrong layer.
4. **Measure honestly or don't bother.** Unwarmed single runs on this box
   showed swings up to 20× (including one phantom 28× Rust win). Warmup,
   medians, interleaved GC were all required to get stable direction.

## Environment note

This box (proot Ubuntu on Termux) needed `apt-get install gcc libc6-dev`:
its default `cc` is a clang without `libgcc_s`, so no Rust crate links.
`rust/build.sh` tries a plain build, then retries with an explicit GNU gcc.
