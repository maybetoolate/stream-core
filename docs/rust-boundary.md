# Rust escape hatch (Stage 9)

`rust/` is a N-API cdylib (`uppercase`, `checksum`, `heavy`) behind
`rust/index.js`, which keeps a byte-identical pure-JS fallback and loads the
native module only if it was built. Build: `sh rust/build.sh`
(`npm run build:rust`). The artifact (`rust/native.node`) is gitignored;
tests skip native cases when it is absent.

## Measured (this box, median of 3–5, warmup, `--expose-gc`)

Raw call, `checksum(64B)`: JS ~117 ns/call, Rust ~443 ns/call.
The boundary itself costs ~300 ns per call, every call, before any work.

Uppercase pipeline, 8–16 MB, instant sink:

| chunk | JS MB/s | Rust MB/s | ratio |
| ----- | ------- | --------- | ----- |
| 64 B | 16 | 5.8 | 0.36x |
| 4 KB | 138 | 82 | 0.59x |
| 64 KB | 400 | 281 | 0.70x |
| 1 MB | 552 | 571 | 1.04x |

Compute-bound kernel, `heavy(64KB)` FNV × rounds:

| rounds | JS | Rust | ratio |
| ------ | -- | ---- | ----- |
| 1 | 78 µs | 77 µs | 1.02x |
| 64 | 5.1 ms | 5.0 ms | 1.02x |

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
