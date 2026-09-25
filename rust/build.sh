#!/bin/sh
# Build the N-API cdylib and place it where rust/index.js loads it.
# Plain `cargo build` first (works wherever cc can link Rust); on toolchains
# whose cc lacks libgcc_s (e.g. Termux clang), retry with an explicit GNU gcc.
set -e
cd "$(dirname "$0")"
if cargo build --release 2>/dev/null; then
  :
else
  echo "plain build failed, retrying with CC/CARGO linker override..." >&2
  CC_aarch64_unknown_linux_gnu=/usr/bin/gcc \
  CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/usr/bin/gcc \
    cargo build --release
fi
SO=$(ls target/release/libstreamcore.so target/release/streamcore.* 2>/dev/null | head -n 1)
cp "$SO" ./native.node
echo "wrote rust/native.node"
