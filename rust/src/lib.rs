//! Stage 9 — CPU escape hatch.
//!
//! The hottest per-chunk work in a byte pipeline is the Transform function.
//! This crate exposes that work over N-API so the pipeline can call into
//! Rust per chunk. The JS side (`rust/index.js`) keeps a byte-identical pure
//! JS fallback, which is what makes the boundary-cost comparison honest.
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

/// Uppercase ASCII a-z in place-copy. Returns a new Buffer.
#[napi]
pub fn uppercase(input: Buffer) -> Buffer {
  let mut v = input.to_vec();
  for b in v.iter_mut() {
    if b.is_ascii_lowercase() {
      *b -= 32;
    }
  }
  v.into()
}

/// FNV-1a 32-bit fold. Tiny Rust->JS payload: isolates call overhead.
#[napi]
pub fn checksum(input: Buffer) -> u32 {
  let mut h: u32 = 0x811c_9dc5;
  for b in input.as_ref() {
    h ^= *b as u32;
    h = h.wrapping_mul(0x0100_0193);
  }
  h
}

/// Heavy kernel stand-in: FNV over the buffer `rounds` times.
/// Models parse/compress-class work where per-byte cost dominates.
#[napi]
pub fn heavy(input: Buffer, rounds: u32) -> u32 {
  let bytes = input.as_ref();
  let mut h: u32 = 0x811c_9dc5;
  for _ in 0..rounds {
    for b in bytes {
      h ^= *b as u32;
      h = h.wrapping_mul(0x0100_0193);
    }
  }
  h
}
