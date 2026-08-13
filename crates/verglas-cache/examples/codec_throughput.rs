//! Codec throughput comparison for the write-back tier (#180): encode the same
//! payload with `reed-solomon-simd` (the production backend) and
//! `reed-solomon-erasure`, both striped identically, and print encode GB/s.
//!
//! This is the "pick with numbers" evidence the issue asks for. Run:
//!   cargo run --release -p verglas-cache --example codec_throughput

use std::time::Instant;

use reed_solomon_erasure::galois_8::ReedSolomon as ErasureRs;
use verglas_cache::writeback_codec::{Geometry, StripeEncoder};

/// Per-fragment stripe chunk both codecs use (1 MiB, even).
const CHUNK: usize = 1024 * 1024;

/// Encodes `body` with the production SIMD codec, returning elapsed seconds.
fn encode_simd(k: usize, m: usize, body: &[u8]) -> f64 {
    let geometry = Geometry::streaming(k, m, CHUNK).expect("geometry");
    let start = Instant::now();
    let mut encoder = StripeEncoder::new(geometry);
    encoder.push(body).expect("push");
    let _ = encoder.finish().expect("finish");
    start.elapsed().as_secs_f64()
}

/// Encodes `body` with reed-solomon-erasure, striped the same way, returning
/// elapsed seconds. Only the encode is timed.
fn encode_erasure(k: usize, m: usize, body: &[u8]) -> f64 {
    let rs = ErasureRs::new(k, m).expect("rs");
    let stripe = k * CHUNK;
    let start = Instant::now();
    let mut offset = 0;
    while offset < body.len() {
        let end = (offset + stripe).min(body.len());
        let mut shards: Vec<Vec<u8>> = vec![vec![0u8; CHUNK]; k + m];
        let slice = &body[offset..end];
        for (i, chunk) in slice.chunks(CHUNK).enumerate() {
            shards[i][..chunk.len()].copy_from_slice(chunk);
        }
        rs.encode(&mut shards).expect("encode");
        offset = end;
    }
    start.elapsed().as_secs_f64()
}

/// Reports GB/s for a payload size across a set of geometries.
fn main() {
    println!("codec throughput (encode only), 1 MiB stripe chunk\n");
    println!(
        "{:>8}  {:>8}  {:>14}  {:>14}",
        "payload", "k+m", "simd GB/s", "erasure GB/s"
    );
    for size_mib in [32usize, 128] {
        let body: Vec<u8> = (0..size_mib * 1024 * 1024)
            .map(|i| (i % 251) as u8)
            .collect();
        let gib = body.len() as f64 / 1e9;
        for (k, m) in [(4usize, 2usize), (8, 3)] {
            // Warm once, then measure the median of three runs each.
            let _ = encode_simd(k, m, &body);
            let mut simd: Vec<f64> = (0..3).map(|_| gib / encode_simd(k, m, &body)).collect();
            let mut era: Vec<f64> = (0..3).map(|_| gib / encode_erasure(k, m, &body)).collect();
            simd.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
            era.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
            println!(
                "{:>6}MiB  {:>4}+{:<3}  {:>14.3}  {:>14.3}",
                size_mib, k, m, simd[1], era[1]
            );
        }
    }
}
