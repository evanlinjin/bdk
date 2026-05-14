//! Micro-benchmarks for [`bdk_chain::block_graph::BlockGraph`].
//!
//! Comparison axes:
//! - `apply_update_extend_tip` — 1-block extension of an N-block graph. Hits the
//!   tip-extension fast path; should be O(m) where m = update size.
//! - `apply_update_fork_midheight` — 1-block fork at height N/2. Hits the
//!   fork-creation fast path; should be O(N) for materialising the new chain
//!   but leaves all other tips untouched (Arc-shared).
//! - `apply_changeset_noop` — no-op `apply_changeset` call. Forces a full
//!   `recompute()` over N blocks without changing the source-of-truth map.
//!   This is the baseline cost the fast paths skip.
//! - `from_changeset_cold` — reconstruct a graph from its full `ChangeSet`.
//!   Also pays one full `recompute()`.

use std::time::{Duration, Instant};

use bdk_chain::block_graph::BlockGraph;
use bdk_core::CheckPoint;
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, CompactTarget, TxMerkleNode};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

const SIZES: &[u32] = &[1_000, 10_000, 100_000];

fn header(prev: BlockHash, marker: u32) -> Header {
    Header {
        version: bitcoin::block::Version::ONE,
        prev_blockhash: prev,
        merkle_root: TxMerkleNode::all_zeros(),
        time: marker,
        bits: CompactTarget::from_consensus(0x207fffff),
        nonce: 0,
    }
}

fn genesis_header() -> Header {
    header(BlockHash::all_zeros(), 0)
}

/// Build a dense chain of length `n + 1` (genesis at index 0, tip at index n).
fn build_chain(n: u32) -> Vec<Header> {
    let g = genesis_header();
    let mut headers = Vec::with_capacity(n as usize + 1);
    headers.push(g);
    let mut prev = g.block_hash();
    for h in 1..=n {
        let hdr = header(prev, h);
        prev = hdr.block_hash();
        headers.push(hdr);
    }
    headers
}

/// Construct a populated `BlockGraph` from a dense chain of `Header`s.
fn build_graph(headers: &[Header]) -> BlockGraph {
    let blocks: Vec<(u32, Header)> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| (i as u32, *h))
        .collect();
    let cp = CheckPoint::from_blocks(blocks).expect("monotonic");
    let (mut graph, _) = BlockGraph::from_genesis(headers[0]);
    let _ = graph.apply_update(cp);
    graph
}

/// Time only `op(g)` — excludes both the clone of `template` and the drop of
/// the mutated graph after each iteration. At N=100K, both bookend operations
/// are O(N) and would otherwise swamp the µs-scale fast paths.
fn time_op<F: FnMut(&mut BlockGraph)>(
    template: &BlockGraph,
    iters: u64,
    mut op: F,
) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..iters {
        let mut g = template.clone();
        let start = Instant::now();
        op(&mut g);
        total += start.elapsed();
        drop(g);
    }
    total
}

fn bench_apply_update_extend_tip(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_graph/apply_update_extend_tip");
    for &n in SIZES {
        let headers = build_chain(n);
        let template = build_graph(&headers);
        let tip_hash = template.tip().hash();
        let new_block = header(tip_hash, n + 1);
        let update = CheckPoint::from_blocks(vec![(0, headers[0]), (n + 1, new_block)])
            .expect("monotonic");

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_custom(|iters| {
                time_op(&template, iters, |g| {
                    let _ = g.apply_update(update.clone());
                })
            });
        });
    }
    group.finish();
}

fn bench_apply_update_fork_midheight(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_graph/apply_update_fork_midheight");
    for &n in SIZES {
        let headers = build_chain(n);
        let template = build_graph(&headers);
        let mid_h = n / 2;
        let parent_hash = headers[mid_h as usize].block_hash();
        let fork_block = header(parent_hash, 0x8000_0000 ^ n);
        let update = CheckPoint::from_blocks(vec![(0, headers[0]), (mid_h + 1, fork_block)])
            .expect("monotonic");

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_custom(|iters| {
                time_op(&template, iters, |g| {
                    let _ = g.apply_update(update.clone());
                })
            });
        });
    }
    group.finish();
}

fn bench_apply_changeset_noop(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_graph/apply_changeset_noop");
    let empty = bdk_chain::block_graph::ChangeSet::default();
    for &n in SIZES {
        let headers = build_chain(n);
        let template = build_graph(&headers);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_custom(|iters| {
                time_op(&template, iters, |g| {
                    g.apply_changeset(&empty);
                })
            });
        });
    }
    group.finish();
}

fn bench_from_changeset_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_graph/from_changeset_cold");
    for &n in SIZES {
        let headers = build_chain(n);
        let template = build_graph(&headers);
        let cs = template.initial_changeset();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let cs = cs.clone();
                    let start = Instant::now();
                    let graph =
                        BlockGraph::from_changeset(cs).expect("genesis preserved");
                    total += start.elapsed();
                    drop(graph);
                }
                total
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_apply_update_extend_tip,
    bench_apply_update_fork_midheight,
    bench_apply_changeset_noop,
    bench_from_changeset_cold,
);
criterion_main!(benches);
