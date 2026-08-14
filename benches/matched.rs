//! Like-for-like channel comparisons.
//!
//! `comparison.rs` pairs quetzalcoatl's spin-retry API against
//! crossbeam's and tokio's blocking API. That mixes an API difference
//! with a scheduling-policy difference, so its numbers cannot be read
//! as a channel comparison.
//!
//! This bench keeps the two policies apart:
//!
//! - `spin_*` modules: every arm retries a try-operation and spins.
//! - `block_*` modules: every arm parks when it cannot proceed.
//!
//! The `block_*` modules are also the only comparison arms that reach
//! the park-arming slow path, so they are the ones that exercise the
//! SeqCst fences.

mod harness;

use harness::mpsc::MpscWorkload;
use harness::spsc::SpscWorkload;

fn main() {
    divan::main();
}

const SPSC_CAPACITY: usize = 4096;
const SPSC_ITEMS: u64 = 100_000;

const MPSC_CAPACITY: usize = 8192;
const MPSC_ITEMS_PER_PRODUCER: u64 = 5_000;
const PRODUCERS: &[u64] = &[1, 2, 4, 8];

mod spin_spsc {
    use super::*;

    #[divan::bench]
    fn quetzalcoatl(bencher: divan::Bencher) {
        let w = SpscWorkload::new(SPSC_CAPACITY, SPSC_ITEMS);
        bencher
            .counter(divan::counter::ItemsCount::new(SPSC_ITEMS))
            .bench_local(|| w.quetzalcoatl_spin());
    }

    #[divan::bench]
    fn crossbeam(bencher: divan::Bencher) {
        let w = SpscWorkload::new(SPSC_CAPACITY, SPSC_ITEMS);
        bencher
            .counter(divan::counter::ItemsCount::new(SPSC_ITEMS))
            .bench_local(|| w.crossbeam_spin());
    }

    #[divan::bench]
    fn std_sync_channel(bencher: divan::Bencher) {
        let w = SpscWorkload::new(SPSC_CAPACITY, SPSC_ITEMS);
        bencher
            .counter(divan::counter::ItemsCount::new(SPSC_ITEMS))
            .bench_local(|| w.std_spin());
    }
}

mod block_spsc {
    use super::*;

    #[divan::bench]
    fn quetzalcoatl(bencher: divan::Bencher) {
        let w = SpscWorkload::new(SPSC_CAPACITY, SPSC_ITEMS);
        bencher
            .counter(divan::counter::ItemsCount::new(SPSC_ITEMS))
            .bench_local(|| w.quetzalcoatl_block());
    }

    #[divan::bench]
    fn crossbeam(bencher: divan::Bencher) {
        let w = SpscWorkload::new(SPSC_CAPACITY, SPSC_ITEMS);
        bencher
            .counter(divan::counter::ItemsCount::new(SPSC_ITEMS))
            .bench_local(|| w.crossbeam_block());
    }

    #[divan::bench]
    fn std_sync_channel(bencher: divan::Bencher) {
        let w = SpscWorkload::new(SPSC_CAPACITY, SPSC_ITEMS);
        bencher
            .counter(divan::counter::ItemsCount::new(SPSC_ITEMS))
            .bench_local(|| w.std_block());
    }
}

mod spin_mpsc {
    use super::*;

    #[divan::bench(args = PRODUCERS)]
    fn quetzalcoatl(bencher: divan::Bencher, num_producers: u64) {
        let w = MpscWorkload::new(MPSC_CAPACITY, MPSC_ITEMS_PER_PRODUCER, num_producers);
        bencher
            .counter(divan::counter::ItemsCount::new(w.total_items()))
            .bench_local(|| w.quetzalcoatl_spin());
    }

    #[divan::bench(args = PRODUCERS)]
    fn crossbeam(bencher: divan::Bencher, num_producers: u64) {
        let w = MpscWorkload::new(MPSC_CAPACITY, MPSC_ITEMS_PER_PRODUCER, num_producers);
        bencher
            .counter(divan::counter::ItemsCount::new(w.total_items()))
            .bench_local(|| w.crossbeam_spin());
    }

    #[divan::bench(args = PRODUCERS)]
    fn std_sync_channel(bencher: divan::Bencher, num_producers: u64) {
        let w = MpscWorkload::new(MPSC_CAPACITY, MPSC_ITEMS_PER_PRODUCER, num_producers);
        bencher
            .counter(divan::counter::ItemsCount::new(w.total_items()))
            .bench_local(|| w.std_spin());
    }
}

mod block_mpsc {
    use super::*;

    #[divan::bench(args = PRODUCERS)]
    fn quetzalcoatl(bencher: divan::Bencher, num_producers: u64) {
        let w = MpscWorkload::new(MPSC_CAPACITY, MPSC_ITEMS_PER_PRODUCER, num_producers);
        bencher
            .counter(divan::counter::ItemsCount::new(w.total_items()))
            .bench_local(|| w.quetzalcoatl_block());
    }

    #[divan::bench(args = PRODUCERS)]
    fn crossbeam(bencher: divan::Bencher, num_producers: u64) {
        let w = MpscWorkload::new(MPSC_CAPACITY, MPSC_ITEMS_PER_PRODUCER, num_producers);
        bencher
            .counter(divan::counter::ItemsCount::new(w.total_items()))
            .bench_local(|| w.crossbeam_block());
    }

    #[divan::bench(args = PRODUCERS)]
    fn std_sync_channel(bencher: divan::Bencher, num_producers: u64) {
        let w = MpscWorkload::new(MPSC_CAPACITY, MPSC_ITEMS_PER_PRODUCER, num_producers);
        bencher
            .counter(divan::counter::ItemsCount::new(w.total_items()))
            .bench_local(|| w.std_block());
    }
}
