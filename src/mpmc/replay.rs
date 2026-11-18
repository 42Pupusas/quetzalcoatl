//! Replays mpmc `push_block` and the consumer's releases and drop
//! against the park model's step graph. See
//! [`park_replay`](crate::common::park_replay).
//!
//! The ring holds four slots, the least mpmc allows, and starts full;
//! the model tracks only the slots freed since. A batch of one and a
//! watermark flushed on every pop keep a failed claim from reserving a
//! position, so the watermark counts the free slots exactly.

use std::any::Any;
use std::sync::Arc;

use super::{Cfg, Producer, RingBuffer, SplitRing};
use crate::capacity::Capacity;
use crate::common::park::WakeSet;
use crate::common::park_replay::{Explorer, Live, ModelGraph, ReplayTarget, RunConfig, Session};
use crate::common::replay_point::ReplayPoint;

type Single = Cfg<1, 64, 1>;

const CAPACITY: usize = 4;

struct MpmcTarget(Arc<RingBuffer<u32, Single>>);

impl ReplayTarget for MpmcTarget {
    fn wake_set(&self) -> &WakeSet {
        &self.0.producer_park
    }

    fn closed(&self) -> bool {
        self.0.consumer_closed.is_closed()
    }

    fn free(&self) -> usize {
        let claim = self.0.claim.load(std::sync::atomic::Ordering::Relaxed);
        self.0.consumed.free(claim, CAPACITY).unwrap_or(0)
    }
}

/// Starts one run of `config`: a full ring, one leased producer per
/// lease pushing one value, and one consumer popping once per release,
/// then dropping if the run closes.
struct MpmcRun;

impl MpmcRun {
    fn start(config: &RunConfig) -> Live {
        assert_eq!(config.variant, "repairedWakingAll", "mpmc wakes all");
        assert!(
            config.leases.iter().all(|&leased| leased),
            "mpmc producers always lease"
        );
        let (first, consumer) = SplitRing::<u32, Single>::new(Capacity::exact(CAPACITY)).split();
        for value in 0..CAPACITY {
            first.push(u32::try_from(value).unwrap()).unwrap();
        }
        let ring = Arc::clone(&first.queue);
        ring.producer_park.gate.follow_in_replay();
        let producers: Vec<Producer<u32, Single>> =
            config.leases.iter().map(|_| first.clone()).collect();
        drop(first);
        let slots = producers.iter().map(|p| p.park_slot).collect();

        let session = Session::new(producers.len() + 1);
        let total = config.releases;
        let closes = config.closes;
        let mut threads = vec![session.spawn(0, move || {
            for _ in 0..total {
                ReplayPoint::Release.reach();
                assert!(consumer.pop().is_some(), "the ring was filled");
            }
            let kept: Box<dyn Any + Send> = if closes {
                drop(consumer);
                Box::new(())
            } else {
                Box::new(consumer)
            };
            kept
        })];
        for (k, producer) in producers.into_iter().enumerate() {
            threads.push(session.spawn(k + 1, move || {
                let pushed = producer.push_block(7).is_ok();
                // Handed out, so the last producer's drop, which flushes
                // the wake set, runs after the run has ended.
                let kept: Box<dyn Any + Send> = Box::new((producer, pushed));
                kept
            }));
        }
        Live {
            session,
            target: Box::new(MpmcTarget(ring)),
            slots,
            threads,
            leases: config.leases.clone(),
            left: total,
            batched: false,
        }
    }
}

#[test]
fn one_producer_and_a_close_follow_the_model() {
    let graph = ModelGraph::load("mpmc_one_leased");
    Explorer::new(&graph, &MpmcRun::start).run();
}

#[test]
#[ignore = "~26k states, ~4 min in release"]
fn two_producers_and_one_release_follow_the_model() {
    let graph = ModelGraph::load("mpmc_two_leased_open");
    Explorer::new(&graph, &MpmcRun::start).run();
}

#[test]
#[ignore = "~130k states, ~22 min in release"]
fn two_producers_and_a_close_follow_the_model() {
    let graph = ModelGraph::load("mpmc_two_leased");
    Explorer::new(&graph, &MpmcRun::start).run();
}

#[test]
#[ignore = "~130k states, ~24 min in release"]
fn two_releases_follow_the_model() {
    let graph = ModelGraph::load("mpmc_two_leased_two_releases");
    Explorer::new(&graph, &MpmcRun::start).run();
}
