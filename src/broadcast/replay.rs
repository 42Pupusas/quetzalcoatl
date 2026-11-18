//! Replays broadcast `push_block` and the consumer's head advances and
//! drop against the park model's step graph. See
//! [`park_replay`](crate::common::park_replay).

use std::any::Any;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::consumer_floor::ConsumerFloor;
use super::{Producer, RingBuffer, SplitRing};
use crate::capacity::Capacity;
use crate::common::park::WakeSet;
use crate::common::park_registry::ParkSlot;
use crate::common::park_replay::{Explorer, Live, ModelGraph, ReplayTarget, RunConfig, Session};
use crate::common::replay_point::ReplayPoint;

struct BroadcastTarget(Arc<RingBuffer<u32>>);

impl ReplayTarget for BroadcastTarget {
    fn wake_set(&self) -> &WakeSet {
        &self.0.producer_park
    }

    fn closed(&self) -> bool {
        !self.0.consumers.any_subscribed()
    }

    fn free(&self) -> usize {
        match self.0.consumer_floor() {
            ConsumerFloor::NoConsumers => 0,
            ConsumerFloor::At(head) => {
                self.0.capacity.get() - self.0.tail.load(Ordering::Relaxed).wrapping_sub(head)
            }
        }
    }
}

/// Starts one run of `config`: a full ring of capacity `releases`, one
/// producer per lease pushing one value, and one consumer advancing its
/// head once per release, then unsubscribing if the run closes.
struct BroadcastRun;

impl BroadcastRun {
    fn start(config: &RunConfig) -> Live {
        assert_eq!(
            config.variant, "observing",
            "broadcast observes a free slot"
        );
        let (first, mut consumer) =
            SplitRing::<u32>::new(Capacity::exact(config.releases), 1).split();
        for value in 0..config.releases {
            first.push(u32::try_from(value).unwrap()).unwrap();
        }
        let ring = Arc::clone(&first.queue);
        ring.producer_park.gate.follow_in_replay();
        let producers: Vec<Producer<u32>> = config
            .leases
            .iter()
            .map(|&leased| Self::sibling_of(&first, leased))
            .collect();
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
                let pushed: Box<dyn Any + Send> = Box::new(producer.push_block(7).is_ok());
                pushed
            }));
        }
        Live {
            session,
            target: Box::new(BroadcastTarget(ring)),
            slots,
            threads,
            leases: config.leases.clone(),
            left: total,
            batched: false,
        }
    }

    /// A producer for the ring of `first`, with a park slot of its own
    /// when `leased`.
    fn sibling_of(first: &Producer<u32>, leased: bool) -> Producer<u32> {
        let mut producer = first.clone();
        if !leased {
            first.queue.producer_park_slots.release(producer.park_slot);
            producer.park_slot = ParkSlot::Shared;
        }
        producer
    }
}

#[test]
fn two_leased_producers_and_an_unsubscribe_follow_the_model() {
    let graph = ModelGraph::load("broadcast_two_leased");
    Explorer::new(&graph, &BroadcastRun::start).run();
}

#[test]
fn a_shared_producer_follows_the_model() {
    let graph = ModelGraph::load("broadcast_leased_and_shared");
    Explorer::new(&graph, &BroadcastRun::start).run();
}

#[test]
fn two_head_advances_follow_the_model() {
    let graph = ModelGraph::load("broadcast_two_leased_two_releases");
    Explorer::new(&graph, &BroadcastRun::start).run();
}
