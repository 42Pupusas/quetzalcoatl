//! Replays mpsc `push_block` and the consumer's release and drop
//! against the park model's step graph. See
//! [`park_replay`](crate::common::park_replay).

use std::any::Any;
use std::sync::Arc;

use super::{Consumer, Producer, RingBuffer, SplitRing};
use crate::capacity::Capacity;
use crate::common::park::WakeSet;
use crate::common::park_registry::ParkSlot;
use crate::common::park_replay::{Explorer, Live, ModelGraph, ReplayTarget, RunConfig, Session};
use crate::common::replay_point::ReplayPoint;

struct MpscTarget(Arc<RingBuffer<u32>>);

impl ReplayTarget for MpscTarget {
    fn wake_set(&self) -> &WakeSet {
        &self.0.producer_park
    }

    fn closed(&self) -> bool {
        self.0.consumer_closed.is_closed()
    }

    fn free(&self) -> usize {
        self.0.capacity.get() - self.0.ledger.len(self.0.capacity)
    }
}

/// Starts one run of `config`: a full ring of capacity `releases`, one
/// producer per lease pushing one value, and the consumer releasing
/// in the batches the explorer grants, then dropping if the run closes.
struct MpscRun;

impl MpscRun {
    fn start(config: &RunConfig) -> Live {
        assert_eq!(config.variant, "repaired", "mpsc stands down");
        let (first, mut consumer) = SplitRing::<u32>::new(Capacity::exact(config.releases)).split();
        for value in 0..config.releases {
            first.push(u32::try_from(value).unwrap()).unwrap();
        }
        let ring = Arc::clone(&first.queue);
        ring.producer_park.gate.follow_in_replay();
        let mut spare = Some(first);
        let producers: Vec<Producer<u32>> = config
            .leases
            .iter()
            .map(|&leased| match (leased, spare.take()) {
                (true, Some(first)) => first,
                (true, None) => {
                    ring.producer_count.register();
                    Producer::new_with(Arc::clone(&ring), ring.producer_slots.lease())
                }
                (false, kept) => {
                    spare = kept;
                    ring.producer_count.register();
                    Producer::new_with(Arc::clone(&ring), ParkSlot::Shared)
                }
            })
            .collect();
        drop(spare);
        let slots = producers.iter().map(|p| p.park_slot).collect();

        let session = Session::new(producers.len() + 1);
        let total = config.releases;
        let closes = config.closes;
        let mut threads = vec![session.spawn(0, move || {
            let mut done = 0;
            while done < total {
                ReplayPoint::Release.reach();
                let batch = Session::granted_batch();
                if batch == 1 {
                    let _ = consumer.pop();
                } else {
                    consumer.drain_up_to(batch, drop);
                }
                done += batch;
            }
            Self::finish(consumer, closes)
        })];
        for (k, producer) in producers.into_iter().enumerate() {
            threads.push(session.spawn(k + 1, move || {
                let pushed: Box<dyn Any + Send> = Box::new(producer.push_block(7).is_ok());
                pushed
            }));
        }
        Live {
            session,
            target: Box::new(MpscTarget(ring)),
            slots,
            threads,
            leases: config.leases.clone(),
            left: total,
            batched: true,
        }
    }

    /// Drops the consumer here when the run closes; otherwise hands it
    /// out of the run, so its drop happens after the run has ended.
    fn finish(consumer: Consumer<u32>, closes: bool) -> Box<dyn Any + Send> {
        if closes {
            drop(consumer);
            Box::new(())
        } else {
            Box::new(consumer)
        }
    }
}

#[test]
fn two_leased_producers_and_a_close_follow_the_model() {
    let graph = ModelGraph::load("mpsc_two_leased");
    Explorer::new(&graph, &MpscRun::start).run();
}

#[test]
fn a_shared_producer_follows_the_model() {
    let graph = ModelGraph::load("mpsc_leased_and_shared");
    Explorer::new(&graph, &MpscRun::start).run();
}

#[test]
fn batched_releases_follow_the_model() {
    let graph = ModelGraph::load("mpsc_two_leased_two_releases");
    Explorer::new(&graph, &MpscRun::start).run();
}
