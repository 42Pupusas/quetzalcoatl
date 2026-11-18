use super::{DefaultConfig, Producer};
use crate::capacity::Capacity;
use crate::mpmc::{Consumer, SplitRing};

/// A producer pinned to a full batch of positions, exactly one of
/// which a consumer has released.
///
/// This is the state a producer parks from: it holds reserved
/// positions it cannot yet write to, and whether it parks depends
/// on finding the one position that has come free. The consumer
/// clones stagger their scan cursors by one position each, which is
/// what lets a chosen position be the one released.
struct PinnedBatch {
    producer: Producer<u64, DefaultConfig>,
    consumers: Vec<Consumer<u64, DefaultConfig>>,
}

/// Releases the consumers before the producer.
///
/// The pinned batch is real, not forged — it is what a refill hands
/// out — so dropping the producer abandons positions the previous
/// round still holds, and `BatchAbandon` waits for a consumer to
/// release each one. Nothing in the test ever will. Dropping the
/// consumers first closes the ring, which is the condition that
/// abandonment gives up on.
impl Drop for PinnedBatch {
    fn drop(&mut self) {
        self.consumers.clear();
    }
}

impl PinnedBatch {
    const CAP: usize = 8;

    /// Fills the ring, releases the single position `free_bit`
    /// positions into the producer's next batch, and pins the
    /// producer to that whole batch.
    ///
    /// A release hands the slot one lap on, so popping position `k`
    /// frees position `k + CAP` — bit `k` of a batch based at
    /// `CAP`. Passing `None` releases nothing, leaving every
    /// position in the batch blocked.
    fn with_one_free(free_bit: Option<usize>) -> Self {
        let (producer, consumer) =
            SplitRing::<u64, DefaultConfig>::new(Capacity::exact(Self::CAP)).split();
        for i in 0..Self::CAP {
            producer.push(i as u64).expect("the ring starts empty");
        }
        let mut consumers = Vec::new();
        if let Some(bit) = free_bit {
            for _ in 0..bit {
                consumers.push(consumer.clone());
            }
            let popper = consumers.last().unwrap_or(&consumer);
            popper.pop().expect("a full ring publishes every position");
        }
        producer.batch_start.set(Self::CAP);
        producer.batch_unused.set((1 << Self::CAP) - 1);
        Self {
            producer,
            consumers,
        }
    }

    /// What the producer would decide: whether the pre-park gate
    /// sees room, and which batch bit a write actually lands on.
    fn acquired(&self) -> (bool, Option<(u32, usize)>) {
        let gate = self.producer.has_free_slot();
        (gate, self.producer.acquire_slot())
    }
}

/// The producer-side mirror of the consumer's scan property: a
/// producer holding a batch finds the one released position
/// wherever it sits, so failing to find one means the ring really
/// is full rather than that the free slot was past the primary.
///
/// Bit zero is the primary, which the short spin resolves on its
/// own; every higher bit is only reachable through `scan_unused`,
/// the out-of-order path that lets a producer publish into whatever
/// slot of its batch drains first. Truncating that scan strands a
/// producer on a ring with room in it.
#[test]
fn a_pinned_producer_finds_its_one_free_position_at_every_offset() {
    for bit in 0..PinnedBatch::CAP {
        let batch = PinnedBatch::with_one_free(Some(bit));
        let (gate, acquired) = batch.acquired();
        assert!(gate, "the pre-park gate missed the free position at {bit}");
        assert_eq!(
            acquired,
            Some((
                u32::try_from(bit).expect("a bit index fits a batch bitmap"),
                PinnedBatch::CAP + bit,
            )),
            "the batch scan did not reach the free position at {bit}",
        );
    }
}

/// The converse: with nothing released the producer must report a
/// full ring, or it never parks and spins instead.
#[test]
fn a_pinned_producer_whose_batch_is_all_blocked_reports_no_room() {
    let batch = PinnedBatch::with_one_free(None);
    let (gate, acquired) = batch.acquired();
    assert!(!gate, "the gate saw room in a batch with nothing released");
    assert_eq!(acquired, None, "a fully blocked batch yielded a position");
}
