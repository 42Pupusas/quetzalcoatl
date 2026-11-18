use super::*;
use crate::common::drop_counter::DropCounter;
use std::sync::atomic::{AtomicUsize, Ordering};

fn ring<T>(cap: usize) -> ArcRingBuffer<T> {
    ArcRingBuffer::new(Capacity::exact(cap), 4)
}

#[test]
fn a_pushed_value_comes_back_through_the_arc() {
    let (producer, mut consumer) = ring::<u64>(4).split();
    producer.push(42).unwrap();
    assert_eq!(*consumer.pop().unwrap(), 42);
}

#[test]
fn a_ring_without_seats_is_rejected() {
    let attempt = std::panic::catch_unwind(|| ArcRingBuffer::<u64>::new(Capacity::exact(4), 0));
    assert!(attempt.is_err());
}

/// The point of the wrapper: every consumer gets the same allocation
/// rather than a full copy of a large payload.
#[test]
fn two_consumers_receive_the_same_allocation() {
    let (producer, mut c1) = ring::<[u8; 2048]>(8).split();
    let mut c2 = c1.clone();
    producer.push([0xAB; 2048]).unwrap();

    let v1 = c1.pop().unwrap();
    let v2 = c2.pop().unwrap();
    assert!(Arc::ptr_eq(&v1, &v2));
    assert_eq!(v1[0], 0xAB);
}

/// The wrapper exists so `T` need not be `Clone`; the plain ring's
/// `pop` requires it.
#[test]
fn a_payload_that_cannot_be_cloned_still_broadcasts() {
    #[derive(Debug)]
    struct NotClone(u64);

    let (producer, mut consumer) = ring::<NotClone>(4).split();
    producer.push(NotClone(99)).unwrap();
    assert_eq!(consumer.pop().unwrap().0, 99);
}

#[test]
fn a_full_ring_hands_the_value_back() {
    let (producer, _consumer) = ring::<u32>(2).split();
    producer.push(1).unwrap();
    producer.push(2).unwrap();
    assert_eq!(producer.push(3).unwrap_err(), 3);
}

/// A rejected push unwraps the `Arc` it speculatively allocated. The
/// caller must get the original value back, dropped exactly once.
#[test]
fn a_rejected_value_is_returned_and_dropped_once() {
    let counter = Arc::new(AtomicUsize::new(0));
    let (producer, _consumer) = ring::<DropCounter>(2).split();
    for _ in 0..2 {
        producer
            .push(DropCounter {
                counter: Arc::clone(&counter),
            })
            .unwrap();
    }

    let returned = producer
        .push(DropCounter {
            counter: Arc::clone(&counter),
        })
        .unwrap_err();
    assert_eq!(counter.load(Ordering::Relaxed), 0);

    drop(returned);
    assert_eq!(counter.load(Ordering::Relaxed), 1);
}

#[test]
fn a_reader_dereferences_past_the_arc_to_the_value() {
    let (producer, mut consumer) = ring::<String>(4).split();
    producer.push("hello".to_string()).unwrap();
    assert_eq!(&*consumer.pop_ref().unwrap(), "hello");
}

#[test]
fn a_reserved_slot_publishes_what_was_written_to_it() {
    let (mut producer, mut consumer) = ring::<u64>(4).split();
    producer.reserve().unwrap().write(42).commit();
    assert_eq!(*consumer.pop().unwrap(), 42);
}

#[test]
fn a_reservation_is_refused_when_the_ring_is_full() {
    let (mut producer, _consumer) = ring::<u64>(2).split();
    producer.reserve().unwrap().write(1).commit();
    producer.reserve().unwrap().write(2).commit();
    assert!(producer.reserve().is_none());
}

/// The raw path: initialize the slot through `slot_mut`, then commit
/// without the writer having seen a value.
#[test]
fn a_slot_initialized_by_hand_can_be_committed_unchecked() {
    let (mut producer, mut consumer) = ring::<u64>(4).split();
    let mut writer = producer.reserve().unwrap();
    writer.slot_mut().write(Arc::new(7));
    // SAFETY: the slot was just initialized by the write above.
    unsafe { writer.commit_unchecked() };
    assert_eq!(*consumer.pop().unwrap(), 7);
}

#[test]
fn an_uncommitted_reservation_publishes_nothing() {
    let (mut producer, mut consumer) = ring::<u64>(4).split();
    drop(producer.reserve().unwrap());
    assert!(consumer.pop().is_none());
}

#[test]
fn a_push_on_an_empty_ring_does_not_block() {
    let (producer, mut consumer) = ring::<u64>(4).split();
    producer.push_block(1).unwrap();
    producer.push_block(2).unwrap();
    assert_eq!(*consumer.pop().unwrap(), 1);
    assert_eq!(*consumer.pop().unwrap(), 2);
}

/// With nobody left to drain, a blocking push must give the value
/// back rather than wait forever.
#[test]
fn a_blocking_push_gives_up_once_every_consumer_has_gone() {
    let (producer, consumer) = ring::<u64>(2).split();
    producer.push(1).unwrap();
    producer.push(2).unwrap();
    drop(consumer);
    assert_eq!(producer.push_block(3).unwrap_err(), 3);
}

#[test]
fn a_blocking_reservation_succeeds_on_an_empty_ring() {
    let (mut producer, mut consumer) = ring::<u64>(4).split();
    producer.reserve_block().unwrap().write(5).commit();
    assert_eq!(*consumer.pop().unwrap(), 5);
}

#[test]
fn a_blocking_reservation_gives_up_once_every_consumer_has_gone() {
    let (mut producer, consumer) = ring::<u64>(2).split();
    producer.reserve_block().unwrap().write(1).commit();
    producer.reserve_block().unwrap().write(2).commit();
    drop(consumer);
    assert!(producer.reserve_block().is_none());
}

#[test]
fn a_blocking_push_waits_for_a_consumer_to_advance() {
    let (producer, mut consumer) = ring::<u64>(2).split();
    producer.push(1).unwrap();
    producer.push(2).unwrap();

    let reader = std::thread::spawn(move || {
        assert_eq!(*consumer.pop().unwrap(), 1);
        consumer
    });
    let consumer = reader.join().unwrap();

    producer.push_block(3).unwrap();
    drop(consumer);
}

#[test]
fn a_cloned_producer_writes_into_the_same_ring() {
    let (producer, mut consumer) = ring::<u64>(4).split();
    let second = producer.clone();
    producer.push(1).unwrap();
    second.push(2).unwrap();

    assert_eq!(*consumer.pop().unwrap(), 1);
    assert_eq!(*consumer.pop().unwrap(), 2);
}

#[test]
fn an_untouched_ring_is_empty_from_both_ends() {
    let (producer, consumer) = ring::<u64>(4).split();
    assert!(producer.is_empty());
    assert!(!producer.is_full());
    assert_eq!(producer.len(), 0);
    assert!(consumer.is_empty());
    assert!(!consumer.is_full());
    assert_eq!(consumer.len(), 0);
}

#[test]
fn a_filled_ring_reports_full_from_both_ends() {
    let (producer, consumer) = ring::<u64>(4).split();
    for i in 0..4 {
        producer.push(i).unwrap();
    }
    assert_eq!(producer.len(), 4);
    assert!(producer.is_full());
    assert!(!producer.is_empty());
    assert_eq!(consumer.len(), 4);
    assert!(consumer.is_full());
    assert!(!consumer.is_empty());
}

/// Each consumer has its own backlog, so one draining does not empty
/// the other's view.
#[test]
fn each_consumer_reports_its_own_backlog() {
    let (producer, mut c1) = ring::<u64>(4).split();
    let c2 = c1.clone();
    producer.push(1).unwrap();
    producer.push(2).unwrap();

    c1.pop().unwrap();
    assert_eq!(c1.len(), 1);
    assert_eq!(c2.len(), 2);
}

/// The slot keeps its `Arc` after every consumer has read it; the
/// payload dies with the ring, exactly once however many consumers
/// held a handle.
#[test]
fn the_payload_is_dropped_once_when_the_ring_goes() {
    let counter = Arc::new(AtomicUsize::new(0));
    {
        let (producer, mut c1) = ring::<DropCounter>(4).split();
        let mut c2 = c1.clone();
        producer
            .push(DropCounter {
                counter: Arc::clone(&counter),
            })
            .unwrap();

        let v1 = c1.pop().unwrap();
        let v2 = c2.pop().unwrap();
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        drop(v1);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        drop(v2);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
    assert_eq!(counter.load(Ordering::Relaxed), 1);
}

#[test]
#[cfg(feature = "async")]
fn a_value_pushed_asynchronously_is_popped_asynchronously() {
    let (producer, mut consumer) = ring::<u64>(4).split();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        producer.push_async(11).await.unwrap();
        assert_eq!(*consumer.pop_async().await.unwrap(), 11);
    }));
}

#[test]
#[cfg(feature = "async")]
fn an_async_pop_ends_once_the_producer_has_gone() {
    let (producer, mut consumer) = ring::<u64>(4).split();
    let h = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(producer);
    });
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        assert!(consumer.pop_async().await.is_none());
    }));
    h.join().unwrap();
}

#[test]
#[cfg(feature = "async")]
fn an_async_push_hands_the_value_back_once_every_consumer_has_gone() {
    let (producer, consumer) = ring::<u64>(4).split();
    for i in 0..4 {
        producer.push(i).unwrap();
    }
    let h = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(consumer);
    });
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        assert_eq!(producer.push_async(99).await, Err(99));
    }));
    h.join().unwrap();
}
