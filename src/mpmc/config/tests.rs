use super::super::SplitRing;
use super::{Cfg, Config, ConfigBounds, DefaultConfig};
use crate::capacity::Capacity;

#[test]
fn default_values_match_the_documented_sweep() {
    assert_eq!(DefaultConfig::PRODUCER_BATCH, 32);
    assert_eq!(DefaultConfig::CAS_FAIL_SKIP, 128);
    assert_eq!(DefaultConfig::CONSUMED_FLUSH, 64);
    const { assert!(DefaultConfig::REFILL_CAS) };
}

#[test]
fn cfg_refills_with_compare_exchange_unless_told_otherwise() {
    const { assert!(<Cfg<8, 64, 32> as Config>::REFILL_CAS) };
    const { assert!(!<Cfg<1, 64, 32, false> as Config>::REFILL_CAS) };
}

#[test]
fn a_ring_with_blind_refill_still_delivers_in_order() {
    let (p, c) = SplitRing::<u64, Cfg<1, 64, 32, false>>::new(Capacity::exact(8)).split();
    for v in 0..100u64 {
        p.push(v).unwrap();
        assert_eq!(c.pop(), Some(v));
    }
}

#[test]
fn cfg_bakes_its_parameters_into_the_impl() {
    assert_eq!(<Cfg<16, 64, 32> as Config>::PRODUCER_BATCH, 16);
    assert_eq!(<Cfg<16, 64, 32> as Config>::CAS_FAIL_SKIP, 64);
    assert_eq!(<Cfg<16, 64, 32> as Config>::CONSUMED_FLUSH, 32);
}

#[test]
fn a_violating_config_is_rejected_at_monomorphization() {
    // The bounds are checked when a RingBuffer<C> is constructed,
    // not when C is declared — so this only fails to compile for
    // an out-of-range C. In-bounds edge values must validate.
    let () = ConfigBounds::<Cfg<1, 1, 1>>::VALIDATE;
    let () = ConfigBounds::<Cfg<32, 1, 1>>::VALIDATE;
    let () = ConfigBounds::<DefaultConfig>::VALIDATE;
}

#[test]
fn a_ring_builds_around_an_inline_config() {
    let (p, c) = SplitRing::<u64, Cfg<16, 64, 32>>::new(Capacity::exact(64)).split();
    p.push(1).unwrap();
    assert_eq!(c.pop(), Some(1));
}
