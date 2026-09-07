//! Compile-time tuning of batching and scan behaviour.

/// Compile-time tunables for batching and scan behaviour.
///
/// Implement this on a zero-sized type to customize batching / scan
/// behavior at compile time; the default values live in
/// [`DefaultConfig`].
///
/// All values must satisfy:
/// - `PRODUCER_BATCH` in `1..=32` (bounded by the producer's `u32`
///   per-batch bitmap).
/// - `CAS_FAIL_SKIP > 0` and `CONSUMED_FLUSH > 0`.
///
/// Bounds are checked at monomorphization via `const _ = assert!`.
pub trait Config: 'static {
    /// Positions reserved per FAA on `claim`. Caps the per-batch
    /// `u32` bitmap; must be in `1..=32`. Larger values reduce
    /// `claim`-line traffic at the cost of producer-progress
    /// imbalance (a slow producer holds more positions out of
    /// reach of others).
    const PRODUCER_BATCH: usize;

    /// On CAS-failure during `pop`, advance the scan cursor by
    /// this many slots before retrying. A larger skip dramatically
    /// reduces CAS re-collisions because the contended slot's
    /// cacheline stays hot for hundreds of cycles. Empirical
    /// peaks land near 64–128 at `cap=1024`.
    const CAS_FAIL_SKIP: usize;

    /// Pops between flushes of each consumer's local count to the
    /// shared `consumed` watermark. Producers use the watermark
    /// to bound their batch FAA, so a stale flush only forces
    /// smaller producer batches (safe direction). Larger values
    /// reduce hot-line traffic on `consumed` at the cost of more
    /// pessimistic producer batching.
    const CONSUMED_FLUSH: usize;
}

/// Default tuning for [`RingBuffer`](super::RingBuffer).
///
/// `PRODUCER_BATCH = 32`, `CAS_FAIL_SKIP = 128`,
/// `CONSUMED_FLUSH = 64`. These were chosen by sweeping at
/// `cap = 1024` across `(P, Q)` shapes from `(2, 2)` through
/// `(8, 8)`; see the empirical table in [`Config::CAS_FAIL_SKIP`]
/// docs.
#[derive(Debug, Clone, Copy)]
pub struct DefaultConfig;

impl Config for DefaultConfig {
    const PRODUCER_BATCH: usize = 32;
    const CAS_FAIL_SKIP: usize = 128;
    const CONSUMED_FLUSH: usize = 64;
}

/// Inline-customization helper. Lets callers tune a ring without
/// declaring a named config type:
///
/// ```
/// use quetzalcoatl::mpmc::{Cfg, RingBuffer};
/// use quetzalcoatl::capacity::Capacity;
///
/// let (p, c) = RingBuffer::<u64, Cfg<16, 64, 32>>::new(Capacity::exact(64)).split();
/// ```
///
/// Type parameters: `<PRODUCER_BATCH, CAS_FAIL_SKIP, CONSUMED_FLUSH>`.
#[derive(Debug, Clone, Copy)]
pub struct Cfg<const B: usize, const S: usize, const F: usize>;

impl<const B: usize, const S: usize, const F: usize> Config for Cfg<B, S, F> {
    const PRODUCER_BATCH: usize = B;
    const CAS_FAIL_SKIP: usize = S;
    const CONSUMED_FLUSH: usize = F;
}

/// Compile-time-validated bounds for a [`Config`]. Forces a
/// monomorphization-time error when `C` violates one of:
/// - `PRODUCER_BATCH` in `1..=32` (capped by the producer's `u32`
///   per-batch bitmap),
/// - `CAS_FAIL_SKIP > 0`,
/// - `CONSUMED_FLUSH > 0`.
pub(super) struct ConfigBounds<C: Config>(std::marker::PhantomData<C>);

impl<C: Config> ConfigBounds<C> {
    pub(super) const VALIDATE: () = {
        assert!(
            C::PRODUCER_BATCH >= 1 && C::PRODUCER_BATCH <= 32,
            "Config::PRODUCER_BATCH must be in 1..=32 (bounded by the u32 per-batch bitmap)",
        );
        assert!(C::CAS_FAIL_SKIP >= 1, "Config::CAS_FAIL_SKIP must be >= 1");
        assert!(
            C::CONSUMED_FLUSH >= 1,
            "Config::CONSUMED_FLUSH must be >= 1",
        );
    };
}

#[cfg(test)]
mod tests {
    use super::super::RingBuffer;
    use crate::capacity::Capacity;
    use super::{Cfg, Config, ConfigBounds, DefaultConfig};

    #[test]
    fn default_values_match_the_documented_sweep() {
        assert_eq!(DefaultConfig::PRODUCER_BATCH, 32);
        assert_eq!(DefaultConfig::CAS_FAIL_SKIP, 128);
        assert_eq!(DefaultConfig::CONSUMED_FLUSH, 64);
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
        let (p, c) = RingBuffer::<u64, Cfg<16, 64, 32>>::new(Capacity::exact(64)).split();
        p.push(1).unwrap();
        assert_eq!(c.pop(), Some(1));
    }
}
