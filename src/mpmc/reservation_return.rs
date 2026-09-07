//! Unwind-safe return of an abandoned MPMC reservation.

use std::cell::Cell;

/// Owns the obligation to return a reserved position to its producer's
/// batch bitmap.
///
/// A producer claims positions a batch at a time and hands the unused
/// ones back to `batch_unused`, where its next `push` or `reserve` can
/// take them without claiming a new batch. When the reservation already
/// holds a value, that return has to happen after the value's destructor
/// — which may panic.
///
/// Unwinding out of `T::drop` with the return still ahead of it loses
/// the bit: the position stays reserved for the rest of the handle's
/// life, and is only released when the producer is dropped.
///
/// Holding one of these across the destructor closes that window, since
/// the bit is restored from this type's own `Drop`.
pub(super) struct ReservationReturn<'a> {
    unused: &'a Cell<u32>,
    bit: u32,
}

impl<'a> ReservationReturn<'a> {
    /// Arms the return of `bit` to the `unused` bitmap.
    ///
    /// Construct this *before* any code that might panic — the value's
    /// destructor, in particular.
    pub(super) const fn new(unused: &'a Cell<u32>, bit: u32) -> Self {
        Self { unused, bit }
    }
}

impl Drop for ReservationReturn<'_> {
    fn drop(&mut self) {
        self.unused.set(self.unused.get() | (1u32 << self.bit));
    }
}
