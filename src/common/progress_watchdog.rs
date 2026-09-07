//! A deadlock detector for the cross-thread async tests.
//!
//! It aborts only when a test stops making progress, never on a
//! deadline. A wall-clock budget is the obvious design and the wrong
//! one here: under coverage instrumentation the lock-free hot loops
//! run orders of magnitude slower, and the tests are deliberately
//! thread-oversubscribed, so any fixed budget either false-fires on a
//! test that is merely slow or is too generous to catch a real hang.
//! Watching a counter advance separates "slow" from "stuck", which is
//! the distinction that matters.
//!
//! The watchdog owns its own stop flag and joins its thread on drop,
//! so a test cannot leave it running or forget to shut it down; it
//! only has to bump the counter it already keeps.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Aborts the process if `progress` stops advancing.
pub struct ProgressWatchdog {
    done: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ProgressWatchdog {
    const STALL_LIMIT: Duration = Duration::from_secs(30);
    const POLL_INTERVAL: Duration = Duration::from_millis(50);

    /// Watches `progress`, aborting if it holds still for 30 seconds.
    ///
    /// `label` names the test in the abort message.
    pub fn spawn(progress: Arc<AtomicU64>, label: &'static str) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&done);
        let thread = std::thread::spawn(move || {
            let mut stall = StallTracker::new();
            while !stop.load(Ordering::Acquire) {
                if stall.observe(&progress, Self::STALL_LIMIT) {
                    eprintln!("\n\n{label}: no progress for 30s, deadlocked — aborting\n");
                    std::process::abort();
                }
                std::thread::sleep(Self::POLL_INTERVAL);
            }
        });
        Self {
            done,
            thread: Some(thread),
        }
    }
}

/// How long the watched counter has been standing still.
///
/// Split out so the stall decision can be tested directly: the
/// watchdog's own answer to it is `abort`, which a test cannot observe
/// and survive.
struct StallTracker {
    last: u64,
    last_change: Instant,
}

impl StallTracker {
    fn new() -> Self {
        Self {
            last: 0,
            last_change: Instant::now(),
        }
    }

    /// Folds one reading into the tracker, reporting whether the
    /// counter has now been still for longer than `limit`.
    fn observe(&mut self, progress: &AtomicU64, limit: Duration) -> bool {
        let current = progress.load(Ordering::Relaxed);
        if current == self.last {
            return self.last_change.elapsed() > limit;
        }
        self.last = current;
        self.last_change = Instant::now();
        false
    }
}

impl Drop for ProgressWatchdog {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_the_watchdog_stops_its_thread() {
        let progress = Arc::new(AtomicU64::new(0));
        let watchdog = ProgressWatchdog::spawn(Arc::clone(&progress), "stops");
        progress.fetch_add(1, Ordering::Relaxed);
        drop(watchdog);
    }

    #[test]
    fn a_stalled_counter_is_tolerated_below_the_limit() {
        let progress = Arc::new(AtomicU64::new(0));
        let watchdog = ProgressWatchdog::spawn(Arc::clone(&progress), "stalled");
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(progress.load(Ordering::Relaxed), 0);
        drop(watchdog);
    }

    #[test]
    fn many_watchdogs_shut_down_independently() {
        let progress = Arc::new(AtomicU64::new(0));
        let watchdogs: Vec<_> = (0..4)
            .map(|_| ProgressWatchdog::spawn(Arc::clone(&progress), "many"))
            .collect();
        progress.fetch_add(1, Ordering::Relaxed);
        drop(watchdogs);
    }

    #[test]
    fn the_stall_limit_exceeds_the_poll_interval() {
        assert!(ProgressWatchdog::STALL_LIMIT > ProgressWatchdog::POLL_INTERVAL);
    }

    #[test]
    fn a_counter_standing_still_past_the_limit_reports_a_stall() {
        let progress = AtomicU64::new(0);
        let mut stall = StallTracker::new();
        assert!(!stall.observe(&progress, Duration::from_millis(50)));
        std::thread::sleep(Duration::from_millis(80));
        assert!(stall.observe(&progress, Duration::from_millis(50)));
    }

    #[test]
    fn an_advancing_counter_never_reports_a_stall() {
        let progress = AtomicU64::new(0);
        let mut stall = StallTracker::new();
        for _ in 0..5 {
            progress.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(30));
            assert!(!stall.observe(&progress, Duration::from_millis(20)));
        }
    }

    #[test]
    fn progress_after_a_near_stall_resets_the_clock() {
        let progress = AtomicU64::new(0);
        let mut stall = StallTracker::new();
        std::thread::sleep(Duration::from_millis(60));
        progress.fetch_add(1, Ordering::Relaxed);
        assert!(!stall.observe(&progress, Duration::from_millis(50)));
        assert!(!stall.observe(&progress, Duration::from_millis(50)));
    }

    #[test]
    fn a_counter_that_advances_then_stops_eventually_reports_a_stall() {
        let progress = AtomicU64::new(0);
        let mut stall = StallTracker::new();
        progress.fetch_add(1, Ordering::Relaxed);
        assert!(!stall.observe(&progress, Duration::from_millis(50)));
        std::thread::sleep(Duration::from_millis(80));
        assert!(stall.observe(&progress, Duration::from_millis(50)));
    }
}
