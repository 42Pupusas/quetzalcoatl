//! How many iterations a stress harness runs.
//!
//! The events the stress harnesses measure are rare, and the sample
//! size a default run affords is the wrong shape for estimating a
//! rate. `QUETZALCOATL_STRESS_ITERS` scales a harness up for a
//! campaign without editing it, and the default keeps the ordinary
//! `--ignored` run at its usual length.

/// The iteration count for one stress harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StressIters(usize);

impl StressIters {
    const VAR: &'static str = "QUETZALCOATL_STRESS_ITERS";

    /// Reads the override, falling back to `default` when it is unset
    /// or unparseable. Zero is treated as unset: a harness that runs
    /// no iterations reports nothing.
    #[must_use]
    pub fn from_env(default: usize) -> Self {
        Self::parse(std::env::var(Self::VAR).ok().as_deref(), default)
    }

    fn parse(raw: Option<&str>, default: usize) -> Self {
        let parsed = raw
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0);
        Self(parsed.unwrap_or(default))
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::StressIters;

    #[test]
    fn an_unset_override_yields_the_default() {
        assert_eq!(StressIters::parse(None, 200).get(), 200);
    }

    #[test]
    fn a_set_override_replaces_the_default() {
        assert_eq!(StressIters::parse(Some("5000"), 200).get(), 5000);
    }

    #[test]
    fn surrounding_whitespace_is_tolerated() {
        assert_eq!(StressIters::parse(Some(" 42\n"), 200).get(), 42);
    }

    #[test]
    fn garbage_falls_back_to_the_default() {
        assert_eq!(StressIters::parse(Some("lots"), 200).get(), 200);
    }

    #[test]
    fn zero_falls_back_to_the_default() {
        assert_eq!(StressIters::parse(Some("0"), 200).get(), 200);
    }
}
