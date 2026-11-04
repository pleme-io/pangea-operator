use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Serialize};

fn default_dial_secs() -> u64 {
    120
}

fn default_read_secs() -> u64 {
    120
}

fn default_idle_secs() -> u64 {
    300
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderGuardConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_dial_secs")]
    pub dial_secs: u64,
    #[serde(default = "default_read_secs")]
    pub read_secs: u64,
    #[serde(default = "default_idle_secs")]
    pub idle_secs: u64,
}

impl Default for ProviderGuardConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dial_secs: default_dial_secs(),
            read_secs: default_read_secs(),
            idle_secs: default_idle_secs(),
        }
    }
}

impl ProviderGuardConfig {
    #[must_use]
    pub fn refusal(&self) -> Option<String> {
        if !self.enabled {
            return None;
        }
        if self.dial_secs == 0 || self.read_secs == 0 {
            return Some("provider_guard: dial_secs and read_secs must be above 0".to_string());
        }
        if self.idle_secs <= self.read_secs {
            return Some(format!(
                "provider_guard: idle_secs {} must exceed read_secs {}, or the watchdog cancels \
                 the whole pass before a read's deadline can replace one provider",
                self.idle_secs, self.read_secs
            ));
        }
        None
    }
}

static SETTINGS: Mutex<Option<ProviderGuardConfig>> = Mutex::new(None);

pub fn configure(cfg: &ProviderGuardConfig) -> Option<String> {
    let refusal = cfg.refusal();
    let mut effective = *cfg;
    effective.enabled = cfg.enabled && refusal.is_none();
    *SETTINGS.lock().unwrap_or_else(PoisonError::into_inner) = Some(effective);
    refusal
}

#[must_use]
pub fn settings() -> ProviderGuardConfig {
    SETTINGS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_guard_defaults_off_at_the_bounds_magma_ships() {
        let d = ProviderGuardConfig::default();
        assert!(!d.enabled);
        assert_eq!((d.dial_secs, d.read_secs, d.idle_secs), (120, 120, 300));
        assert_eq!(d.refusal(), None);
    }

    #[test]
    fn a_bound_the_guard_cannot_honour_is_refused_only_when_enabled() {
        let on = |dial_secs, read_secs, idle_secs| ProviderGuardConfig {
            enabled: true,
            dial_secs,
            read_secs,
            idle_secs,
        };
        assert_eq!(on(120, 120, 300).refusal(), None);
        assert!(on(0, 120, 300).refusal().is_some());
        assert!(on(120, 0, 300).refusal().is_some());
        assert!(on(120, 300, 300).refusal().unwrap().contains("idle_secs"));
        assert!(on(120, 300, 120).refusal().is_some());
        let off = ProviderGuardConfig {
            enabled: false,
            ..on(0, 0, 0)
        };
        assert_eq!(off.refusal(), None);
    }
}
