use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriftLeaf {
    pub name: String,
    pub last_check: Option<u64>,
}

fn hash_of(name: &str) -> [u8; 32] {
    *blake3::hash(name.as_bytes()).as_bytes()
}

#[must_use]
pub fn offsets<'a>(
    names: impl IntoIterator<Item = &'a str>,
    interval_secs: u64,
) -> BTreeMap<String, u64> {
    let mut ranked: Vec<([u8; 32], &str)> = names.into_iter().map(|n| (hash_of(n), n)).collect();
    ranked.sort_unstable();
    ranked.dedup_by(|a, b| a.1 == b.1);
    let n = u128::try_from(ranked.len()).unwrap_or(u128::MAX).max(1);
    let interval = u128::from(interval_secs.max(1));
    ranked
        .into_iter()
        .enumerate()
        .map(|(rank, (_, name))| {
            let r = u128::try_from(rank).unwrap_or(0);
            let offset = u64::try_from(r * interval / n).unwrap_or(0);
            (name.to_string(), offset)
        })
        .collect()
}

#[must_use]
pub fn last_slot(offset: u64, now: u64, interval_secs: u64) -> Option<u64> {
    let interval = interval_secs.max(1);
    if now < offset {
        return None;
    }
    Some(now - (now - offset) % interval)
}

#[must_use]
pub fn is_due(offset: u64, now: u64, interval_secs: u64, last_check: Option<u64>) -> bool {
    match (last_slot(offset, now, interval_secs), last_check) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some(slot), Some(last)) => last < slot,
    }
}

#[must_use]
pub fn next_due_in(offset: u64, now: u64, interval_secs: u64, last_check: Option<u64>) -> u64 {
    if is_due(offset, now, interval_secs, last_check) {
        return 0;
    }
    let interval = interval_secs.max(1);
    let next = match last_slot(offset, now, interval) {
        Some(slot) => slot + interval,
        None => offset,
    };
    next.saturating_sub(now)
}

#[must_use]
pub fn drift_due(leaves: &[DriftLeaf], now: u64, interval_secs: u64) -> Vec<String> {
    let placed = offsets(leaves.iter().map(|l| l.name.as_str()), interval_secs);
    let mut due: Vec<String> = leaves
        .iter()
        .filter(|l| {
            placed
                .get(&l.name)
                .is_some_and(|o| is_due(*o, now, interval_secs, l.last_check))
        })
        .map(|l| l.name.clone())
        .collect();
    due.sort();
    due.dedup();
    due
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3600;

    fn org(n: usize) -> Vec<String> {
        (0..n)
            .map(|i| format!("pleme-io-github-repos-repo-{i:04}"))
            .collect()
    }

    #[test]
    fn over_a_fake_hour_each_of_1023_leaves_is_due_once_and_no_minute_holds_more_than_its_share() {
        let names = org(1023);
        let start = 1_700_000_000 - (1_700_000_000 % HOUR);
        let mut leaves: Vec<DriftLeaf> = names
            .iter()
            .map(|n| DriftLeaf {
                name: n.clone(),
                last_check: Some(start),
            })
            .collect();
        let mut per_leaf: BTreeMap<String, u32> = BTreeMap::new();
        let mut per_minute = vec![0u32; 60];
        for t in (start + 1)..=(start + HOUR) {
            let due = drift_due(&leaves, t, HOUR);
            for name in &due {
                *per_leaf.entry(name.clone()).or_default() += 1;
                let minute = usize::try_from((t - start - 1) / 60).unwrap();
                per_minute[minute] += 1;
                if let Some(l) = leaves.iter_mut().find(|l| &l.name == name) {
                    l.last_check = Some(t);
                }
            }
        }
        assert_eq!(per_leaf.len(), 1023);
        assert!(per_leaf.values().all(|c| *c == 1));
        let bound = 1023_u32.div_ceil(60) + 1;
        let worst = per_minute.iter().copied().max().unwrap();
        assert!(worst <= bound, "worst minute {worst} > {bound}");
    }

    #[test]
    fn a_leaf_never_checked_is_due_at_its_first_slot_and_not_before() {
        assert!(!is_due(100, 50, HOUR, None));
        assert!(is_due(100, 100, HOUR, None));
        assert_eq!(next_due_in(100, 50, HOUR, None), 50);
    }

    #[test]
    fn a_leaf_checked_after_its_slot_waits_one_interval() {
        assert!(!is_due(100, 200, HOUR, Some(150)));
        assert_eq!(next_due_in(100, 200, HOUR, Some(150)), HOUR - 100);
        assert!(is_due(100, HOUR + 100, HOUR, Some(150)));
    }

    #[test]
    fn placement_is_stable_and_independent_of_input_order() {
        let a = offsets(["x", "y", "z"], HOUR);
        let b = offsets(["z", "x", "y"], HOUR);
        assert_eq!(a, b);
        let mut seen: Vec<u64> = a.values().copied().collect();
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1200, 2400]);
    }
}
