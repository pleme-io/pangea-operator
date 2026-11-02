use std::sync::OnceLock;

static CURRENT: OnceLock<String> = OnceLock::new();

#[must_use]
pub fn current() -> &'static str {
    CURRENT.get_or_init(|| {
        std::env::current_exe()
            .ok()
            .and_then(|path| std::fs::read(path).ok())
            .map(|bytes| blake3::hash(&bytes).to_hex()[..32].to_string())
            .unwrap_or_else(|| ["pkg", env!("CARGO_PKG_VERSION")].join(":"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_build_identity_is_the_running_binary_and_is_stable() {
        let exe = std::fs::read(std::env::current_exe().unwrap()).unwrap();
        assert_eq!(current(), &blake3::hash(&exe).to_hex()[..32]);
        assert_eq!(current(), current());
    }
}
