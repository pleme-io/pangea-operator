use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AddressError {
    #[error("empty address")]
    Empty,
    #[error("unterminated index in `{0}`")]
    UnterminatedIndex(String),
    #[error("unterminated string in `{0}`")]
    UnterminatedString(String),
    #[error("empty segment in `{0}`")]
    EmptySegment(String),
    #[error("unexpected `{found}` after an index in `{address}`")]
    TrailingAfterIndex { address: String, found: char },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum LeafKey {
    Keyed(String),
    Module(String),
    Address(String),
}

impl LeafKey {
    #[must_use]
    pub fn raw(&self) -> &str {
        match self {
            Self::Keyed(s) | Self::Module(s) | Self::Address(s) => s,
        }
    }

    #[must_use]
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Keyed(_) => "keyed",
            Self::Module(_) => "module",
            Self::Address(_) => "address",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Step {
    name: String,
    key: Option<String>,
}

fn parse_steps(address: &str) -> Result<Vec<Step>, AddressError> {
    if address.is_empty() {
        return Err(AddressError::Empty);
    }
    let chars: Vec<char> = address.chars().collect();
    let mut steps = Vec::new();
    let mut i = 0;
    loop {
        let start = i;
        while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
            i += 1;
        }
        let name: String = chars[start..i].iter().collect();
        if name.is_empty() {
            return Err(AddressError::EmptySegment(address.to_string()));
        }
        let mut key = None;
        if i < chars.len() && chars[i] == '[' {
            i += 1;
            let mut buf = String::new();
            if i < chars.len() && chars[i] == '"' {
                i += 1;
                loop {
                    match chars.get(i) {
                        None => return Err(AddressError::UnterminatedString(address.to_string())),
                        Some('\\') => {
                            match chars.get(i + 1) {
                                Some(c) => buf.push(*c),
                                None => {
                                    return Err(AddressError::UnterminatedString(
                                        address.to_string(),
                                    ))
                                }
                            }
                            i += 2;
                        }
                        Some('"') => {
                            i += 1;
                            break;
                        }
                        Some(c) => {
                            buf.push(*c);
                            i += 1;
                        }
                    }
                }
                if chars.get(i) != Some(&']') {
                    return Err(AddressError::UnterminatedIndex(address.to_string()));
                }
                i += 1;
            } else {
                while i < chars.len() && chars[i] != ']' {
                    buf.push(chars[i]);
                    i += 1;
                }
                if i >= chars.len() {
                    return Err(AddressError::UnterminatedIndex(address.to_string()));
                }
                i += 1;
            }
            key = Some(buf);
        }
        steps.push(Step { name, key });
        match chars.get(i) {
            None => return Ok(steps),
            Some('.') => i += 1,
            Some(c) => {
                return Err(AddressError::TrailingAfterIndex {
                    address: address.to_string(),
                    found: *c,
                })
            }
        }
    }
}

pub fn leaf_key(address: &str) -> Result<LeafKey, AddressError> {
    let steps = parse_steps(address)?;
    if let Some(key) = steps.iter().find_map(|s| s.key.clone()) {
        return Ok(LeafKey::Keyed(key));
    }
    if steps.len() >= 2 && steps[0].name == "module" {
        return Ok(LeafKey::Module(format!("module.{}", steps[1].name)));
    }
    Ok(LeafKey::Address(address.to_string()))
}

pub fn strip_index(address: &str) -> Result<String, AddressError> {
    let steps = parse_steps(address)?;
    Ok(steps
        .iter()
        .map(|s| s.name.as_str())
        .collect::<Vec<_>>()
        .join("."))
}

#[must_use]
pub fn is_data(address: &str) -> bool {
    let rest = address
        .strip_prefix("module.")
        .and_then(|r| r.split_once('.').map(|(_, t)| t))
        .unwrap_or(address);
    address.starts_with("data.") || rest.starts_with("data.")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(a: &str) -> LeafKey {
        leaf_key(a).unwrap()
    }

    #[test]
    fn for_each_key_is_the_leaf() {
        assert_eq!(
            key("github_repository.repos[\"acude\"]"),
            LeafKey::Keyed("acude".into())
        );
    }

    #[test]
    fn count_index_is_the_leaf() {
        assert_eq!(key("aws_instance.web[3]"), LeafKey::Keyed("3".into()));
    }

    #[test]
    fn same_key_across_resource_types_is_one_leaf() {
        assert_eq!(
            key("github_repository.r[\"a\"]"),
            key("github_branch_protection.p[\"a\"]")
        );
    }

    #[test]
    fn module_key_wins_over_the_resource_key() {
        assert_eq!(
            key("module.net[\"east\"].aws_subnet.s[\"x\"]"),
            LeafKey::Keyed("east".into())
        );
    }

    #[test]
    fn unkeyed_module_is_the_top_level_instance() {
        assert_eq!(
            key("module.vpc.module.inner.aws_subnet.s"),
            LeafKey::Module("module.vpc".into())
        );
    }

    #[test]
    fn plain_resource_is_its_own_leaf() {
        assert_eq!(
            key("aws_s3_bucket.logs"),
            LeafKey::Address("aws_s3_bucket.logs".into())
        );
    }

    #[test]
    fn quoted_key_may_hold_dots_brackets_and_escaped_quotes() {
        assert_eq!(
            key("x.y[\"a.b[1]\\\"c\"]"),
            LeafKey::Keyed("a.b[1]\"c".into())
        );
    }

    #[test]
    fn malformed_addresses_are_errors_never_guesses() {
        assert_eq!(leaf_key(""), Err(AddressError::Empty));
        assert!(matches!(
            leaf_key("a.b[\"x"),
            Err(AddressError::UnterminatedString(_))
        ));
        assert!(matches!(
            leaf_key("a.b[1"),
            Err(AddressError::UnterminatedIndex(_))
        ));
        assert!(matches!(leaf_key("a..b"), Err(AddressError::EmptySegment(_))));
        assert!(matches!(
            leaf_key("a.b[1]c"),
            Err(AddressError::TrailingAfterIndex { .. })
        ));
    }

    #[test]
    fn the_key_function_is_total_over_well_formed_addresses() {
        for a in [
            "a.b",
            "a.b[0]",
            "module.m.a.b",
            "module.m[\"k\"].a.b",
            "data.x.y",
            "module.m.data.x.y[\"k\"]",
        ] {
            assert!(leaf_key(a).is_ok(), "{a}");
        }
    }

    #[test]
    fn strip_index_drops_every_key() {
        assert_eq!(
            strip_index("module.m[\"k\"].a.b[2]").unwrap(),
            "module.m.a.b"
        );
    }

    #[test]
    fn data_addresses_are_recognised() {
        assert!(is_data("data.aws_ami.x"));
        assert!(is_data("module.m.data.aws_ami.x"));
        assert!(!is_data("aws_ami.x"));
    }
}
