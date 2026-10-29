use std::fmt;

pub const PG_IDENT_MAX: usize = 63;
pub const DIGEST_HEX: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PgIdent(String);

impl PgIdent {
    #[must_use]
    pub fn bounded(full: &str) -> Self {
        if full.len() <= PG_IDENT_MAX {
            return Self(full.to_owned());
        }
        let mut cut = PG_IDENT_MAX - DIGEST_HEX - 1;
        while !full.is_char_boundary(cut) {
            cut -= 1;
        }
        let digest = blake3::hash(full.as_bytes()).to_hex();
        Self(format!("{}_{}", &full[..cut], &digest[..DIGEST_HEX]))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for PgIdent {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PgIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[must_use]
pub fn tofu_state_schema(schema: &str, template: &str) -> PgIdent {
    PgIdent::bounded(&format!("{schema}_{template}_states"))
}

#[must_use]
pub fn state_table(template: &str) -> PgIdent {
    PgIdent::bounded(&format!("{template}_states"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    const NS: &str = "pangea_pleme-io-github";

    const COLLIDING: &[&[&str]] = &[
        &[
            "pleme-io-github-repos-akeyless-custom-server-kustomize-patches",
            "pleme-io-github-repos-akeyless-custom-server-ui-rotate",
            "pleme-io-github-repos-akeyless-custom-server-w-puppeteer",
        ],
        &[
            "pleme-io-github-repos-akeyless-java-cloud-id",
            "pleme-io-github-repos-akeyless-java-cloud-id-lightweight",
        ],
        &[
            "pleme-io-github-repos-akeyless-k8s-auth-examples",
            "pleme-io-github-repos-akeyless-k8s-auth-terraform",
        ],
        &[
            "pleme-io-github-repos-akeyless-tf-cloud-docker-agent",
            "pleme-io-github-repos-akeyless-tf-cloud-intro",
        ],
        &[
            "pleme-io-github-repos-crossplane-akeyless",
            "pleme-io-github-repos-crossplane-akeyless-gen",
        ],
        &[
            "pleme-io-github-repos-pleme-actions-shared",
            "pleme-io-github-repos-pleme-actions-shared-go",
        ],
        &[
            "pleme-io-github-repos-python-synthesizer",
            "pleme-io-github-repos-python-synthesizer-go",
        ],
        &[
            "pleme-io-github-repos-terraform-provider-akeyless",
            "pleme-io-github-repos-terraform-provider-akeyless-gen",
        ],
    ];

    fn truncated(full: &str) -> &str {
        &full[..full.len().min(PG_IDENT_MAX)]
    }

    #[test]
    fn the_nine_colliding_leaves_collided_under_truncation() {
        let mut lost = 0;
        for group in COLLIDING {
            let names: BTreeSet<String> = group
                .iter()
                .map(|t| truncated(&format!("{NS}_{t}_states")).to_owned())
                .collect();
            assert_eq!(names.len(), 1, "{group:?} should truncate to one name");
            lost += group.len() - 1;
        }
        assert_eq!(lost, 9);
    }

    #[test]
    fn the_nine_colliding_leaves_map_to_distinct_identifiers() {
        for group in COLLIDING {
            let idents: BTreeSet<PgIdent> =
                group.iter().map(|t| tofu_state_schema(NS, t)).collect();
            assert_eq!(idents.len(), group.len(), "{group:?}");
            for i in &idents {
                assert!(i.as_str().len() <= PG_IDENT_MAX, "{i}");
            }
        }
    }

    #[test]
    fn every_pleme_io_github_repos_leaf_has_its_own_identifier() {
        let leaves: Vec<&str> = include_str!("../tests/fixtures/pleme-io-github-repos-leaves.txt")
            .lines()
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(leaves.len(), 1026);
        let before: BTreeSet<String> = leaves
            .iter()
            .map(|t| truncated(&format!("{NS}_{t}_states")).to_owned())
            .collect();
        assert_eq!(before.len(), 1017);
        let over = leaves
            .iter()
            .filter(|t| format!("{NS}_{t}_states").len() > PG_IDENT_MAX)
            .count();
        assert_eq!(over, 541);
        let after: BTreeSet<PgIdent> = leaves.iter().map(|t| tofu_state_schema(NS, t)).collect();
        assert_eq!(after.len(), 1026);
        assert!(after.iter().all(|i| i.as_str().len() <= PG_IDENT_MAX));
    }

    #[test]
    fn existing_identifiers_are_unchanged() {
        for (schema, template, live) in [
            (
                NS,
                "pleme-io-github-repos",
                "pangea_pleme-io-github_pleme-io-github-repos_states",
            ),
            (
                "pangea_cloudflare-pleme",
                "cloudflare-pleme",
                "pangea_cloudflare-pleme_cloudflare-pleme_states",
            ),
            (
                "pangea_rio-infra",
                "rio-zot-cloudflare-tunnel",
                "pangea_rio-infra_rio-zot-cloudflare-tunnel_states",
            ),
        ] {
            assert_eq!(tofu_state_schema(schema, template).as_str(), live);
        }
        assert_eq!(
            state_table("cloudflare-pleme").as_str(),
            "cloudflare-pleme_states"
        );
    }

    #[test]
    fn a_long_identifier_is_a_prefix_an_underscore_and_ten_hex() {
        let full = format!("{NS}_pleme-io-github-repos-akeyless-java-cloud-id-lightweight_states");
        let i = PgIdent::bounded(&full);
        assert_eq!(i.as_str().len(), PG_IDENT_MAX);
        let (head, digest) = i.as_str().split_at(PG_IDENT_MAX - DIGEST_HEX);
        assert_eq!(head, format!("{}_", &full[..PG_IDENT_MAX - DIGEST_HEX - 1]));
        assert!(digest.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(i, PgIdent::bounded(&full));
    }

    #[test]
    fn a_multibyte_name_is_cut_on_a_char_boundary() {
        let full = format!("{}é{}", "a".repeat(51), "b".repeat(40));
        let i = PgIdent::bounded(&full);
        assert!(i.as_str().len() <= PG_IDENT_MAX);
        assert!(i.as_str().starts_with(&"a".repeat(51)));
    }

    #[test]
    fn display_and_as_ref_render_the_identifier() {
        let i = tofu_state_schema(NS, "pleme-io-github-repos-crossplane-akeyless-gen");
        assert_eq!(i.to_string(), i.as_str());
        assert_eq!(AsRef::<str>::as_ref(&i), i.as_str());
        assert!(i
            .as_str()
            .starts_with("pangea_pleme-io-github_pleme-io-github-repos"));
    }

    #[test]
    fn the_boundary_is_sixty_three_bytes() {
        let at = "x".repeat(PG_IDENT_MAX);
        assert_eq!(PgIdent::bounded(&at).as_str(), at);
        let over = "x".repeat(PG_IDENT_MAX + 1);
        assert_ne!(PgIdent::bounded(&over).as_str(), &over[..PG_IDENT_MAX]);
        assert_eq!(PgIdent::bounded(&over).as_str().len(), PG_IDENT_MAX);
    }

    fn k8s_name() -> impl Strategy<Value = String> {
        "[a-z0-9]([a-z0-9.-]{0,70}[a-z0-9])?"
    }

    proptest! {
        #[test]
        fn identifiers_fit_and_short_names_are_unchanged(ns in k8s_name(), t in k8s_name()) {
            let schema = format!("pangea_{ns}");
            let full = format!("{schema}_{t}_states");
            let i = tofu_state_schema(&schema, &t);
            prop_assert!(i.as_str().len() <= PG_IDENT_MAX);
            if full.len() <= PG_IDENT_MAX {
                prop_assert_eq!(i.as_str(), full.as_str());
            }
            let table = state_table(&t);
            prop_assert!(table.as_str().len() <= PG_IDENT_MAX);
        }

        #[test]
        fn distinct_pairs_map_to_distinct_identifiers(
            a in (k8s_name(), k8s_name()),
            b in (k8s_name(), k8s_name()),
        ) {
            prop_assume!(a != b);
            prop_assert_ne!(
                tofu_state_schema(&format!("pangea_{}", a.0), &a.1),
                tofu_state_schema(&format!("pangea_{}", b.0), &b.1)
            );
        }

        #[test]
        fn templates_sharing_a_long_prefix_stay_distinct(
            ns in k8s_name(),
            stem in "[a-z0-9]{60}",
            x in "[a-z0-9-]{0,20}",
            y in "[a-z0-9-]{0,20}",
        ) {
            prop_assume!(x != y);
            let schema = format!("pangea_{ns}");
            let a = tofu_state_schema(&schema, &format!("{stem}{x}"));
            let b = tofu_state_schema(&schema, &format!("{stem}{y}"));
            prop_assert!(a.as_str().len() <= PG_IDENT_MAX);
            prop_assert!(b.as_str().len() <= PG_IDENT_MAX);
            prop_assert_ne!(a, b);
            prop_assert_ne!(state_table(&format!("{stem}{x}")), state_table(&format!("{stem}{y}")));
        }
    }
}
