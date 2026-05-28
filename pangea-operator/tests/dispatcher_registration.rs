//! Verify pangea-operator registers TargetKind into the
//! gen-platform fleet catalog. pangea-operator is the NINTH
//! consumer class adopting the typed-dispatcher catamorphism
//! (after gen / caixa / wasm-platform / cofre / shigoto /
//! engenho / magma / kura).
//!
//! TargetKind enumerates which CRD kinds pangea's compliance
//! binding may target — InfrastructureTemplate / Infrastructure
//! Flow / ImagePipeline / PackerBuild.

use gen_platform::{catalog, TypedDispatcherTrait};
use pangea_operator::crd::compliance_binding::TargetKind;

#[test]
fn target_kind_registers_into_catalog() {
    let entry = catalog::by_label("pangea.target-kind")
        .expect("TargetKind must register into the fleet catalog");
    assert_eq!((entry.variant_count)(), 4);
}

#[test]
fn target_kind_variants() {
    let kinds = TargetKind::variant_kinds();
    assert_eq!(
        kinds,
        vec![
            "infrastructure-template",
            "infrastructure-flow",
            "image-pipeline",
            "packer-build"
        ]
    );
}

#[test]
fn target_kind_round_trip() {
    use std::str::FromStr;
    for v in [
        TargetKind::InfrastructureTemplate,
        TargetKind::InfrastructureFlow,
        TargetKind::ImagePipeline,
        TargetKind::PackerBuild,
    ] {
        let k = v.discriminant();
        let back = TargetKind::from_str(k)
            .unwrap_or_else(|_| panic!("FromStr must accept own discriminant: {k}"));
        assert_eq!(back.discriminant(), v.discriminant());
    }
}

#[test]
fn target_kind_predicates() {
    let t = TargetKind::PackerBuild;
    assert!(t.is_packer_build());
    assert!(!t.is_infrastructure_template());
    assert!(!t.is_image_pipeline());
}
