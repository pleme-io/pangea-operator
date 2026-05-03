//! Sub-modules carved out of `controller/template_controller.rs` during
//! R6 (2026-05-03 review pass) to bring the monolith from 3329 → ~3100
//! lines and group cohesive helpers in their own files.
//!
//! Each sub-module is a self-contained helper group consumed only by
//! `template_controller`; nothing here is part of the public crate
//! surface. Future R-passes can pull more modules out of
//! `template_controller.rs` (status helpers, reactive policy, reconcile-
//! cycle receipts) by following the same shape.

pub mod events;
pub mod finalizer;
pub mod provider_creds;
pub mod reactive_policy;
pub mod status;
