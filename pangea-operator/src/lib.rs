// Drift floor: every previous review pass cleaned a wave of unused imports
// (R2 of the 2026-05-03 review took 45 → 0). Promote the warnings to hard
// errors so the next 45 don't accumulate silently. New unused imports
// become a build break — fix them at the keystroke that introduced them.
#![deny(unused_imports)]

//! Pangea Operator - Rust-based Kubernetes operator for infrastructure management.
//!
//! This crate provides a Kubernetes operator that manages infrastructure templates
//! using Pangea Ruby DSL compiled to OpenTofu configurations with PostgreSQL state backend.
//!
//! # Features
//!
//! - **CRDs**: InfrastructureTemplate and PangeaNamespace custom resources
//! - **PostgreSQL Backend**: State storage using OpenTofu's `pg` backend
//! - **GraphQL API**: Interactive queries and subscriptions (optional, `graphql` feature)
//! - **gRPC API**: High-performance service interface (optional, `grpc` feature)
//! - **Observability**: Prometheus metrics and OpenTelemetry tracing
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                   Pangea Operator                        │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
//! │  │ Controller  │  │ API Server  │  │ Compiler Sidecar│  │
//! │  │ (kube-rs)   │  │ GraphQL/gRPC│  │ (Ruby Pangea)   │  │
//! │  └──────┬──────┘  └──────┬──────┘  └────────┬────────┘  │
//! │         │                │                  │           │
//! │         v                v                  v           │
//! │  ┌─────────────────────────────────────────────────┐    │
//! │  │           Core Services Layer                   │    │
//! │  │  • State Machine Reconciler                    │    │
//! │  │  • OpenTofu Executor                           │    │
//! │  │  • PostgreSQL Backend                          │    │
//! │  └─────────────────────────────────────────────────┘    │
//! └─────────────────────────────────────────────────────────┘
//! ```

pub mod backend;
pub mod chart_values;
// Typed shikumi progressive-discovery config surface (the operator's whole
// env surface as one TieredConfig). See src/config.rs.
pub mod config;
pub mod crd;
pub mod controller;
pub mod error;
pub mod executor;
pub mod executor_migration;
pub mod leader;
pub mod observability;
pub mod ruby;
// UTF-8-safe truncation, shared by every call site that caps
// externally-sourced text (apply-error output, inline DSL previews) for a
// status field / k8s Event / GraphQL response. See src/text_util.rs.
pub mod text_util;

#[cfg(feature = "graphql")]
pub mod api;

// Re-exports for convenience
pub use crd::{
    AmiTest, ComplianceBinding, ComplianceSchedule, ImagePipeline, InfrastructureTemplate,
    PackerBuild, PangeaNamespace, SynthesizerFormat,
};
pub use error::{Error, Result};

#[cfg(feature = "graphql")]
pub use api::graphql::{build_schema, graphql_router, run_graphql_server, PangeaSchema};
