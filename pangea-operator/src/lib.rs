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
pub mod crd;
pub mod controller;
pub mod error;
pub mod executor;
pub mod observability;

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
