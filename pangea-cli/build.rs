//! Build script for registering cynic GraphQL schema.

fn main() {
    println!("cargo:rerun-if-changed=schema.graphql");

    cynic_codegen::register_schema("pangea")
        .from_sdl_file("schema.graphql")
        .expect("Failed to register schema")
        .as_default()
        .expect("Failed to set default schema");
}
