//! Regenerate the JSON Schema and TypeScript consumer artifacts.

use std::{fs, path::PathBuf};

use verglas_microvm_contract::MicroVmStack;

/// Generate checked-in artifacts from the canonical Rust contract types.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("artifacts");
    fs::write(
        directory.join("microvm-stack.schema.json"),
        format!("{}\n", MicroVmStack::json_schema_pretty()?),
    )?;
    fs::write(
        directory.join("index.d.ts"),
        MicroVmStack::typescript_declarations(),
    )?;
    Ok(())
}
