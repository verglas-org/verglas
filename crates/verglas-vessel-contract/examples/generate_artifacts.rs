//! Regenerates checked-in consumer artifacts from the Rust contract.

use std::fs;
use std::path::PathBuf;

use verglas_vessel_contract::VesselManifest;

/// Writes the JSON Schema and TypeScript declaration artifacts.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let artifacts = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("artifacts");
    fs::create_dir_all(&artifacts)?;
    fs::write(
        artifacts.join("vessel.schema.json"),
        format!("{}\n", VesselManifest::json_schema_pretty()?),
    )?;
    fs::write(
        artifacts.join("index.d.ts"),
        VesselManifest::typescript_declarations(),
    )?;
    Ok(())
}
