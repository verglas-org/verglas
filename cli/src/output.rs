//! Human-readable and JSON output helpers shared by CLI commands.

use serde::Serialize;
use std::io::{self, Write};

/// Errors while rendering command output.
#[derive(Debug, thiserror::Error)]
pub enum OutputError {
    /// stdout could not be written.
    #[error("failed to write command output: {0}")]
    WriteFailed(#[from] io::Error),
}

/// Serializes `value` as pretty JSON when `json` is set, otherwise uses `render`.
pub fn emit<T, F>(value: &T, json: bool, render: F) -> Result<(), OutputError>
where
    T: Serialize,
    F: FnOnce(&T) -> Result<(), OutputError>,
{
    if json {
        let encoded = serde_json::to_string_pretty(value)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        writeln!(io::stdout(), "{encoded}")?;
        return Ok(());
    }

    render(value)
}
