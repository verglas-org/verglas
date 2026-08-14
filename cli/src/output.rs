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

/// Renders a two-column table or JSON document depending on `json`.
pub fn print_key_value_table(
    headers: (&str, &str),
    rows: &[(&str, String)],
    json: bool,
) -> Result<(), OutputError> {
    if json {
        let mut object = serde_json::Map::new();
        for (key, value) in rows {
            object.insert((*key).to_owned(), serde_json::Value::String(value.clone()));
        }
        let encoded = serde_json::to_string_pretty(&serde_json::Value::Object(object))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        writeln!(io::stdout(), "{encoded}")?;
        return Ok(());
    }

    let key_width = rows
        .iter()
        .map(|(key, _)| key.chars().count())
        .chain(std::iter::once(headers.0.chars().count()))
        .max()
        .unwrap_or(headers.0.chars().count());

    writeln!(io::stdout(), "{:<key_width$}  {}", headers.0, headers.1)?;
    for (key, value) in rows {
        writeln!(io::stdout(), "{:<key_width$}  {value}", key)?;
    }

    Ok(())
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
