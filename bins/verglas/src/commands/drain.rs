//! `verglas drain`: gracefully drain the LOCAL daemon (issue #31).
//!
//! The CLI operates on the local node only — it POSTs `POST /admin/drain` to
//! this machine's admin endpoint and never resolves or targets other nodes
//! (cluster operations are not a CLI concern, #288). The daemon marks itself
//! `draining`: it gossips the state so peers shed its ownership to their
//! successors, keeps serving what it holds as a donor while they warm from it,
//! then exits so the ring rebalances — no client-visible error spike.

use crate::admin_client::AdminClient;
use crate::cli::DrainArgs;
use verglas_core::admin::DrainRequest;

/// Errors specific to `verglas drain` beyond the admin transport errors.
#[derive(Debug, thiserror::Error)]
pub enum DrainError {
    /// The `--timeout` value could not be parsed.
    #[error("invalid --timeout `{0}`: expected e.g. `10m`, `30s`, `1h`, or a plain seconds count")]
    BadTimeout(String),
}

/// Runs `verglas drain [--timeout <dur>]`.
///
/// POSTs `POST /admin/drain` to the local daemon at `endpoint` with the
/// optional timeout. The drain is asynchronous: the ack confirms the daemon is
/// now `draining`; it exits once its keys are re-owned warm or the timeout
/// elapses, so `verglas status` will show it leave.
pub async fn run(
    endpoint: &str,
    args: &DrainArgs,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let timeout_secs = match &args.timeout {
        Some(raw) => {
            Some(parse_duration_secs(raw).ok_or_else(|| DrainError::BadTimeout(raw.clone()))?)
        }
        None => None,
    };

    let client = AdminClient::new(endpoint)?;
    let ack = client.drain(&DrainRequest { timeout_secs }).await?;

    crate::output::emit(&ack, json, |ack| {
        crate::output::print_key_value_table(
            ("Field", "Value"),
            &[
                ("node", ack.node_id.clone()),
                ("state", ack.state.clone()),
                ("timeout_secs", ack.timeout_secs.to_string()),
            ],
            false,
        )
    })?;
    Ok(())
}

/// Parses a human drain duration into seconds: a plain integer is seconds, or a
/// single `s`/`m`/`h` suffix scales it (`30s`, `10m`, `1h`). Returns `None` on
/// anything else. Pure, so it is unit-tested directly.
fn parse_duration_secs(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let (digits, scale) = match raw.chars().last()? {
        's' => (&raw[..raw.len() - 1], 1),
        'm' => (&raw[..raw.len() - 1], 60),
        'h' => (&raw[..raw.len() - 1], 3600),
        c if c.is_ascii_digit() => (raw, 1),
        _ => return None,
    };
    digits.parse::<u64>().ok()?.checked_mul(scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_secs_handles_suffixes_and_plain_seconds() {
        assert_eq!(parse_duration_secs("30s"), Some(30));
        assert_eq!(parse_duration_secs("10m"), Some(600));
        assert_eq!(parse_duration_secs("1h"), Some(3600));
        assert_eq!(parse_duration_secs("45"), Some(45));
        assert_eq!(parse_duration_secs("  2m "), Some(120));
    }

    #[test]
    fn parse_duration_secs_rejects_garbage() {
        assert_eq!(parse_duration_secs(""), None);
        assert_eq!(parse_duration_secs("m"), None);
        assert_eq!(parse_duration_secs("10x"), None);
        assert_eq!(parse_duration_secs("1.5m"), None);
        assert_eq!(parse_duration_secs("-5"), None);
    }
}
