//! Drive the public Verglas WAL protocol for the durability benchmark.
//!
//! This deliberately imports the engine's request and response types. It never
//! recreates, guesses at, or loosely parses the WAL wire format.

use std::io::{self, BufRead};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use reqwest::{Client, StatusCode};
use serde_json::json;
use sha2::{Digest, Sha256};
use verglas_safekeeper::{WalRequest, WalResponse};

const CHUNK_BYTES: usize = 8 * 1024 * 1024;
const SEGMENT_BYTES: u64 = 16 * 1024 * 1024;

/// Command line inputs intentionally restricted to the frozen acceptance workload.
#[derive(Debug, Parser)]
struct Args {
    /// Comma-separated, explicitly routed cache-node WAL ingress URLs.
    #[arg(long, value_delimiter = ',')]
    endpoints: Vec<String>,
    /// Tenant passed unchanged to the public WAL route.
    #[arg(long)]
    tenant: String,
    /// Timeline passed unchanged to the public WAL route.
    #[arg(long)]
    timeline: String,
    /// Exact logical WAL bytes to append.
    #[arg(long)]
    bytes: usize,
    /// Number of durable appends before the coordinator kills the leader.
    #[arg(long)]
    pause_after_appends: usize,
}

/// One client that retries an identical canonical request only through other ingresses.
struct WalClient {
    http: Client,
    endpoints: Vec<String>,
    tenant: String,
    timeline: String,
}

impl WalClient {
    /// Constructs a fail-closed client with nonempty endpoint and route identities.
    fn new(args: &Args) -> Result<Self> {
        if args.endpoints.is_empty() || args.tenant.is_empty() || args.timeline.is_empty() {
            bail!("endpoints, tenant, and timeline are required");
        }
        Ok(Self {
            http: Client::builder().build()?,
            endpoints: args
                .endpoints
                .iter()
                .map(|endpoint| endpoint.trim_end_matches('/').to_owned())
                .collect(),
            tenant: args.tenant.clone(),
            timeline: args.timeline.clone(),
        })
    }

    /// Submits one pre-encoded canonical operation and strictly decodes a full response.
    async fn submit(&self, request: &WalRequest) -> Result<WalResponse> {
        let body = request.encode().context("encode canonical WAL request")?;
        let mut failures = Vec::new();
        for endpoint in &self.endpoints {
            let url = format!("{endpoint}/wal/v1/{}/{}", self.tenant, self.timeline);
            match self
                .http
                .post(&url)
                .header("content-type", "application/octet-stream")
                .body(body.clone())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    let bytes = response.bytes().await.context("read WAL response body")?;
                    return WalResponse::decode(&bytes).context("decode canonical WAL response");
                }
                Ok(response)
                    if response.status() == StatusCode::CONFLICT
                        || response.status() == StatusCode::SERVICE_UNAVAILABLE =>
                {
                    failures.push(format!("{url}: {}", response.status()));
                }
                Ok(response) => bail!(
                    "WAL ingress rejected request at {url}: {}",
                    response.status()
                ),
                Err(error) => failures.push(format!("{url}: {error}")),
            }
        }
        bail!(
            "no WAL ingress completed the canonical request: {}",
            failures.join("; ")
        )
    }
}

/// Constructs deterministic bytes so every full and post-restart read has one expected digest.
fn chunk(sequence: usize, length: usize) -> Vec<u8> {
    /// Advances a local xorshift generator without a mutable global source.
    fn next(value: &mut u64) -> u8 {
        *value ^= *value << 13;
        *value ^= *value >> 7;
        *value ^= *value << 17;
        *value as u8
    }
    let mut state = 0x9e37_79b9_7f4a_7c15_u64 ^ sequence as u64;
    (0..length).map(|_| next(&mut state)).collect()
}

/// Reads a coordinator acknowledgement from standard input, rejecting missing or altered control words.
fn wait_for(word: &str) -> Result<()> {
    let mut line = String::new();
    io::stdin()
        .lock()
        .read_line(&mut line)
        .context("read coordinator acknowledgement")?;
    if line.trim() != word {
        bail!("expected coordinator acknowledgement `{word}`");
    }
    Ok(())
}

/// Extracts an applied writer epoch and WAL start from the public response surface.
fn applied_writer(response: WalResponse) -> Result<(u64, u64)> {
    match response {
        WalResponse::Applied {
            writer_epoch: Some(epoch),
            wal_end: Some(end),
            ..
        } => Ok((epoch, end)),
        other => Err(anyhow!("writer operation returned {other:?}")),
    }
}

/// Extracts the committed tail from an applied non-writer operation.
fn applied_end(response: WalResponse) -> Result<u64> {
    match response {
        WalResponse::Applied {
            wal_end: Some(end), ..
        } => Ok(end),
        other => Err(anyhow!("timeline operation returned {other:?}")),
    }
}

/// Reads the complete committed workload and checks it against the exact append digest.
async fn verify_read(
    client: &WalClient,
    bytes: usize,
    expected: &[u8],
    minimum_index: u64,
) -> Result<bool> {
    match client
        .submit(&WalRequest::ReadWal {
            from: 0,
            to: bytes as u64,
            minimum_index,
        })
        .await?
    {
        WalResponse::WalBytes { payload } => Ok(payload == expected),
        other => Err(anyhow!("WAL read returned {other:?}")),
    }
}

/// Waits only for the committed archive checkpoint that covers every complete segment.
async fn wait_for_archive(client: &WalClient, bytes: usize, minimum_index: u64) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        match client.submit(&WalRequest::Status { minimum_index }).await? {
            WalResponse::Status { archive_lsn, .. } if archive_lsn >= bytes as u64 => return Ok(()),
            WalResponse::Status { .. } if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            WalResponse::Status { archive_lsn, .. } => {
                bail!("archive checkpoint stopped at {archive_lsn}, below {bytes}");
            }
            other => bail!("WAL status returned {other:?}"),
        }
    }
}

/// Performs the fixed append, kill, immediate-read, restart-read, and archival protocol.
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.bytes != 256 * 1024 * 1024 || args.bytes % CHUNK_BYTES != 0 {
        bail!("the durability protocol requires exactly 256 MiB in 8 MiB appends");
    }
    if args.pause_after_appends == 0 || args.pause_after_appends >= args.bytes / CHUNK_BYTES {
        bail!("pause-after-appends must fall during the WAL append stream");
    }
    let client = WalClient::new(&args)?;
    let opened = client
        .submit(&WalRequest::OpenTimeline {
            request_id: 0x1350_0001,
            start_lsn: 0,
        })
        .await?;
    let open_end = applied_end(opened)?;
    if open_end != 0 {
        bail!("timeline did not open at LSN zero");
    }
    let (writer_epoch, mut lsn) = applied_writer(
        client
            .submit(&WalRequest::AcquireWriter {
                request_id: 0x1350_0002,
                writer: "tpch-durability".to_owned(),
            })
            .await?,
    )?;
    let mut expected = Vec::with_capacity(args.bytes);
    let mut minimum_index = 0;
    for sequence in 0..(args.bytes / CHUNK_BYTES) {
        let payload = chunk(sequence, CHUNK_BYTES);
        let response = client
            .submit(&WalRequest::Append {
                request_id: 0x1350_1000 + sequence as u128,
                writer_epoch,
                start_lsn: lsn,
                payload: payload.clone(),
            })
            .await?;
        let WalResponse::Applied {
            index,
            wal_end: Some(end),
            ..
        } = response
        else {
            bail!("append did not return a committed WAL boundary");
        };
        if end != lsn + payload.len() as u64 {
            bail!("append committed an unexpected WAL boundary");
        }
        expected.extend_from_slice(&payload);
        lsn = end;
        minimum_index = index;
        if sequence + 1 == args.pause_after_appends {
            println!("READY_TO_KILL");
            wait_for("continue")?;
        }
    }
    let immediate = verify_read(&client, args.bytes, &expected, minimum_index).await?;
    println!("READY_TO_RESTART");
    wait_for("continue")?;
    let after_restart = verify_read(&client, args.bytes, &expected, minimum_index).await?;
    wait_for_archive(&client, args.bytes, minimum_index).await?;
    let digest = hex::encode(Sha256::digest(&expected));
    println!(
        "{}",
        json!({
            "bytes": args.bytes,
            "leader_killed_during_append": true,
            "immediate_read_checksum_match": immediate,
            "post_restart_checksum_match": after_restart,
            "archive_checkpoint_committed": true,
            "archived_objects": args.bytes as u64 / SEGMENT_BYTES,
            "archived_bytes": args.bytes,
            "checksum": digest,
        })
    );
    Ok(())
}
