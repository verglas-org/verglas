//! Lakekeeper extension hooks owned by Verglas. Catalog mutations are delivered
//! directly to each tenant cache member without a broker or cloud relay.

use std::{
    fmt::{Debug, Formatter},
    time::Duration,
};

use anyhow::{Context, bail};
/// Comma-separated cache event receivers on the tenant network.
pub const CACHE_EVENT_URLS_ENV: &str = "VERGLAS_CACHE_EVENT_URLS";
/// Shared bearer presented to every cache event receiver.
pub const CACHE_EVENT_TOKEN_ENV: &str = "VERGLAS_CACHE_EVENT_TOKEN";

const RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(2),
];

/// A Lakekeeper CloudEvent sink that invalidates every member of one cache ring.
#[derive(Clone)]
pub struct VerglasCachePublisher {
    client: reqwest::Client,
    urls: Vec<String>,
    token: String,
}

impl Debug for VerglasCachePublisher {
    /// Omits the bearer while retaining useful endpoint diagnostics.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerglasCachePublisher")
            .field("urls", &self.urls)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl VerglasCachePublisher {
    /// Builds a mandatory direct publisher from explicit tenant endpoints.
    pub fn new(urls: Vec<String>, token: String) -> anyhow::Result<Self> {
        if urls.is_empty() {
            bail!("{CACHE_EVENT_URLS_ENV} must contain at least one cache endpoint");
        }
        if urls.iter().any(|url| url.trim().is_empty()) {
            bail!("{CACHE_EVENT_URLS_ENV} contains an empty cache endpoint");
        }
        if token.is_empty() {
            bail!("{CACHE_EVENT_TOKEN_ENV} must not be empty");
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .context("build the cache event HTTP client")?,
            urls,
            token,
        })
    }

    /// Reads the required deployment contract without a disabled or polling mode.
    pub fn from_env() -> anyhow::Result<Self> {
        let urls = std::env::var(CACHE_EVENT_URLS_ENV)
            .with_context(|| format!("{CACHE_EVENT_URLS_ENV} is required"))?
            .split(',')
            .map(str::trim)
            .map(ToOwned::to_owned)
            .collect();
        let token = std::env::var(CACHE_EVENT_TOKEN_ENV)
            .with_context(|| format!("{CACHE_EVENT_TOKEN_ENV} is required"))?;
        Self::new(urls, token)
    }

    /// Selects events that can change the authoritative table pointer or identity.
    fn is_table_mutation(event_type: &str) -> bool {
        matches!(
            event_type,
            "updateTable" | "dropTable" | "registerTable" | "createTable" | "renameTable"
        )
    }

    /// Sends one event to one cache, retrying bounded transport failures visibly.
    async fn deliver(&self, url: &str, event: &serde_json::Value) -> anyhow::Result<()> {
        let mut last_error = None;
        for attempt in 0..=RETRY_DELAYS.len() {
            let result = self
                .client
                .post(url)
                .bearer_auth(&self.token)
                .header("content-type", "application/cloudevents+json")
                .json(event)
                .send()
                .await
                .context("send cache mutation event")
                .and_then(|response| {
                    if response.status().is_success() {
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!(
                            "cache event endpoint {url} returned {}",
                            response.status()
                        ))
                    }
                });
            match result {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
            if let Some(delay) = RETRY_DELAYS.get(attempt) {
                tokio::time::sleep(*delay).await;
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("cache event delivery was not attempted")))
    }
}

impl VerglasCachePublisher {
    /// Fans a committed table mutation to every configured cache member.
    pub async fn publish(&self, event_type: &str, event: &serde_json::Value) -> anyhow::Result<()> {
        if !Self::is_table_mutation(event_type) {
            return Ok(());
        }
        futures::future::try_join_all(self.urls.iter().map(|url| self.deliver(url, event))).await?;
        Ok(())
    }
}
