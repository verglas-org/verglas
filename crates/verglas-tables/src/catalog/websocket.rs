//! The websocket transport for the catalog change feed: the second change-feed
//! implementation after polling (#47), and the default when the upstream is the
//! Verglas catalog service.
//!
//! # Transport selection (no config knob)
//!
//! At startup and after failures the daemon attempts a websocket upgrade at
//! `<catalog origin>/v1/catalog/feed` with the same bearer auth it uses for
//! catalog requests. A `101` means the upstream is the Verglas catalog and the
//! websocket is the feed; anything else (a `404`, a non-`101` response, a
//! connect error — i.e. a third-party catalog) falls back to polling, which is
//! then the only mode, and the upgrade is retried on a fixed interval.
//!
//! # While connected
//!
//! Each `change` frame drives the same downstream handling the poller drives: a
//! targeted pointer read for the named table, diffed and emitted (see
//! [`super::watcher::refresh_table`]). The last-seen event sequence is kept in
//! this task's memory and replayed on reconnect (in-memory per process is
//! enough for v1 — a resync recovers a cursor the server has aged out; the
//! daemon's cache dir would be the natural home for cross-restart persistence).
//! On a socket drop the task reconnects with exponential backoff (cap ~1 min),
//! re-subscribing from the last-seen sequence; a catch-up polling pass on each
//! reconnect covers the gap.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::feed::{ClientMessage, FeedAction, FeedState, ServerMessage};
use super::watcher::{Shared, poll_once, refresh_table};
use super::{CatalogSource, WatcherOptions};

/// The feed endpoint appended to the catalog origin.
const FEED_PATH: &str = "/v1/catalog/feed";

/// How long to poll before retrying the upgrade after a fallback (third-party
/// catalog). A sane fixed interval — not a config knob.
const UPGRADE_RETRY_INTERVAL: Duration = Duration::from_secs(300);

/// Reconnect backoff floor after a confirmed feed drops.
const RECONNECT_MIN: Duration = Duration::from_secs(1);

/// Reconnect backoff cap after a confirmed feed drops.
const RECONNECT_MAX: Duration = Duration::from_secs(60);

/// The connected websocket stream type.
type FeedStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Static config for the websocket feed: the derived feed URL and the bearer
/// token reused from the catalog auth.
#[derive(Clone, Debug)]
pub struct WsFeedConfig {
    /// The `ws://`/`wss://` feed URL (`<origin>/v1/catalog/feed`).
    url: String,
    /// The bearer token sent on the upgrade, if any.
    bearer: Option<String>,
}

impl WsFeedConfig {
    /// Derives the feed URL from the catalog HTTP `uri`'s origin (scheme +
    /// authority), pairing `http`→`ws` and `https`→`wss`. Returns `None` when
    /// the uri has no usable `scheme://authority` — the daemon then polls only.
    pub fn from_catalog_uri(uri: &str, bearer: Option<String>) -> Option<WsFeedConfig> {
        Some(WsFeedConfig {
            url: feed_url(uri)?,
            bearer,
        })
    }
}

/// Builds the feed URL from a catalog uri: keep the origin, drop any path or
/// query, switch the scheme to its websocket counterpart, and append the feed
/// path.
fn feed_url(uri: &str) -> Option<String> {
    let (scheme, rest) = uri.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    if authority.is_empty() {
        return None;
    }
    let ws_scheme = match scheme {
        "https" => "wss",
        "http" => "ws",
        _ => return None,
    };
    Some(format!("{ws_scheme}://{authority}{FEED_PATH}"))
}

/// The feed supervisor: attempts the websocket upgrade, runs the connected
/// session, and falls back to polling when the endpoint is not upgradeable.
/// Never returns — resilience is this loop's job, mirroring the polling
/// watcher.
pub(crate) async fn run<S: CatalogSource>(
    source: S,
    options: WatcherOptions,
    shared: Arc<Shared>,
    config: WsFeedConfig,
) {
    // Last-seen event sequence, kept across reconnects (in-memory per process).
    let mut cursor: Option<i64> = None;
    // Whether the first successful seeding pass has run.
    let mut seeded = false;
    // Whether a websocket upgrade has ever succeeded (distinguishes a dropped
    // feed, which reconnects fast, from a third-party catalog, which polls).
    let mut confirmed = false;
    let mut backoff = RECONNECT_MIN;

    loop {
        match connect(&config).await {
            Ok(mut stream) => {
                confirmed = true;
                backoff = RECONNECT_MIN;
                // Seed the watched set on first attach; on reconnect this pass
                // catches up on anything missed while disconnected.
                seed_pass(&source, &options, &shared, &mut seeded).await;
                tracing::info!(url = %config.url, "catalog websocket feed attached");
                run_session(&mut stream, &source, &options, &shared, &mut cursor).await;
                tracing::warn!(url = %config.url, "catalog websocket feed dropped; reconnecting");
                // Back off before the outer loop reconnects; the catch-up poll
                // above covers the gap on the next attach.
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
            }
            Err(error) => {
                if confirmed {
                    // A previously-working feed failed to reconnect: keep
                    // polling to cover the gap during the backoff, then retry.
                    tracing::warn!(url = %config.url, %error, "catalog websocket reconnect failed; polling meanwhile");
                    poll_window(&source, &options, &shared, &mut seeded, backoff).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                } else {
                    // Never upgraded: a third-party catalog. Poll as the only
                    // mode and retry the upgrade after a fixed interval.
                    tracing::info!(url = %config.url, %error, "catalog does not offer a websocket feed; polling");
                    poll_window(
                        &source,
                        &options,
                        &shared,
                        &mut seeded,
                        UPGRADE_RETRY_INTERVAL,
                    )
                    .await;
                }
            }
        }
    }
}

/// Attempts the websocket upgrade with bearer auth. Returns the connected
/// stream on a `101`, or an error string (non-`101` response or connect
/// failure) the caller treats as "poll instead".
async fn connect(config: &WsFeedConfig) -> Result<FeedStream, String> {
    let mut request = config
        .url
        .as_str()
        .into_client_request()
        .map_err(|error| error.to_string())?;
    if let Some(token) = &config.bearer {
        let value =
            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|error| error.to_string())?;
        request.headers_mut().insert(AUTHORIZATION, value);
    }
    match tokio_tungstenite::connect_async(request).await {
        Ok((stream, _response)) => Ok(stream),
        Err(error) => Err(error.to_string()),
    }
}

/// Runs one seeding/catch-up poll pass and marks state seeded. A failed pass
/// keeps last-known state (mirroring the polling loop's outage handling).
async fn seed_pass<S: CatalogSource>(
    source: &S,
    options: &WatcherOptions,
    shared: &Shared,
    seeded: &mut bool,
) {
    if let Err(error) = poll_once(source, options, shared, *seeded).await {
        tracing::warn!(%error, "catalog feed seeding poll failed; keeping last-known state");
        return;
    }
    *seeded = true;
    shared.mark_seeded();
}

/// Polls on the configured interval for `window`, covering changes while the
/// websocket is unavailable. Used for the third-party fallback and to bridge
/// reconnect gaps.
async fn poll_window<S: CatalogSource>(
    source: &S,
    options: &WatcherOptions,
    shared: &Shared,
    seeded: &mut bool,
    window: Duration,
) {
    let deadline = Instant::now() + window;
    loop {
        seed_pass(source, options, shared, seeded).await;
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        let remaining = deadline - now;
        tokio::time::sleep(options.interval.min(remaining)).await;
    }
}

/// Drives one connected session until the socket drops: parse each text frame,
/// fold it through the [`FeedState`], and carry out the resulting action.
/// Updates `cursor` (the last-seen sequence) so a reconnect resumes correctly.
async fn run_session<S: CatalogSource>(
    stream: &mut FeedStream,
    source: &S,
    options: &WatcherOptions,
    shared: &Shared,
    cursor: &mut Option<i64>,
) {
    let mut state = FeedState::new(*cursor);
    loop {
        let text = match stream.next().await {
            Some(Ok(Message::Text(text))) => text,
            // Control frames and binary are ignored; the library answers pings.
            Some(Ok(_)) => continue,
            // Socket closed or errored: end the session so the caller reconnects.
            Some(Err(error)) => {
                tracing::warn!(%error, "catalog feed socket error");
                return;
            }
            None => return,
        };
        let message: ServerMessage = match serde_json::from_str(&text) {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(%error, frame = %text, "unparseable catalog feed frame; ignoring");
                continue;
            }
        };
        match state.on_message(message) {
            FeedAction::Subscribe(sub_cursor) => {
                if !send_subscribe(stream, sub_cursor).await {
                    return;
                }
            }
            FeedAction::Refresh(table) => {
                refresh_table(source, options, shared, &table).await;
                *cursor = state.last_seen();
            }
            FeedAction::ResyncThenResubscribe(resume) => {
                // Catch up with one polling pass, then resume the live stream.
                if let Err(error) = poll_once(source, options, shared, true).await {
                    tracing::warn!(%error, "catalog feed resync poll failed");
                }
                *cursor = state.last_seen();
                if !send_subscribe(stream, Some(resume)).await {
                    return;
                }
            }
        }
    }
}

/// Sends a `subscribe` frame; returns `false` if the socket send failed (the
/// caller then reconnects).
async fn send_subscribe(stream: &mut FeedStream, cursor: Option<i64>) -> bool {
    let message = ClientMessage::Subscribe { cursor };
    let json = match serde_json::to_string(&message) {
        Ok(json) => json,
        Err(error) => {
            tracing::warn!(%error, "failed to serialize subscribe frame");
            return false;
        }
    };
    stream.send(Message::Text(json)).await.is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The feed URL keeps the origin, drops path/query, and maps the scheme.
    #[test]
    fn feed_url_maps_scheme_and_appends_path() {
        assert_eq!(
            feed_url("https://catalog.verglas.io"),
            Some("wss://catalog.verglas.io/v1/catalog/feed".to_owned())
        );
        assert_eq!(
            feed_url("http://127.0.0.1:8181/some/prefix?warehouse=x"),
            Some("ws://127.0.0.1:8181/v1/catalog/feed".to_owned())
        );
    }

    /// A uri without a usable origin (or an unknown scheme) yields no feed URL,
    /// so the daemon polls only.
    #[test]
    fn feed_url_rejects_unusable_uris() {
        assert_eq!(feed_url("not-a-url"), None);
        assert_eq!(feed_url("https://"), None);
        assert_eq!(feed_url("ftp://host/x"), None);
    }
}
