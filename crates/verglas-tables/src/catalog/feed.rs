//! The websocket change-feed protocol and its transport-independent state
//! machine. The wire messages are JSON text frames; [`FeedState`] tracks the
//! last-seen event sequence and turns each server message into a [`FeedAction`]
//! the websocket driver carries out. Factoring the decisions here keeps them
//! unit-testable without a live socket.
//!
//! # Protocol
//!
//! On attach the server sends `hello` with the latest event sequence (0 if
//! none). The client answers `subscribe`: a null cursor means live-only, an
//! integer replays events after that sequence and then goes live. The server
//! pushes `change` frames as tables commit. A `resync` tells the client its
//! cursor is too old to replay: run one polling pass to catch up, then
//! re-subscribe from the hello cursor.

use serde::{Deserialize, Serialize};

use super::TableIdent;

/// A message the catalog service pushes over the feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerMessage {
    /// Sent once on attach: the latest event sequence the server holds (0 if
    /// the server has emitted no events yet).
    Hello {
        /// The latest event sequence.
        cursor: i64,
    },
    /// A committed change on one table. `snapshot_id` and `committed_at` are
    /// advisory; the driver re-reads the table's authoritative pointer.
    Change {
        /// This event's sequence (monotonic per server).
        seq: i64,
        /// The dotted `namespace.table` identifier that changed.
        table: String,
        /// The new snapshot id, as the catalog serializes it (advisory).
        snapshot_id: String,
        /// Commit time in ISO 8601 (advisory).
        committed_at: String,
    },
    /// The client's cursor is too old to replay: catch up by polling once,
    /// then re-subscribe from the hello cursor.
    Resync {
        /// Why the resync was requested (e.g. `cursor-too-old`).
        reason: String,
    },
}

/// A message the client sends over the feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ClientMessage {
    /// Requests the event stream. `null` = live-only; an integer replays every
    /// event with a sequence greater than it, then continues live.
    Subscribe {
        /// The replay cursor, or `None` for live-only.
        cursor: Option<i64>,
    },
}

/// What the websocket driver must do in response to a server message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedAction {
    /// Send a `subscribe` with this cursor (`None` = live-only).
    Subscribe(Option<i64>),
    /// Refresh this table: read its current pointer, diff, and emit downstream.
    Refresh(TableIdent),
    /// Run one polling pass to catch up, then re-subscribe with this cursor.
    ResyncThenResubscribe(i64),
}

/// The feed's transport-independent state: the last-seen event sequence plus
/// the most recent hello cursor. One instance drives one connected session;
/// `last_seen` is carried across reconnects so a resumed subscription replays
/// only the events missed during the gap.
#[derive(Debug, Clone)]
pub struct FeedState {
    /// Highest event sequence applied so far, or `None` before the first
    /// change (a fresh, poll-seeded attach subscribes live-only).
    last_seen: Option<i64>,
    /// The cursor from the most recent hello, used to re-subscribe after a
    /// resync.
    hello_cursor: i64,
}

impl FeedState {
    /// Starts a session with a known last-seen sequence (`None` on a fresh
    /// attach that a polling pass has already seeded).
    pub fn new(last_seen: Option<i64>) -> FeedState {
        FeedState {
            last_seen,
            hello_cursor: 0,
        }
    }

    /// The highest event sequence applied so far — persisted by the driver
    /// across reconnects.
    pub fn last_seen(&self) -> Option<i64> {
        self.last_seen
    }

    /// Folds one server message into the state and returns the action to take.
    ///
    /// - `hello` → subscribe with the carried `last_seen` (live-only when it is
    ///   `None`), and remember the hello cursor for a later resync.
    /// - `change` → advance `last_seen` and refresh the named table.
    /// - `resync` → adopt the hello cursor as `last_seen`, poll once to catch
    ///   up, then re-subscribe from that cursor.
    pub fn on_message(&mut self, message: ServerMessage) -> FeedAction {
        match message {
            ServerMessage::Hello { cursor } => {
                self.hello_cursor = cursor;
                FeedAction::Subscribe(self.last_seen)
            }
            ServerMessage::Change { seq, table, .. } => {
                self.last_seen = Some(self.last_seen.map_or(seq, |seen| seen.max(seq)));
                FeedAction::Refresh(parse_dotted_table(&table))
            }
            ServerMessage::Resync { .. } => {
                self.last_seen = Some(self.hello_cursor);
                FeedAction::ResyncThenResubscribe(self.hello_cursor)
            }
        }
    }
}

/// Splits a dotted `namespace.table` into a [`TableIdent`]: the last segment is
/// the table name, the rest is the namespace. Mirrors [`TableIdent::dotted`],
/// so a round trip through both is stable for single- and multi-level
/// namespaces.
pub fn parse_dotted_table(dotted: &str) -> TableIdent {
    let mut segments: Vec<&str> = dotted.split('.').collect();
    let name = segments.pop().unwrap_or_default().to_owned();
    TableIdent {
        namespace: segments.into_iter().map(str::to_owned).collect(),
        name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `hello` frame round-trips through serde with its integer cursor.
    #[test]
    fn hello_parses_from_wire_json() {
        let parsed: ServerMessage =
            serde_json::from_str(r#"{"type":"hello","cursor":42}"#).expect("hello parses");
        assert_eq!(parsed, ServerMessage::Hello { cursor: 42 });
    }

    /// A `change` frame parses every field the server sends.
    #[test]
    fn change_parses_all_fields() {
        let json = r#"{"type":"change","seq":7,"table":"db.events","snapshot_id":"9182",
                       "committed_at":"2026-08-01T00:00:00Z"}"#;
        let parsed: ServerMessage = serde_json::from_str(json).expect("change parses");
        assert_eq!(
            parsed,
            ServerMessage::Change {
                seq: 7,
                table: "db.events".to_owned(),
                snapshot_id: "9182".to_owned(),
                committed_at: "2026-08-01T00:00:00Z".to_owned(),
            }
        );
    }

    /// A `resync` frame parses its reason.
    #[test]
    fn resync_parses_reason() {
        let parsed: ServerMessage =
            serde_json::from_str(r#"{"type":"resync","reason":"cursor-too-old"}"#)
                .expect("resync parses");
        assert_eq!(
            parsed,
            ServerMessage::Resync {
                reason: "cursor-too-old".to_owned()
            }
        );
    }

    /// A live-only subscribe serializes `cursor` as JSON null.
    #[test]
    fn subscribe_live_only_serializes_null_cursor() {
        let json = serde_json::to_string(&ClientMessage::Subscribe { cursor: None })
            .expect("subscribe serializes");
        assert_eq!(json, r#"{"type":"subscribe","cursor":null}"#);
    }

    /// A replay subscribe serializes the integer cursor.
    #[test]
    fn subscribe_replay_serializes_integer_cursor() {
        let json = serde_json::to_string(&ClientMessage::Subscribe { cursor: Some(99) })
            .expect("subscribe serializes");
        assert_eq!(json, r#"{"type":"subscribe","cursor":99}"#);
    }

    /// A fresh (poll-seeded) attach subscribes live-only: no replay cursor.
    #[test]
    fn hello_on_fresh_attach_subscribes_live_only() {
        let mut state = FeedState::new(None);
        let action = state.on_message(ServerMessage::Hello { cursor: 5 });
        assert_eq!(action, FeedAction::Subscribe(None));
    }

    /// A reconnect (last-seen known) subscribes with the resume cursor so the
    /// server replays only events missed during the gap.
    #[test]
    fn hello_on_reconnect_subscribes_from_last_seen() {
        let mut state = FeedState::new(Some(12));
        let action = state.on_message(ServerMessage::Hello { cursor: 30 });
        assert_eq!(action, FeedAction::Subscribe(Some(12)));
    }

    /// A change advances last-seen and asks to refresh the named table.
    #[test]
    fn change_advances_cursor_and_refreshes_table() {
        let mut state = FeedState::new(None);
        let action = state.on_message(ServerMessage::Change {
            seq: 3,
            table: "db.events".to_owned(),
            snapshot_id: "1".to_owned(),
            committed_at: "t".to_owned(),
        });
        assert_eq!(
            action,
            FeedAction::Refresh(TableIdent::new(&["db"], "events"))
        );
        assert_eq!(state.last_seen(), Some(3));
    }

    /// Out-of-order or replayed change frames never move last-seen backwards.
    #[test]
    fn change_cursor_never_regresses() {
        let mut state = FeedState::new(Some(10));
        state.on_message(ServerMessage::Change {
            seq: 4,
            table: "db.t".to_owned(),
            snapshot_id: "1".to_owned(),
            committed_at: "t".to_owned(),
        });
        assert_eq!(state.last_seen(), Some(10));
    }

    /// A resync adopts the hello cursor, then re-subscribes from it.
    #[test]
    fn resync_polls_then_resubscribes_from_hello_cursor() {
        let mut state = FeedState::new(Some(2));
        // The hello cursor is learned from the hello frame.
        state.on_message(ServerMessage::Hello { cursor: 100 });
        let action = state.on_message(ServerMessage::Resync {
            reason: "cursor-too-old".to_owned(),
        });
        assert_eq!(action, FeedAction::ResyncThenResubscribe(100));
        assert_eq!(state.last_seen(), Some(100));
    }

    /// Multi-level namespaces round-trip: `dotted()` and `parse_dotted_table`
    /// agree on where the namespace ends and the table name begins.
    #[test]
    fn dotted_table_round_trips_multi_level_namespace() {
        let ident = TableIdent::new(&["ns", "sub"], "t");
        assert_eq!(parse_dotted_table(&ident.dotted()), ident);
        assert_eq!(
            parse_dotted_table("db.events"),
            TableIdent::new(&["db"], "events")
        );
    }
}
