//! The minimal Neon safekeeper wire contract Verglas implements. It parses the
//! proposer/acceptor protocol carried in PostgreSQL `COPY BOTH` messages and
//! serializes the replies expected by Neon's walproposer.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::Lsn;

/// The only proposer/acceptor protocol revision implemented by this prototype.
pub const PROTOCOL_VERSION: u32 = 3;

/// The upstream source revision whose protocol-v3 layout the compatibility
/// vectors and this codec follow.
pub const NEON_PROTOCOL_SOURCE: &str =
    "https://github.com/neondatabase/neon/tree/8f60b04da47ffefe0e52bda2440134b42874eb75";

/// Neon's maximum WAL payload in one proposer append message: 16 8-KiB pages.
const MAX_SEND_SIZE: usize = 8 * 1024 * 16;

/// One safekeeper member as represented in Neon's membership configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    /// Numeric Neon node id.
    pub id: u64,
    /// Host the walproposer uses for this member.
    pub host: String,
    /// PostgreSQL protocol port.
    pub port: u16,
}

/// The membership generation carried by protocol-v3 election messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    /// Monotonic membership generation.
    pub generation: u32,
    /// Active member set.
    pub members: Vec<Member>,
    /// Joint-consensus destination set, empty outside membership transitions.
    pub new_members: Vec<Member>,
}

/// The first message sent by walproposer on a WAL-push stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Greeting {
    /// Neon tenant id in its 32-character hexadecimal representation.
    pub tenant_id: String,
    /// Neon timeline id in its 32-character hexadecimal representation.
    pub timeline_id: String,
    /// Membership configuration supplied by walproposer.
    pub membership: Membership,
    /// PostgreSQL server version in `PG_VERSION_NUM` form.
    pub pg_version: u32,
    /// PostgreSQL system identifier.
    pub system_id: u64,
    /// PostgreSQL WAL segment size.
    pub wal_segment_size: u32,
}

/// A walproposer request asking the acceptor to vote in `term`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteRequest {
    /// Membership generation the vote belongs to.
    pub generation: u32,
    /// Proposed consensus term.
    pub term: u64,
}

/// One term boundary in the proposer and acceptor histories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TermSwitch {
    /// Consensus term beginning at `lsn`.
    pub term: u64,
    /// First WAL byte in the term.
    pub lsn: Lsn,
}

/// The announcement that the proposer has completed its election.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Elected {
    /// Membership generation the election belongs to.
    pub generation: u32,
    /// Elected term.
    pub term: u64,
    /// First WAL byte this proposer will stream.
    pub start_streaming_at: Lsn,
    /// Complete term-switch history selected during election.
    pub term_history: Vec<TermSwitch>,
}

/// One WAL range sent by walproposer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    /// Membership generation the append belongs to.
    pub generation: u32,
    /// Elected term authorizing the append.
    pub term: u64,
    /// First LSN in `wal`.
    pub begin_lsn: Lsn,
    /// One-past-last LSN in `wal`.
    pub end_lsn: Lsn,
    /// Quorum-committed LSN known by walproposer.
    pub commit_lsn: Lsn,
    /// Oldest LSN still needed for recovery.
    pub truncate_lsn: Lsn,
    /// Byte-identical PostgreSQL WAL for `[begin_lsn, end_lsn)`.
    pub wal: Bytes,
}

/// A decoded message sent from Neon walproposer to Verglas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposerMessage {
    /// Stream greeting and timeline identity.
    Greeting(Greeting),
    /// Election vote request.
    Vote(VoteRequest),
    /// Elected-proposer announcement.
    Elected(Elected),
    /// WAL append request.
    Append(AppendRequest),
}

/// Safekeeper greeting returned to walproposer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptorGreeting {
    /// Numeric identity of this Verglas ingress.
    pub node_id: u64,
    /// Membership echoed for walproposer's generation checks.
    pub membership: Membership,
    /// Highest term durably accepted by this timeline.
    pub term: u64,
}

/// Safekeeper vote returned to walproposer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteResponse {
    /// Membership generation the vote belongs to.
    pub generation: u32,
    /// Term persisted before granting the vote.
    pub term: u64,
    /// Whether this acceptor granted the vote.
    pub vote_given: bool,
    /// End of byte-identical durable WAL.
    pub flush_lsn: Lsn,
    /// Oldest WAL retained for recovery.
    pub truncate_lsn: Lsn,
    /// Durable term-switch history.
    pub term_history: Vec<TermSwitch>,
}

/// Durability acknowledgement returned after processing WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResponse {
    /// Membership generation the append belongs to.
    pub generation: u32,
    /// Current durable term.
    pub term: u64,
    /// End of WAL durable in the Verglas EC quorum.
    pub flush_lsn: Lsn,
    /// Commit watermark learned from walproposer.
    pub commit_lsn: Lsn,
}

/// A reply sent from the Verglas acceptor to Neon walproposer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptorMessage {
    /// Initial acceptor state.
    Greeting(AcceptorGreeting),
    /// Persisted election vote.
    Vote(VoteResponse),
    /// WAL durability watermark.
    Append(AppendResponse),
}

/// A PostgreSQL simple-query command understood by the safekeeper listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SafekeeperCommand {
    /// Initialize a newly-created timeline at the pageserver's base LSN before
    /// the first compute connects. Mirrors Neon's timeline-create API.
    TimelineCreate {
        /// First valid WAL position for the timeline.
        start_lsn: Lsn,
    },
    /// Enter proposer/acceptor `COPY BOTH` mode.
    StartWalPush {
        /// Proposer/acceptor protocol revision.
        protocol_version: u32,
        /// Whether the greeting may create a previously unknown timeline.
        allow_timeline_creation: bool,
    },
    /// Stream physical WAL to pageserver or a recovery client.
    StartReplication {
        /// First requested WAL byte.
        start_lsn: Lsn,
        /// Optional consensus term required by recovery.
        term: Option<u64>,
    },
    /// PostgreSQL replication identity query.
    IdentifySystem,
    /// Neon timeline status query.
    TimelineStatus,
}

/// A malformed or unsupported Neon protocol frame.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtocolError {
    /// This build intentionally implements one pinned protocol revision.
    #[error("unsupported Neon safekeeper protocol version {0}; expected 3")]
    UnsupportedVersion(u32),
    /// A frame ended before a required field was present.
    #[error("incomplete {field}: needed {needed} bytes, only {remaining} remain")]
    Incomplete {
        /// Field being decoded.
        field: &'static str,
        /// Bytes required by that field.
        needed: usize,
        /// Bytes left in the frame.
        remaining: usize,
    },
    /// A NUL-terminated protocol string was invalid.
    #[error("invalid {field}: {reason}")]
    InvalidString {
        /// Field being decoded.
        field: &'static str,
        /// Validation failure.
        reason: String,
    },
    /// A field or command violates the pinned protocol contract.
    #[error("invalid Neon protocol: {0}")]
    Invalid(String),
    /// A message tag is not part of protocol v3.
    #[error("unknown Neon proposer message tag {0:#x}")]
    UnknownTag(u8),
    /// The PostgreSQL command is not a safekeeper command.
    #[error("unsupported safekeeper command: {0}")]
    UnsupportedCommand(String),
}

/// Parses one proposer/acceptor `CopyData` payload using the pinned v3 layout.
pub fn parse_proposer(
    mut frame: Bytes,
    protocol_version: u32,
) -> Result<ProposerMessage, ProtocolError> {
    require_version(protocol_version)?;
    let tag = take_u8(&mut frame, "message tag")?;
    let message = match tag {
        b'g' => ProposerMessage::Greeting(parse_greeting(&mut frame)?),
        b'v' => ProposerMessage::Vote(VoteRequest {
            generation: take_u32(&mut frame, "vote generation")?,
            term: take_u64(&mut frame, "vote term")?,
        }),
        b'e' => ProposerMessage::Elected(Elected {
            generation: take_u32(&mut frame, "elected generation")?,
            term: take_u64(&mut frame, "elected term")?,
            start_streaming_at: Lsn(take_u64(&mut frame, "start_streaming_at")?),
            term_history: take_term_history(&mut frame)?,
        }),
        b'a' => ProposerMessage::Append(parse_append(&mut frame)?),
        other => return Err(ProtocolError::UnknownTag(other)),
    };
    if !frame.is_empty() {
        return Err(ProtocolError::Invalid(format!(
            "{} trailing bytes after message",
            frame.len()
        )));
    }
    Ok(message)
}

/// Serializes one acceptor reply using Neon's pinned protocol-v3 layout.
pub fn serialize_acceptor(
    message: &AcceptorMessage,
    protocol_version: u32,
) -> Result<Bytes, ProtocolError> {
    require_version(protocol_version)?;
    let mut frame = BytesMut::new();
    match message {
        AcceptorMessage::Greeting(message) => {
            frame.put_u8(b'g');
            frame.put_u64(message.node_id);
            put_membership(&mut frame, &message.membership)?;
            frame.put_u64(message.term);
        }
        AcceptorMessage::Vote(message) => {
            frame.put_u8(b'v');
            frame.put_u32(message.generation);
            frame.put_u64(message.term);
            frame.put_u8(u8::from(message.vote_given));
            frame.put_u64(message.flush_lsn.0);
            frame.put_u64(message.truncate_lsn.0);
            put_term_history(&mut frame, &message.term_history)?;
        }
        AcceptorMessage::Append(message) => {
            frame.put_u8(b'a');
            frame.put_u32(message.generation);
            frame.put_u64(message.term);
            frame.put_u64(message.flush_lsn.0);
            frame.put_u64(message.commit_lsn.0);
            // Empty hot-standby feedback. Pageserver feedback is optional and
            // omitted until the replication listener has feedback to report.
            frame.put_i64(0);
            frame.put_u64(0);
            frame.put_u64(0);
        }
    }
    Ok(frame.freeze())
}

/// Parses the simple-query commands used by Neon compute and pageserver.
pub fn parse_command(command: &str) -> Result<SafekeeperCommand, ProtocolError> {
    let command = command.trim();
    if let Some(value) = command.strip_prefix("TIMELINE_CREATE ") {
        return Ok(SafekeeperCommand::TimelineCreate {
            start_lsn: parse_lsn(value.trim())?,
        });
    }
    if let Some(rest) = command.strip_prefix("START_WAL_PUSH") {
        return parse_start_wal_push(command, rest);
    }
    if command.starts_with("START_REPLICATION") {
        return parse_start_replication(command);
    }
    if command.starts_with("IDENTIFY_SYSTEM") {
        return Ok(SafekeeperCommand::IdentifySystem);
    }
    if command.starts_with("TIMELINE_STATUS") {
        return Ok(SafekeeperCommand::TimelineStatus);
    }
    Err(ProtocolError::UnsupportedCommand(command.to_owned()))
}

/// Rejects any protocol revision other than the pinned v3 contract.
fn require_version(protocol_version: u32) -> Result<(), ProtocolError> {
    if protocol_version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion(protocol_version))
    }
}

/// Parses the protocol-v3 greeting body after its tag.
fn parse_greeting(frame: &mut Bytes) -> Result<Greeting, ProtocolError> {
    let tenant_id = take_hex_id(frame, "tenant id")?;
    let timeline_id = take_hex_id(frame, "timeline id")?;
    let membership = take_membership(frame)?;
    Ok(Greeting {
        tenant_id,
        timeline_id,
        membership,
        pg_version: take_u32(frame, "PostgreSQL version")?,
        system_id: take_u64(frame, "system id")?,
        wal_segment_size: take_u32(frame, "WAL segment size")?,
    })
}

/// Parses one protocol-v3 append body and verifies its LSN-derived byte count.
fn parse_append(frame: &mut Bytes) -> Result<AppendRequest, ProtocolError> {
    let generation = take_u32(frame, "append generation")?;
    let term = take_u64(frame, "append term")?;
    let begin_lsn = Lsn(take_u64(frame, "append begin_lsn")?);
    let end_lsn = Lsn(take_u64(frame, "append end_lsn")?);
    let commit_lsn = Lsn(take_u64(frame, "append commit_lsn")?);
    let truncate_lsn = Lsn(take_u64(frame, "append truncate_lsn")?);
    let wal_len = end_lsn
        .0
        .checked_sub(begin_lsn.0)
        .ok_or_else(|| ProtocolError::Invalid("append begin_lsn exceeds end_lsn".to_owned()))?
        as usize;
    if wal_len > MAX_SEND_SIZE {
        return Err(ProtocolError::Invalid(format!(
            "append has {wal_len} WAL bytes; maximum is {MAX_SEND_SIZE}"
        )));
    }
    ensure(frame, wal_len, "append WAL")?;
    let wal = frame.split_to(wal_len);
    Ok(AppendRequest {
        generation,
        term,
        begin_lsn,
        end_lsn,
        commit_lsn,
        truncate_lsn,
        wal,
    })
}

/// Parses one membership configuration.
fn take_membership(frame: &mut Bytes) -> Result<Membership, ProtocolError> {
    let generation = take_u32(frame, "membership generation")?;
    let members = take_members(frame, "members")?;
    let new_members = take_members(frame, "new members")?;
    Ok(Membership {
        generation,
        members,
        new_members,
    })
}

/// Parses one length-prefixed member set.
fn take_members(frame: &mut Bytes, field: &'static str) -> Result<Vec<Member>, ProtocolError> {
    let count = take_u32(frame, field)? as usize;
    let mut members = Vec::with_capacity(count);
    for _ in 0..count {
        members.push(Member {
            id: take_u64(frame, "member id")?,
            host: take_cstr(frame, "member host")?,
            port: take_u16(frame, "member port")?,
        });
    }
    Ok(members)
}

/// Parses one length-prefixed term history.
fn take_term_history(frame: &mut Bytes) -> Result<Vec<TermSwitch>, ProtocolError> {
    let count = take_u32(frame, "term history length")? as usize;
    let mut history = Vec::with_capacity(count);
    for _ in 0..count {
        history.push(TermSwitch {
            term: take_u64(frame, "history term")?,
            lsn: Lsn(take_u64(frame, "history LSN")?),
        });
    }
    Ok(history)
}

/// Serializes one membership configuration.
fn put_membership(frame: &mut BytesMut, membership: &Membership) -> Result<(), ProtocolError> {
    frame.put_u32(membership.generation);
    put_members(frame, &membership.members)?;
    put_members(frame, &membership.new_members)
}

/// Serializes one member set after checking its length fits the wire field.
fn put_members(frame: &mut BytesMut, members: &[Member]) -> Result<(), ProtocolError> {
    let count = u32::try_from(members.len())
        .map_err(|_| ProtocolError::Invalid("too many membership entries".to_owned()))?;
    frame.put_u32(count);
    for member in members {
        frame.put_u64(member.id);
        put_cstr(frame, &member.host)?;
        frame.put_u16(member.port);
    }
    Ok(())
}

/// Serializes one term history after checking its length fits the wire field.
fn put_term_history(frame: &mut BytesMut, history: &[TermSwitch]) -> Result<(), ProtocolError> {
    let count = u32::try_from(history.len())
        .map_err(|_| ProtocolError::Invalid("term history is too long".to_owned()))?;
    frame.put_u32(count);
    for entry in history {
        frame.put_u64(entry.term);
        frame.put_u64(entry.lsn.0);
    }
    Ok(())
}

/// Parses `START_WAL_PUSH` options with Neon's defaults.
fn parse_start_wal_push(command: &str, rest: &str) -> Result<SafekeeperCommand, ProtocolError> {
    let mut protocol_version = 2;
    let mut allow_timeline_creation = true;
    let rest = rest.trim();
    if !rest.is_empty() {
        let options = rest
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .ok_or_else(|| ProtocolError::Invalid(format!("malformed command {command}")))?;
        for option in options.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            let mut fields = option.split_whitespace();
            let key = fields
                .next()
                .ok_or_else(|| ProtocolError::Invalid(format!("malformed option {option}")))?;
            let value = fields
                .next()
                .ok_or_else(|| ProtocolError::Invalid(format!("malformed option {option}")))?
                .trim_matches('\'');
            if fields.next().is_some() {
                return Err(ProtocolError::Invalid(format!("malformed option {option}")));
            }
            match key {
                "proto_version" => {
                    protocol_version = value.parse().map_err(|_| {
                        ProtocolError::Invalid(format!("invalid proto_version {value}"))
                    })?;
                }
                "allow_timeline_creation" => {
                    allow_timeline_creation = value.parse().map_err(|_| {
                        ProtocolError::Invalid(format!("invalid allow_timeline_creation {value}"))
                    })?;
                }
                _ => {}
            }
        }
    }
    Ok(SafekeeperCommand::StartWalPush {
        protocol_version,
        allow_timeline_creation,
    })
}

/// Parses Neon's physical replication command and optional recovery term.
fn parse_start_replication(command: &str) -> Result<SafekeeperCommand, ProtocolError> {
    let before_options = command.split_once('(').map_or(command, |(head, _)| head);
    let mut fields = before_options.split_whitespace();
    if fields.next() != Some("START_REPLICATION") {
        return Err(ProtocolError::Invalid(format!(
            "malformed command {command}"
        )));
    }
    let mut next = fields.next();
    if next == Some("SLOT") {
        fields
            .next()
            .ok_or_else(|| ProtocolError::Invalid("missing replication slot".to_owned()))?;
        next = fields.next();
    }
    if next == Some("PHYSICAL") {
        next = fields.next();
    }
    let start_lsn = parse_lsn(
        next.ok_or_else(|| ProtocolError::Invalid("missing replication LSN".to_owned()))?,
    )?;
    if fields.next().is_some() {
        return Err(ProtocolError::Invalid(format!(
            "malformed command {command}"
        )));
    }
    let term = command
        .split_once("term='")
        .map(|(_, suffix)| {
            let value = suffix.split_once('\'').map_or(suffix, |(value, _)| value);
            value
                .parse::<u64>()
                .map_err(|_| ProtocolError::Invalid(format!("invalid term {value}")))
        })
        .transpose()?;
    Ok(SafekeeperCommand::StartReplication { start_lsn, term })
}

/// Parses PostgreSQL's conventional `high/low` hexadecimal LSN rendering.
fn parse_lsn(value: &str) -> Result<Lsn, ProtocolError> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| ProtocolError::Invalid(format!("invalid LSN {value}")))?;
    let high = u32::from_str_radix(high, 16)
        .map_err(|_| ProtocolError::Invalid(format!("invalid LSN {value}")))?;
    let low = u32::from_str_radix(low, 16)
        .map_err(|_| ProtocolError::Invalid(format!("invalid LSN {value}")))?;
    Ok(Lsn((u64::from(high) << 32) | u64::from(low)))
}

/// Reads and validates a Neon 16-byte id encoded as 32 hexadecimal characters.
fn take_hex_id(frame: &mut Bytes, field: &'static str) -> Result<String, ProtocolError> {
    let value = take_cstr(frame, field)?;
    if value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(value)
    } else {
        Err(ProtocolError::InvalidString {
            field,
            reason: "expected 32 hexadecimal characters".to_owned(),
        })
    }
}

/// Reads one NUL-terminated UTF-8 string.
fn take_cstr(frame: &mut Bytes, field: &'static str) -> Result<String, ProtocolError> {
    let end =
        frame
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| ProtocolError::InvalidString {
                field,
                reason: "missing NUL terminator".to_owned(),
            })?;
    let bytes = frame.split_to(end);
    frame.advance(1);
    std::str::from_utf8(&bytes)
        .map(str::to_owned)
        .map_err(|error| ProtocolError::InvalidString {
            field,
            reason: error.to_string(),
        })
}

/// Writes one NUL-terminated string, rejecting embedded terminators.
fn put_cstr(frame: &mut BytesMut, value: &str) -> Result<(), ProtocolError> {
    if value.as_bytes().contains(&0) {
        return Err(ProtocolError::Invalid(
            "membership host contains NUL".to_owned(),
        ));
    }
    frame.put_slice(value.as_bytes());
    frame.put_u8(0);
    Ok(())
}

/// Verifies that `frame` still contains a fixed-width field.
fn ensure(frame: &Bytes, needed: usize, field: &'static str) -> Result<(), ProtocolError> {
    if frame.remaining() < needed {
        Err(ProtocolError::Incomplete {
            field,
            needed,
            remaining: frame.remaining(),
        })
    } else {
        Ok(())
    }
}

/// Reads one network-order byte.
fn take_u8(frame: &mut Bytes, field: &'static str) -> Result<u8, ProtocolError> {
    ensure(frame, 1, field)?;
    Ok(frame.get_u8())
}

/// Reads one network-order `u16`.
fn take_u16(frame: &mut Bytes, field: &'static str) -> Result<u16, ProtocolError> {
    ensure(frame, 2, field)?;
    Ok(frame.get_u16())
}

/// Reads one network-order `u32`.
fn take_u32(frame: &mut Bytes, field: &'static str) -> Result<u32, ProtocolError> {
    ensure(frame, 4, field)?;
    Ok(frame.get_u32())
}

/// Reads one network-order `u64`.
fn take_u64(frame: &mut Bytes, field: &'static str) -> Result<u64, ProtocolError> {
    ensure(frame, 8, field)?;
    Ok(frame.get_u64())
}
