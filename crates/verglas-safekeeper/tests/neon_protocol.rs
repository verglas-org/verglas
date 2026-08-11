//! Compatibility vectors for Neon's proposer/acceptor protocol v3. The byte
//! layout is pinned to neondatabase/neon commit
//! `8f60b04da47ffefe0e52bda2440134b42874eb75`.

use bytes::{BufMut, Bytes, BytesMut};
use verglas_safekeeper::Lsn;
use verglas_safekeeper::protocol::{
    AcceptorGreeting, AcceptorMessage, AppendRequest, AppendResponse, Greeting, Member, Membership,
    NEON_PROTOCOL_SOURCE, PROTOCOL_VERSION, ProposerMessage, SafekeeperCommand, TermSwitch,
    VoteRequest, parse_command, parse_proposer, serialize_acceptor,
};

const TENANT: &str = "0123456789abcdef0123456789abcdef";
const TIMELINE: &str = "fedcba9876543210fedcba9876543210";

/// Appends one NUL-terminated string in Neon's protocol-v3 representation.
fn put_cstr(frame: &mut BytesMut, value: &str) {
    frame.put_slice(value.as_bytes());
    frame.put_u8(0);
}

/// The one-member configuration emitted by a walproposer configured with one
/// selected Verglas safekeeper endpoint.
fn membership() -> Membership {
    Membership {
        generation: 7,
        members: vec![Member {
            id: 41,
            host: "10.44.0.8".to_owned(),
            port: 5454,
        }],
        new_members: Vec::new(),
    }
}

#[test]
fn parses_neon_v3_greeting_vector() {
    let mut frame = BytesMut::new();
    frame.put_u8(b'g');
    put_cstr(&mut frame, TENANT);
    put_cstr(&mut frame, TIMELINE);
    frame.put_u32(7);
    frame.put_u32(1);
    frame.put_u64(41);
    put_cstr(&mut frame, "10.44.0.8");
    frame.put_u16(5454);
    frame.put_u32(0);
    frame.put_u32(160_000);
    frame.put_u64(0x1122_3344_5566_7788);
    frame.put_u32(16 * 1024 * 1024);

    let parsed = parse_proposer(frame.freeze(), PROTOCOL_VERSION).expect("v3 greeting");
    assert_eq!(
        parsed,
        ProposerMessage::Greeting(Greeting {
            tenant_id: TENANT.to_owned(),
            timeline_id: TIMELINE.to_owned(),
            membership: membership(),
            pg_version: 160_000,
            system_id: 0x1122_3344_5566_7788,
            wal_segment_size: 16 * 1024 * 1024,
        })
    );
    assert!(NEON_PROTOCOL_SOURCE.contains("8f60b04d"));
}

#[test]
fn parses_timeline_create_command() {
    assert_eq!(
        parse_command("TIMELINE_CREATE 0/14F13F0").expect("timeline create"),
        SafekeeperCommand::TimelineCreate {
            start_lsn: Lsn(0x14F13F0),
        },
    );
}

#[test]
fn parses_vote_elected_and_append_vectors() {
    let mut vote = BytesMut::new();
    vote.put_u8(b'v');
    vote.put_u32(7);
    vote.put_u64(12);
    assert_eq!(
        parse_proposer(vote.freeze(), PROTOCOL_VERSION).expect("vote"),
        ProposerMessage::Vote(VoteRequest {
            generation: 7,
            term: 12,
        })
    );

    let mut elected = BytesMut::new();
    elected.put_u8(b'e');
    elected.put_u32(7);
    elected.put_u64(12);
    elected.put_u64(0x20);
    elected.put_u32(2);
    elected.put_u64(10);
    elected.put_u64(0x10);
    elected.put_u64(12);
    elected.put_u64(0x20);
    assert!(matches!(
        parse_proposer(elected.freeze(), PROTOCOL_VERSION).expect("elected"),
        ProposerMessage::Elected(message)
            if message.generation == 7
                && message.term == 12
                && message.start_streaming_at == Lsn(0x20)
                && message.term_history
                    == vec![
                        TermSwitch { term: 10, lsn: Lsn(0x10) },
                        TermSwitch { term: 12, lsn: Lsn(0x20) },
                    ]
    ));

    let wal = Bytes::from_static(b"postgres-wal");
    let mut append = BytesMut::new();
    append.put_u8(b'a');
    append.put_u32(7);
    append.put_u64(12);
    append.put_u64(0x20);
    append.put_u64(0x20 + wal.len() as u64);
    append.put_u64(0x18);
    append.put_u64(0x10);
    append.put_slice(&wal);
    assert_eq!(
        parse_proposer(append.freeze(), PROTOCOL_VERSION).expect("append"),
        ProposerMessage::Append(AppendRequest {
            generation: 7,
            term: 12,
            begin_lsn: Lsn(0x20),
            end_lsn: Lsn(0x20 + wal.len() as u64),
            commit_lsn: Lsn(0x18),
            truncate_lsn: Lsn(0x10),
            wal,
        })
    );
}

#[test]
fn parses_the_large_append_emitted_by_the_verglas_neon_compute() {
    let wal = vec![0x5a; 512 * 1024];
    let begin_lsn = 0x14_EE2C0_u64;
    let mut append = BytesMut::new();
    append.put_u8(b'a');
    append.put_u32(0);
    append.put_u64(2);
    append.put_u64(begin_lsn);
    append.put_u64(begin_lsn + wal.len() as u64);
    append.put_u64(begin_lsn);
    append.put_u64(begin_lsn);
    append.put_slice(&wal);

    assert!(matches!(
        parse_proposer(append.freeze(), PROTOCOL_VERSION).expect("large append"),
        ProposerMessage::Append(message) if message.wal.len() == wal.len()
    ));
}

#[test]
fn serializes_neon_v3_acceptor_vectors() {
    let greeting = serialize_acceptor(
        &AcceptorMessage::Greeting(AcceptorGreeting {
            node_id: 41,
            membership: membership(),
            term: 12,
        }),
        PROTOCOL_VERSION,
    )
    .expect("greeting response");
    assert_eq!(greeting[0], b'g');
    assert_eq!(&greeting[1..9], &41_u64.to_be_bytes());

    let append = serialize_acceptor(
        &AcceptorMessage::Append(AppendResponse {
            generation: 7,
            term: 12,
            flush_lsn: Lsn(0x80),
            commit_lsn: Lsn(0x70),
        }),
        PROTOCOL_VERSION,
    )
    .expect("append response");
    let mut expected = BytesMut::new();
    expected.put_u8(b'a');
    expected.put_u32(7);
    expected.put_u64(12);
    expected.put_u64(0x80);
    expected.put_u64(0x70);
    expected.put_i64(0);
    expected.put_u64(0);
    expected.put_u64(0);
    assert_eq!(append, expected.freeze());
}

#[test]
fn parses_the_two_postgres_replication_commands_neon_uses() {
    assert_eq!(
        parse_command("START_WAL_PUSH (proto_version '3', allow_timeline_creation 'false')")
            .expect("wal push"),
        SafekeeperCommand::StartWalPush {
            protocol_version: 3,
            allow_timeline_creation: false,
        }
    );
    assert_eq!(
        parse_command(
            "START_REPLICATION SLOT \"repl_44444444444444444444444444444444_\" 0/16B60A10 TIMELINE 1"
        )
        .expect("replication"),
        SafekeeperCommand::StartReplication {
            start_lsn: Lsn(0x16B6_0A10),
            term: None,
        }
    );
}
