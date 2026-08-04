//! A from-scratch decoder for the PostgreSQL pgoutput logical-replication
//! protocol, version 1 (`proto_version=1`).
//!
//! Each logical-replication change is one pgoutput message: a first byte that
//! tags the message kind, followed by a fixed layout of big-endian integers,
//! NUL-terminated C strings, and (for row changes) a `TupleData` block. This
//! module decodes a single message byte buffer into a typed [`Message`]. It
//! pulls in no `postgres-protocol` crate — the bytes are decoded here so the
//! CDC runner owns the exact wire contract it depends on.
//!
//! # Timestamps
//!
//! pgoutput timestamps are microseconds since the PostgreSQL epoch
//! (2000-01-01 00:00:00 UTC). The decoder converts them to unix microseconds by
//! adding [`PG_EPOCH_UNIX_MICROS`], so every timestamp a caller sees is already
//! unix-epoch micros.
//!
//! # A note on the `Insert` new-tuple marker
//!
//! Real pgoutput prefixes an insert's tuple with a `N` submessage byte
//! (`I` rel_oid `N` TupleData); an update's new tuple is likewise `N`, and its
//! optional old tuple is `K` (key only) or `O` (full old row). This decoder
//! follows the real protocol exactly, including the `N` marker for inserts, so
//! it decodes a live PostgreSQL stream unmodified.

use thiserror::Error;

/// Microseconds between the unix epoch (1970-01-01) and the PostgreSQL epoch
/// (2000-01-01). Added to every pgoutput timestamp to yield unix micros.
pub const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

/// A decode failure: the buffer ended early, or a tag/kind byte was not one the
/// protocol defines.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    /// The buffer ended before a field could be fully read.
    #[error("unexpected end of pgoutput message (needed {needed} more byte(s) at offset {offset})")]
    UnexpectedEof {
        /// The byte offset the read started at.
        offset: usize,
        /// How many more bytes were needed.
        needed: usize,
    },
    /// The leading message tag byte is not a known message kind.
    #[error("unknown pgoutput message tag {0:#04x}")]
    UnknownTag(u8),
    /// A `TupleData` column kind byte was not `n`, `u`, or `t`.
    #[error("unknown tuple column kind {0:#04x}")]
    UnknownTupleKind(u8),
    /// An update/delete tuple submessage marker was not `K`, `O`, or `N`.
    #[error("unknown tuple submessage marker {0:#04x}")]
    UnknownSubmessage(u8),
    /// A C string field was not valid UTF-8.
    #[error("invalid UTF-8 in a pgoutput string field")]
    InvalidUtf8,
}

/// One column value inside a decoded `TupleData`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleCol {
    /// The column is SQL NULL (wire kind `n`).
    Null,
    /// The column value is an unchanged TOAST value, not sent in this message
    /// (wire kind `u`). The value is unknown; treated downstream as null.
    UnchangedToast,
    /// A textual column value (wire kind `t`): pgoutput's text representation of
    /// the column, still to be parsed into its target type.
    Text(String),
}

/// One column of a [`Message::Relation`] descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    /// Column flags; bit 0 set means the column is part of the replica identity
    /// key.
    pub flags: u8,
    /// The column name.
    pub name: String,
    /// The column's PostgreSQL type oid.
    pub type_oid: u32,
    /// The column's type modifier (atttypmod), or -1 when none.
    pub type_mod: i32,
}

/// A `Relation` message: the schema descriptor for a published table. Every row
/// change references a relation by `rel_oid`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    /// The relation's oid — the key row changes reference.
    pub rel_oid: u32,
    /// The schema (namespace) the table is in.
    pub namespace: String,
    /// The table name.
    pub rel_name: String,
    /// The table's replica identity setting (`d`/`n`/`f`/`i` as a byte).
    pub replica_identity: u8,
    /// The table's columns, in ordinal order.
    pub columns: Vec<RelationColumn>,
}

/// A decoded pgoutput message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Transaction start. `commit_ts` is unix micros.
    Begin {
        /// The LSN of the transaction's commit record.
        final_lsn: u64,
        /// The transaction commit timestamp, unix micros.
        commit_ts: i64,
        /// The transaction id.
        xid: u32,
    },
    /// Transaction commit. `commit_ts` is unix micros.
    Commit {
        /// Commit flags (currently always 0).
        flags: u8,
        /// The LSN of the commit record.
        commit_lsn: u64,
        /// The end LSN of the transaction (the next byte after the commit).
        end_lsn: u64,
        /// The transaction commit timestamp, unix micros.
        commit_ts: i64,
    },
    /// A replication-origin marker.
    Origin {
        /// The commit LSN this origin refers to.
        commit_lsn: u64,
        /// The origin name.
        name: String,
    },
    /// A relation (table) schema descriptor.
    Relation(Relation),
    /// A type announcement for a user-defined type used by a relation.
    Type {
        /// The type oid.
        type_oid: u32,
        /// The type's schema (namespace).
        namespace: String,
        /// The type name.
        name: String,
    },
    /// An inserted row.
    Insert {
        /// The relation the row belongs to.
        rel_oid: u32,
        /// The new row's column values.
        tuple: Vec<TupleCol>,
    },
    /// An updated row.
    Update {
        /// The relation the row belongs to.
        rel_oid: u32,
        /// The old row (key columns for `K`, full row for `O`), when present.
        old_tuple: Option<Vec<TupleCol>>,
        /// The new row's column values.
        new_tuple: Vec<TupleCol>,
    },
    /// A deleted row (its replica-identity columns).
    Delete {
        /// The relation the row belonged to.
        rel_oid: u32,
        /// The identifying columns of the deleted row.
        old_tuple: Vec<TupleCol>,
    },
    /// A truncate of one or more relations.
    Truncate {
        /// Truncate option flags (bit 0 = CASCADE, bit 1 = RESTART IDENTITY).
        flags: u8,
        /// The relations truncated.
        rel_oids: Vec<u32>,
    },
}

/// A big-endian cursor over a message buffer.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.pos + n > self.buf.len() {
            return Err(DecodeError::UnexpectedEof {
                offset: self.pos,
                needed: (self.pos + n) - self.buf.len(),
            });
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(self.u32()? as i32)
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        let b = self.take(8)?;
        Ok(u64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(self.u64()? as i64)
    }

    /// Reads a NUL-terminated C string.
    fn cstring(&mut self) -> Result<String, DecodeError> {
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.buf.len() {
            return Err(DecodeError::UnexpectedEof {
                offset: start,
                needed: 1,
            });
        }
        let bytes = &self.buf[start..self.pos];
        self.pos += 1; // consume the NUL
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| DecodeError::InvalidUtf8)
    }
}

/// Converts a raw pgoutput timestamp (micros since 2000-01-01) to unix micros.
#[inline]
pub fn pg_micros_to_unix_micros(pg: i64) -> i64 {
    pg.wrapping_add(PG_EPOCH_UNIX_MICROS)
}

/// Decodes a single pgoutput message buffer into a typed [`Message`].
pub fn decode(buf: &[u8]) -> Result<Message, DecodeError> {
    let mut c = Cursor::new(buf);
    let tag = c.u8()?;
    match tag {
        b'B' => {
            let final_lsn = c.u64()?;
            let commit_ts = pg_micros_to_unix_micros(c.i64()?);
            let xid = c.u32()?;
            Ok(Message::Begin {
                final_lsn,
                commit_ts,
                xid,
            })
        }
        b'C' => {
            let flags = c.u8()?;
            let commit_lsn = c.u64()?;
            let end_lsn = c.u64()?;
            let commit_ts = pg_micros_to_unix_micros(c.i64()?);
            Ok(Message::Commit {
                flags,
                commit_lsn,
                end_lsn,
                commit_ts,
            })
        }
        b'O' => {
            let commit_lsn = c.u64()?;
            let name = c.cstring()?;
            Ok(Message::Origin { commit_lsn, name })
        }
        b'R' => {
            let rel_oid = c.u32()?;
            let namespace = c.cstring()?;
            let rel_name = c.cstring()?;
            let replica_identity = c.u8()?;
            let ncols = c.u16()?;
            let mut columns = Vec::with_capacity(ncols as usize);
            for _ in 0..ncols {
                let flags = c.u8()?;
                let name = c.cstring()?;
                let type_oid = c.u32()?;
                let type_mod = c.i32()?;
                columns.push(RelationColumn {
                    flags,
                    name,
                    type_oid,
                    type_mod,
                });
            }
            Ok(Message::Relation(Relation {
                rel_oid,
                namespace,
                rel_name,
                replica_identity,
                columns,
            }))
        }
        b'Y' => {
            let type_oid = c.u32()?;
            let namespace = c.cstring()?;
            let name = c.cstring()?;
            Ok(Message::Type {
                type_oid,
                namespace,
                name,
            })
        }
        b'I' => {
            let rel_oid = c.u32()?;
            let marker = c.u8()?;
            if marker != b'N' {
                return Err(DecodeError::UnknownSubmessage(marker));
            }
            let tuple = decode_tuple(&mut c)?;
            Ok(Message::Insert { rel_oid, tuple })
        }
        b'U' => {
            let rel_oid = c.u32()?;
            let marker = c.u8()?;
            let (old_tuple, new_tuple) = match marker {
                b'K' | b'O' => {
                    let old = decode_tuple(&mut c)?;
                    let n = c.u8()?;
                    if n != b'N' {
                        return Err(DecodeError::UnknownSubmessage(n));
                    }
                    (Some(old), decode_tuple(&mut c)?)
                }
                b'N' => (None, decode_tuple(&mut c)?),
                other => return Err(DecodeError::UnknownSubmessage(other)),
            };
            Ok(Message::Update {
                rel_oid,
                old_tuple,
                new_tuple,
            })
        }
        b'D' => {
            let rel_oid = c.u32()?;
            let marker = c.u8()?;
            if marker != b'K' && marker != b'O' {
                return Err(DecodeError::UnknownSubmessage(marker));
            }
            let old_tuple = decode_tuple(&mut c)?;
            Ok(Message::Delete { rel_oid, old_tuple })
        }
        b'T' => {
            let nrels = c.u32()?;
            let flags = c.u8()?;
            let mut rel_oids = Vec::with_capacity(nrels as usize);
            for _ in 0..nrels {
                rel_oids.push(c.u32()?);
            }
            Ok(Message::Truncate { flags, rel_oids })
        }
        other => Err(DecodeError::UnknownTag(other)),
    }
}

/// Decodes a `TupleData` block: a u16 column count then one column per count.
fn decode_tuple(c: &mut Cursor<'_>) -> Result<Vec<TupleCol>, DecodeError> {
    let ncols = c.u16()?;
    let mut cols = Vec::with_capacity(ncols as usize);
    for _ in 0..ncols {
        let kind = c.u8()?;
        match kind {
            b'n' => cols.push(TupleCol::Null),
            b'u' => cols.push(TupleCol::UnchangedToast),
            b't' => {
                let len = c.i32()?;
                let bytes = c.take(len as usize)?;
                let text = std::str::from_utf8(bytes)
                    .map(str::to_owned)
                    .map_err(|_| DecodeError::InvalidUtf8)?;
                cols.push(TupleCol::Text(text));
            }
            other => return Err(DecodeError::UnknownTupleKind(other)),
        }
    }
    Ok(cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test-only pgoutput encoder helpers, mirroring the wire layout the decoder
    /// reads. Used to build hand-constructed fixtures.
    mod enc {
        pub fn u16(v: u16) -> Vec<u8> {
            v.to_be_bytes().to_vec()
        }
        pub fn u32(v: u32) -> Vec<u8> {
            v.to_be_bytes().to_vec()
        }
        pub fn i32(v: i32) -> Vec<u8> {
            v.to_be_bytes().to_vec()
        }
        pub fn u64(v: u64) -> Vec<u8> {
            v.to_be_bytes().to_vec()
        }
        pub fn i64(v: i64) -> Vec<u8> {
            v.to_be_bytes().to_vec()
        }
        pub fn cstr(s: &str) -> Vec<u8> {
            let mut b = s.as_bytes().to_vec();
            b.push(0);
            b
        }
        pub fn text_col(s: &str) -> Vec<u8> {
            let mut b = vec![b't'];
            b.extend(i32(s.len() as i32));
            b.extend_from_slice(s.as_bytes());
            b
        }
    }

    fn cat(parts: &[Vec<u8>]) -> Vec<u8> {
        parts.iter().flatten().copied().collect()
    }

    #[test]
    fn decodes_begin_and_converts_timestamp_to_unix_micros() {
        // commit_ts raw = 0 (PG epoch) -> unix micros = PG_EPOCH_UNIX_MICROS.
        let buf = cat(&[vec![b'B'], enc::u64(0x1234), enc::i64(0), enc::u32(42)]);
        assert_eq!(
            decode(&buf).expect("decode"),
            Message::Begin {
                final_lsn: 0x1234,
                commit_ts: PG_EPOCH_UNIX_MICROS,
                xid: 42,
            }
        );
    }

    #[test]
    fn decodes_commit() {
        let buf = cat(&[
            vec![b'C'],
            vec![0u8],
            enc::u64(0x1000),
            enc::u64(0x1008),
            enc::i64(5),
        ]);
        assert_eq!(
            decode(&buf).expect("decode"),
            Message::Commit {
                flags: 0,
                commit_lsn: 0x1000,
                end_lsn: 0x1008,
                commit_ts: PG_EPOCH_UNIX_MICROS + 5,
            }
        );
    }

    #[test]
    fn decodes_multi_column_relation_with_typmod() {
        let buf = cat(&[
            vec![b'R'],
            enc::u32(16384),
            enc::cstr("public"),
            enc::cstr("orders"),
            vec![b'd'], // replica identity default
            enc::u16(2),
            // col 0: id int4, key column
            vec![1u8],
            enc::cstr("id"),
            enc::u32(23),
            enc::i32(-1),
            // col 1: amount numeric(10,2): typmod = ((10<<16)|2)+4
            vec![0u8],
            enc::cstr("amount"),
            enc::u32(1700),
            enc::i32(((10i32 << 16) | 2) + 4),
        ]);
        let msg = decode(&buf).expect("decode");
        let Message::Relation(rel) = msg else {
            panic!("expected relation");
        };
        assert_eq!(rel.rel_oid, 16384);
        assert_eq!(rel.namespace, "public");
        assert_eq!(rel.rel_name, "orders");
        assert_eq!(rel.replica_identity, b'd');
        assert_eq!(rel.columns.len(), 2);
        assert_eq!(rel.columns[0].name, "id");
        assert_eq!(rel.columns[0].flags, 1);
        assert_eq!(rel.columns[0].type_oid, 23);
        assert_eq!(rel.columns[0].type_mod, -1);
        assert_eq!(rel.columns[1].name, "amount");
        assert_eq!(rel.columns[1].type_oid, 1700);
        assert_eq!(rel.columns[1].type_mod, ((10i32 << 16) | 2) + 4);
    }

    #[test]
    fn decodes_insert_with_three_tuplecol_kinds() {
        let buf = cat(&[
            vec![b'I'],
            enc::u32(16384),
            vec![b'N'],
            enc::u16(3),
            enc::text_col("42"),
            vec![b'n'], // null
            vec![b'u'], // unchanged toast
        ]);
        assert_eq!(
            decode(&buf).expect("decode"),
            Message::Insert {
                rel_oid: 16384,
                tuple: vec![
                    TupleCol::Text("42".to_owned()),
                    TupleCol::Null,
                    TupleCol::UnchangedToast,
                ],
            }
        );
    }

    #[test]
    fn decodes_update_without_old_tuple() {
        let buf = cat(&[
            vec![b'U'],
            enc::u32(16384),
            vec![b'N'],
            enc::u16(1),
            enc::text_col("new"),
        ]);
        assert_eq!(
            decode(&buf).expect("decode"),
            Message::Update {
                rel_oid: 16384,
                old_tuple: None,
                new_tuple: vec![TupleCol::Text("new".to_owned())],
            }
        );
    }

    #[test]
    fn decodes_update_with_key_old_tuple() {
        let buf = cat(&[
            vec![b'U'],
            enc::u32(16384),
            vec![b'K'],
            enc::u16(1),
            enc::text_col("1"),
            vec![b'N'],
            enc::u16(1),
            enc::text_col("2"),
        ]);
        assert_eq!(
            decode(&buf).expect("decode"),
            Message::Update {
                rel_oid: 16384,
                old_tuple: Some(vec![TupleCol::Text("1".to_owned())]),
                new_tuple: vec![TupleCol::Text("2".to_owned())],
            }
        );
    }

    #[test]
    fn decodes_delete_key_tuple() {
        let buf = cat(&[
            vec![b'D'],
            enc::u32(16384),
            vec![b'K'],
            enc::u16(1),
            enc::text_col("7"),
        ]);
        assert_eq!(
            decode(&buf).expect("decode"),
            Message::Delete {
                rel_oid: 16384,
                old_tuple: vec![TupleCol::Text("7".to_owned())],
            }
        );
    }

    #[test]
    fn decodes_truncate_multiple_relations() {
        let buf = cat(&[
            vec![b'T'],
            enc::u32(2),
            vec![0u8],
            enc::u32(16384),
            enc::u32(16385),
        ]);
        assert_eq!(
            decode(&buf).expect("decode"),
            Message::Truncate {
                flags: 0,
                rel_oids: vec![16384, 16385],
            }
        );
    }

    #[test]
    fn rejects_unknown_tag() {
        assert_eq!(decode(b"Z"), Err(DecodeError::UnknownTag(b'Z')));
    }

    #[test]
    fn rejects_truncated_buffer() {
        // 'B' then only 4 bytes where 8 (u64 lsn) are needed.
        let buf = cat(&[vec![b'B'], vec![0, 0, 0, 1]]);
        assert!(matches!(
            decode(&buf),
            Err(DecodeError::UnexpectedEof { .. })
        ));
    }

    #[test]
    fn rejects_unknown_tuple_kind() {
        let buf = cat(&[vec![b'I'], enc::u32(1), vec![b'N'], enc::u16(1), vec![b'z']]);
        assert_eq!(decode(&buf), Err(DecodeError::UnknownTupleKind(b'z')));
    }
}
