//! Decoder for PostgreSQL pgoutput logical replication protocol.
//!
//! Reference: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html

use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Cursor, Read};

use crate::error::{PgError, PgResult};

/// A decoded pgoutput message.
#[derive(Debug, Clone)]
pub enum PgOutputMessage {
    Begin(BeginMessage),
    Commit(CommitMessage),
    Relation(RelationMessage),
    Type(TypeMessage),
    Insert(InsertMessage),
    Update(UpdateMessage),
    Delete(DeleteMessage),
    Truncate(TruncateMessage),
    Origin(OriginMessage),
    Message(LogicalMessage),
}

#[derive(Debug, Clone)]
pub struct BeginMessage {
    pub final_lsn: u64,
    pub timestamp: i64, // microseconds since 2000-01-01
    pub xid: u32,
}

#[derive(Debug, Clone)]
pub struct CommitMessage {
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct RelationMessage {
    pub relation_id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default, // 'd' - default (primary key or unique index)
    Nothing, // 'n' - nothing
    Full,    // 'f' - full (all columns)
    Index,   // 'i' - index
}

impl From<u8> for ReplicaIdentity {
    fn from(b: u8) -> Self {
        match b {
            b'd' => ReplicaIdentity::Default,
            b'n' => ReplicaIdentity::Nothing,
            b'f' => ReplicaIdentity::Full,
            b'i' => ReplicaIdentity::Index,
            _ => ReplicaIdentity::Default,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub flags: u8, // 1 = part of key
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug, Clone)]
pub struct TypeMessage {
    pub type_id: u32,
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct InsertMessage {
    pub relation_id: u32,
    pub tuple: TupleData,
}

#[derive(Debug, Clone)]
pub struct UpdateMessage {
    pub relation_id: u32,
    pub old_tuple: Option<TupleData>,
    pub new_tuple: TupleData,
}

#[derive(Debug, Clone)]
pub struct DeleteMessage {
    pub relation_id: u32,
    pub old_tuple: TupleData,
}

#[derive(Debug, Clone)]
pub struct TruncateMessage {
    pub options: u8,
    pub relation_ids: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct OriginMessage {
    pub origin_lsn: u64,
    pub origin_name: String,
}

#[derive(Debug, Clone)]
pub struct LogicalMessage {
    pub flags: u8,
    pub lsn: u64,
    pub prefix: String,
    pub content: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct TupleData {
    pub columns: Vec<ColumnValue>,
}

#[derive(Debug, Clone)]
pub enum ColumnValue {
    Null,
    Unchanged, // TOASTed value unchanged
    Text(String),
    Binary(Vec<u8>),
}

/// Decoder for pgoutput binary protocol messages.
pub struct PgOutputDecoder;

impl PgOutputDecoder {
    pub fn new() -> Self {
        Self
    }

    /// Decode a pgoutput message from raw bytes.
    pub fn decode(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        if data.is_empty() {
            return Err(PgError::PgOutput("empty message".into()));
        }

        let msg_type = data[0];
        let payload = &data[1..];

        match msg_type {
            b'B' => self.decode_begin(payload),
            b'C' => self.decode_commit(payload),
            b'R' => self.decode_relation(payload),
            b'Y' => self.decode_type(payload),
            b'I' => self.decode_insert(payload),
            b'U' => self.decode_update(payload),
            b'D' => self.decode_delete(payload),
            b'T' => self.decode_truncate(payload),
            b'O' => self.decode_origin(payload),
            b'M' => self.decode_message(payload),
            other => Err(PgError::PgOutput(format!(
                "unknown message type: {} (0x{:02X})",
                other as char, other
            ))),
        }
    }

    fn decode_begin(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        let mut cursor = Cursor::new(data);
        let final_lsn = cursor.read_u64::<BigEndian>()?;
        let timestamp = cursor.read_i64::<BigEndian>()?;
        let xid = cursor.read_u32::<BigEndian>()?;

        Ok(PgOutputMessage::Begin(BeginMessage {
            final_lsn,
            timestamp,
            xid,
        }))
    }

    fn decode_commit(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        let mut cursor = Cursor::new(data);
        let flags = cursor.read_u8()?;
        let commit_lsn = cursor.read_u64::<BigEndian>()?;
        let end_lsn = cursor.read_u64::<BigEndian>()?;
        let timestamp = cursor.read_i64::<BigEndian>()?;

        Ok(PgOutputMessage::Commit(CommitMessage {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        }))
    }

    fn decode_relation(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        let mut cursor = Cursor::new(data);
        let relation_id = cursor.read_u32::<BigEndian>()?;
        let namespace = self.read_string(&mut cursor)?;
        let name = self.read_string(&mut cursor)?;
        let replica_identity = cursor.read_u8()?.into();
        let num_columns = cursor.read_i16::<BigEndian>()? as usize;

        let mut columns = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            let flags = cursor.read_u8()?;
            let col_name = self.read_string(&mut cursor)?;
            let type_oid = cursor.read_u32::<BigEndian>()?;
            let type_modifier = cursor.read_i32::<BigEndian>()?;

            columns.push(ColumnInfo {
                flags,
                name: col_name,
                type_oid,
                type_modifier,
            });
        }

        Ok(PgOutputMessage::Relation(RelationMessage {
            relation_id,
            namespace,
            name,
            replica_identity,
            columns,
        }))
    }

    fn decode_type(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        let mut cursor = Cursor::new(data);
        let type_id = cursor.read_u32::<BigEndian>()?;
        let namespace = self.read_string(&mut cursor)?;
        let name = self.read_string(&mut cursor)?;

        Ok(PgOutputMessage::Type(TypeMessage {
            type_id,
            namespace,
            name,
        }))
    }

    fn decode_insert(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        let mut cursor = Cursor::new(data);
        let relation_id = cursor.read_u32::<BigEndian>()?;
        let tuple_type = cursor.read_u8()?;

        if tuple_type != b'N' {
            return Err(PgError::PgOutput(format!(
                "expected 'N' for new tuple, got '{}'",
                tuple_type as char
            )));
        }

        let tuple = self.decode_tuple(&mut cursor)?;

        Ok(PgOutputMessage::Insert(InsertMessage {
            relation_id,
            tuple,
        }))
    }

    fn decode_update(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        let mut cursor = Cursor::new(data);
        let relation_id = cursor.read_u32::<BigEndian>()?;

        let first_type = cursor.read_u8()?;
        let (old_tuple, new_tuple) = match first_type {
            b'K' | b'O' => {
                // Old tuple present (K = key, O = old)
                let old = self.decode_tuple(&mut cursor)?;
                let new_type = cursor.read_u8()?;
                if new_type != b'N' {
                    return Err(PgError::PgOutput(format!(
                        "expected 'N' for new tuple, got '{}'",
                        new_type as char
                    )));
                }
                let new = self.decode_tuple(&mut cursor)?;
                (Some(old), new)
            }
            b'N' => {
                // Only new tuple
                let new = self.decode_tuple(&mut cursor)?;
                (None, new)
            }
            other => {
                return Err(PgError::PgOutput(format!(
                    "unexpected tuple type in update: '{}'",
                    other as char
                )));
            }
        };

        Ok(PgOutputMessage::Update(UpdateMessage {
            relation_id,
            old_tuple,
            new_tuple,
        }))
    }

    fn decode_delete(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        let mut cursor = Cursor::new(data);
        let relation_id = cursor.read_u32::<BigEndian>()?;
        let tuple_type = cursor.read_u8()?;

        if tuple_type != b'K' && tuple_type != b'O' {
            return Err(PgError::PgOutput(format!(
                "expected 'K' or 'O' for delete tuple, got '{}'",
                tuple_type as char
            )));
        }

        let old_tuple = self.decode_tuple(&mut cursor)?;

        Ok(PgOutputMessage::Delete(DeleteMessage {
            relation_id,
            old_tuple,
        }))
    }

    fn decode_truncate(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        let mut cursor = Cursor::new(data);
        let num_relations = cursor.read_u32::<BigEndian>()? as usize;
        let options = cursor.read_u8()?;

        let mut relation_ids = Vec::with_capacity(num_relations);
        for _ in 0..num_relations {
            relation_ids.push(cursor.read_u32::<BigEndian>()?);
        }

        Ok(PgOutputMessage::Truncate(TruncateMessage {
            options,
            relation_ids,
        }))
    }

    fn decode_origin(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        let mut cursor = Cursor::new(data);
        let origin_lsn = cursor.read_u64::<BigEndian>()?;
        let origin_name = self.read_string(&mut cursor)?;

        Ok(PgOutputMessage::Origin(OriginMessage {
            origin_lsn,
            origin_name,
        }))
    }

    fn decode_message(&self, data: &[u8]) -> PgResult<PgOutputMessage> {
        let mut cursor = Cursor::new(data);
        let flags = cursor.read_u8()?;
        let lsn = cursor.read_u64::<BigEndian>()?;
        let prefix = self.read_string(&mut cursor)?;
        let content_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut content = vec![0u8; content_len];
        cursor.read_exact(&mut content)?;

        Ok(PgOutputMessage::Message(LogicalMessage {
            flags,
            lsn,
            prefix,
            content,
        }))
    }

    fn decode_tuple(&self, cursor: &mut Cursor<&[u8]>) -> PgResult<TupleData> {
        let num_columns = cursor.read_i16::<BigEndian>()? as usize;
        let mut columns = Vec::with_capacity(num_columns);

        for _ in 0..num_columns {
            let col_type = cursor.read_u8()?;
            let value = match col_type {
                b'n' => ColumnValue::Null,
                b'u' => ColumnValue::Unchanged,
                b't' => {
                    let len = cursor.read_i32::<BigEndian>()? as usize;
                    let mut buf = vec![0u8; len];
                    cursor.read_exact(&mut buf)?;
                    ColumnValue::Text(String::from_utf8_lossy(&buf).into_owned())
                }
                b'b' => {
                    let len = cursor.read_i32::<BigEndian>()? as usize;
                    let mut buf = vec![0u8; len];
                    cursor.read_exact(&mut buf)?;
                    ColumnValue::Binary(buf)
                }
                other => {
                    return Err(PgError::PgOutput(format!(
                        "unknown column value type: '{}' (0x{:02X})",
                        other as char, other
                    )));
                }
            };
            columns.push(value);
        }

        Ok(TupleData { columns })
    }

    /// Read a null-terminated string.
    fn read_string(&self, cursor: &mut Cursor<&[u8]>) -> PgResult<String> {
        let mut bytes = Vec::new();
        loop {
            let b = cursor.read_u8()?;
            if b == 0 {
                break;
            }
            bytes.push(b);
        }
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

impl Default for PgOutputDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_begin() {
        // 'B' + Int64(final_lsn) + Int64(timestamp) + Int32(xid)
        let mut data = vec![b'B'];
        data.extend_from_slice(&0x16B3748u64.to_be_bytes()); // final_lsn
        data.extend_from_slice(&12345678i64.to_be_bytes()); // timestamp
        data.extend_from_slice(&123u32.to_be_bytes()); // xid

        let decoder = PgOutputDecoder::new();
        let msg = decoder.decode(&data).unwrap();

        match msg {
            PgOutputMessage::Begin(b) => {
                assert_eq!(b.xid, 123);
                assert_eq!(b.final_lsn, 0x16B3748);
                assert_eq!(b.timestamp, 12345678);
            }
            _ => panic!("expected Begin message"),
        }
    }

    #[test]
    fn test_decode_commit() {
        let mut data = vec![b'C'];
        data.push(0); // flags
        data.extend_from_slice(&100u64.to_be_bytes()); // commit_lsn
        data.extend_from_slice(&200u64.to_be_bytes()); // end_lsn
        data.extend_from_slice(&12345i64.to_be_bytes()); // timestamp

        let decoder = PgOutputDecoder::new();
        let msg = decoder.decode(&data).unwrap();

        match msg {
            PgOutputMessage::Commit(c) => {
                assert_eq!(c.flags, 0);
                assert_eq!(c.commit_lsn, 100);
                assert_eq!(c.end_lsn, 200);
            }
            _ => panic!("expected Commit message"),
        }
    }

    #[test]
    fn test_decode_relation() {
        let mut data = vec![b'R'];
        data.extend_from_slice(&16384u32.to_be_bytes()); // relation_id
        data.extend_from_slice(b"public\0"); // namespace
        data.extend_from_slice(b"users\0"); // name
        data.push(b'd'); // replica identity (default)
        data.extend_from_slice(&2i16.to_be_bytes()); // num columns

        // Column 1: id
        data.push(1); // flags (part of key)
        data.extend_from_slice(b"id\0"); // name
        data.extend_from_slice(&23u32.to_be_bytes()); // type_oid (int4)
        data.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier

        // Column 2: name
        data.push(0); // flags
        data.extend_from_slice(b"name\0"); // name
        data.extend_from_slice(&25u32.to_be_bytes()); // type_oid (text)
        data.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier

        let decoder = PgOutputDecoder::new();
        let msg = decoder.decode(&data).unwrap();

        match msg {
            PgOutputMessage::Relation(r) => {
                assert_eq!(r.relation_id, 16384);
                assert_eq!(r.namespace, "public");
                assert_eq!(r.name, "users");
                assert_eq!(r.replica_identity, ReplicaIdentity::Default);
                assert_eq!(r.columns.len(), 2);
                assert_eq!(r.columns[0].name, "id");
                assert_eq!(r.columns[0].type_oid, 23);
                assert_eq!(r.columns[1].name, "name");
            }
            _ => panic!("expected Relation message"),
        }
    }

    #[test]
    fn test_decode_insert() {
        let mut data = vec![b'I'];
        data.extend_from_slice(&16384u32.to_be_bytes()); // relation_id
        data.push(b'N'); // new tuple marker
        data.extend_from_slice(&2i16.to_be_bytes()); // 2 columns
        data.push(b't'); // text value
        data.extend_from_slice(&1i32.to_be_bytes()); // length
        data.push(b'1'); // value "1"
        data.push(b't'); // text value
        data.extend_from_slice(&5i32.to_be_bytes()); // length
        data.extend_from_slice(b"hello"); // value "hello"

        let decoder = PgOutputDecoder::new();
        let msg = decoder.decode(&data).unwrap();

        match msg {
            PgOutputMessage::Insert(i) => {
                assert_eq!(i.relation_id, 16384);
                assert_eq!(i.tuple.columns.len(), 2);
                match &i.tuple.columns[0] {
                    ColumnValue::Text(s) => assert_eq!(s, "1"),
                    _ => panic!("expected text value"),
                }
                match &i.tuple.columns[1] {
                    ColumnValue::Text(s) => assert_eq!(s, "hello"),
                    _ => panic!("expected text value"),
                }
            }
            _ => panic!("expected Insert message"),
        }
    }

    #[test]
    fn test_decode_insert_with_null() {
        let mut data = vec![b'I'];
        data.extend_from_slice(&16384u32.to_be_bytes()); // relation_id
        data.push(b'N'); // new tuple marker
        data.extend_from_slice(&2i16.to_be_bytes()); // 2 columns
        data.push(b't'); // text value
        data.extend_from_slice(&1i32.to_be_bytes()); // length
        data.push(b'1'); // value "1"
        data.push(b'n'); // null value

        let decoder = PgOutputDecoder::new();
        let msg = decoder.decode(&data).unwrap();

        match msg {
            PgOutputMessage::Insert(i) => {
                assert_eq!(i.tuple.columns.len(), 2);
                match &i.tuple.columns[1] {
                    ColumnValue::Null => {}
                    _ => panic!("expected null value"),
                }
            }
            _ => panic!("expected Insert message"),
        }
    }

    #[test]
    fn test_decode_delete() {
        let mut data = vec![b'D'];
        data.extend_from_slice(&16384u32.to_be_bytes()); // relation_id
        data.push(b'K'); // key tuple
        data.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        data.push(b't'); // text value
        data.extend_from_slice(&1i32.to_be_bytes()); // length
        data.push(b'1'); // value "1"

        let decoder = PgOutputDecoder::new();
        let msg = decoder.decode(&data).unwrap();

        match msg {
            PgOutputMessage::Delete(d) => {
                assert_eq!(d.relation_id, 16384);
                assert_eq!(d.old_tuple.columns.len(), 1);
            }
            _ => panic!("expected Delete message"),
        }
    }
}
