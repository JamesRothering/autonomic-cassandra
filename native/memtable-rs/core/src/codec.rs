// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The binary format that crosses the Java/Rust boundary.
//!
//! One format, a *partition record*, is used in both directions: Java sends
//! an update as one record, a point read returns at most one, and a flush
//! batch is a sequence of them. All integers are big-endian (Java's
//! `ByteBuffer` default).
//!
//! ```text
//! partition := key_len:u32 key[key_len] deletion row_count:u32 row*
//! row       := clustering_len:u32 clustering[clustering_len]
//!              liveness deletion shadowable:u8 cell_count:u32 cell*
//! cell      := column:u32 timestamp:i64 ttl:i32 local_deletion_time:i64
//!              value_len:u32 value[value_len]
//! deletion  := marked_for_delete_at:i64 local_deletion_time:i64
//! liveness  := timestamp:i64 ttl:i32 local_expiration_time:i64
//! ```
//!
//! Keys and clusterings are Cassandra byte-comparable encodings
//! (`utils/bytecomparable/ByteComparable.md`), so ordering is `memcmp`.
//! Rows must be strictly increasing by clustering and cells strictly
//! increasing by column; the decoder rejects anything else, and every value
//! Java could not have built (see the `is_valid` methods in [`crate::model`]).

use std::fmt;

use crate::model::{Cell, DeletionTime, LivenessInfo, Row, RowDeletion};

/// A decoded update for one partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionUpdate {
    pub key: Box<[u8]>,
    pub deletion: DeletionTime,
    /// Strictly increasing by clustering.
    pub rows: Vec<(Box<[u8]>, Row)>,
}

/// Why an input record was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The input ended before the named field.
    Truncated(&'static str),
    /// Bytes were left after the record.
    TrailingBytes(usize),
    /// Rows not strictly increasing by clustering.
    RowsOutOfOrder,
    /// Cells not strictly increasing by column.
    CellsOutOfOrder,
    /// A value Java could not have built.
    Invalid(&'static str),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated(field) => write!(f, "record truncated at {field}"),
            Self::TrailingBytes(n) => write!(f, "{n} trailing bytes after record"),
            Self::RowsOutOfOrder => f.write_str("rows not strictly increasing by clustering"),
            Self::CellsOutOfOrder => f.write_str("cells not strictly increasing by column"),
            Self::Invalid(what) => write!(f, "invalid {what}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// A cursor over input bytes. Every read checks the remaining length before
/// allocating, so a corrupt length cannot cause a large allocation.
struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize, field: &'static str) -> Result<&'a [u8], DecodeError> {
        let (head, tail) = self
            .buf
            .split_at_checked(n)
            .ok_or(DecodeError::Truncated(field))?;
        self.buf = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], DecodeError> {
        let (head, tail) = self
            .buf
            .split_first_chunk::<N>()
            .ok_or(DecodeError::Truncated(field))?;
        self.buf = tail;
        Ok(*head)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, DecodeError> {
        Ok(self.array::<1>(field)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, DecodeError> {
        self.array(field).map(u32::from_be_bytes)
    }

    fn i32(&mut self, field: &'static str) -> Result<i32, DecodeError> {
        self.array(field).map(i32::from_be_bytes)
    }

    fn i64(&mut self, field: &'static str) -> Result<i64, DecodeError> {
        self.array(field).map(i64::from_be_bytes)
    }

    fn len(&mut self, field: &'static str) -> Result<usize, DecodeError> {
        // u32 always fits in usize on the 32- and 64-bit targets we build for.
        usize::try_from(self.u32(field)?).map_err(|_| DecodeError::Invalid(field))
    }

    fn bytes(&mut self, field: &'static str) -> Result<Box<[u8]>, DecodeError> {
        let n = self.len(field)?;
        Ok(self.take(n, field)?.into())
    }

    fn deletion(&mut self) -> Result<DeletionTime, DecodeError> {
        let d = DeletionTime {
            marked_for_delete_at: self.i64("deletion.marked_for_delete_at")?,
            local_deletion_time: self.i64("deletion.local_deletion_time")?,
        };
        if d.is_valid() {
            Ok(d)
        } else {
            Err(DecodeError::Invalid("deletion time"))
        }
    }

    fn liveness(&mut self) -> Result<LivenessInfo, DecodeError> {
        let l = LivenessInfo {
            timestamp: self.i64("liveness.timestamp")?,
            ttl: self.i32("liveness.ttl")?,
            local_expiration_time: self.i64("liveness.local_expiration_time")?,
        };
        if l.is_valid() {
            Ok(l)
        } else {
            Err(DecodeError::Invalid("liveness info"))
        }
    }

    fn cell(&mut self) -> Result<Cell, DecodeError> {
        let c = Cell {
            column: self.u32("cell.column")?,
            timestamp: self.i64("cell.timestamp")?,
            ttl: self.i32("cell.ttl")?,
            local_deletion_time: self.i64("cell.local_deletion_time")?,
            value: self.bytes("cell.value")?,
        };
        if c.is_valid() {
            Ok(c)
        } else {
            Err(DecodeError::Invalid("cell"))
        }
    }

    fn row(&mut self) -> Result<(Box<[u8]>, Row), DecodeError> {
        let clustering = self.bytes("row.clustering")?;
        let liveness = self.liveness()?;
        let time = self.deletion()?;
        let deletion = RowDeletion {
            time,
            shadowable: match self.u8("row.shadowable")? {
                0 => false,
                1 => true,
                _ => return Err(DecodeError::Invalid("shadowable flag")),
            },
        };
        if !deletion.is_valid() {
            return Err(DecodeError::Invalid("row deletion"));
        }
        let count = self.len("row.cell_count")?;
        // Do not trust `count` for the allocation: a cell is at least 28 bytes.
        let mut cells = Vec::with_capacity(count.min(self.buf.len() / 28));
        for _ in 0..count {
            let cell = self.cell()?;
            if cells.last().is_some_and(|prev: &Cell| prev.column >= cell.column) {
                return Err(DecodeError::CellsOutOfOrder);
            }
            cells.push(cell);
        }
        Ok((
            clustering,
            Row {
                liveness,
                deletion,
                cells,
            },
        ))
    }

    fn partition(&mut self) -> Result<PartitionUpdate, DecodeError> {
        let key = self.bytes("partition.key")?;
        let deletion = self.deletion()?;
        let count = self.len("partition.row_count")?;
        // A row is at least 45 bytes.
        let mut rows: Vec<(Box<[u8]>, Row)> = Vec::with_capacity(count.min(self.buf.len() / 45));
        for _ in 0..count {
            let row = self.row()?;
            if rows.last().is_some_and(|(prev, _)| *prev >= row.0) {
                return Err(DecodeError::RowsOutOfOrder);
            }
            rows.push(row);
        }
        Ok(PartitionUpdate {
            key,
            deletion,
            rows,
        })
    }
}

/// Decodes exactly one partition record.
///
/// # Errors
/// Returns a [`DecodeError`] if the bytes are not exactly one valid record.
pub fn decode_partition(buf: &[u8]) -> Result<PartitionUpdate, DecodeError> {
    let mut reader = Reader { buf };
    let update = reader.partition()?;
    if reader.buf.is_empty() {
        Ok(update)
    } else {
        Err(DecodeError::TrailingBytes(reader.buf.len()))
    }
}

/// Decodes a concatenation of partition records (a flush batch).
///
/// # Errors
/// Returns a [`DecodeError`] if any record is invalid.
pub fn decode_partitions(buf: &[u8]) -> Result<Vec<PartitionUpdate>, DecodeError> {
    let mut reader = Reader { buf };
    let mut out = Vec::new();
    while !reader.buf.is_empty() {
        out.push(reader.partition()?);
    }
    Ok(out)
}

/// Why a record could not be encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TooLarge;

impl fmt::Display for TooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a length does not fit the format's u32")
    }
}

impl std::error::Error for TooLarge {}

fn put_len(out: &mut Vec<u8>, n: usize) -> Result<(), TooLarge> {
    out.extend_from_slice(&u32::try_from(n).map_err(|_| TooLarge)?.to_be_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) -> Result<(), TooLarge> {
    put_len(out, b.len())?;
    out.extend_from_slice(b);
    Ok(())
}

fn put_deletion(out: &mut Vec<u8>, d: DeletionTime) {
    out.extend_from_slice(&d.marked_for_delete_at.to_be_bytes());
    out.extend_from_slice(&d.local_deletion_time.to_be_bytes());
}

fn put_row(out: &mut Vec<u8>, clustering: &[u8], row: &Row) -> Result<(), TooLarge> {
    put_bytes(out, clustering)?;
    out.extend_from_slice(&row.liveness.timestamp.to_be_bytes());
    out.extend_from_slice(&row.liveness.ttl.to_be_bytes());
    out.extend_from_slice(&row.liveness.local_expiration_time.to_be_bytes());
    put_deletion(out, row.deletion.time);
    out.push(u8::from(row.deletion.shadowable));
    put_len(out, row.cells.len())?;
    for c in &row.cells {
        out.extend_from_slice(&c.column.to_be_bytes());
        out.extend_from_slice(&c.timestamp.to_be_bytes());
        out.extend_from_slice(&c.ttl.to_be_bytes());
        out.extend_from_slice(&c.local_deletion_time.to_be_bytes());
        put_bytes(out, &c.value)?;
    }
    Ok(())
}

/// Appends one partition record to `out`. The row count is written after the
/// rows, into a placeholder, so `rows` can be a lazy range.
///
/// # Errors
/// Returns [`TooLarge`] if a length exceeds `u32::MAX`; `out` then holds a
/// partial record and the caller must discard it.
pub fn encode_partition<'a>(
    out: &mut Vec<u8>,
    key: &[u8],
    deletion: DeletionTime,
    rows: impl IntoIterator<Item = (&'a [u8], &'a Row)>,
) -> Result<(), TooLarge> {
    put_bytes(out, key)?;
    put_deletion(out, deletion);
    let count_at = out.len();
    out.extend_from_slice(&[0; 4]);
    let mut count = 0usize;
    for (clustering, row) in rows {
        put_row(out, clustering, row)?;
        count += 1;
    }
    let count = u32::try_from(count).map_err(|_| TooLarge)?.to_be_bytes();
    out[count_at..count_at + 4].copy_from_slice(&count);
    Ok(())
}

/// Encodes a [`PartitionUpdate`] (used by tests and fuzzing).
///
/// # Errors
/// Returns [`TooLarge`] if a length exceeds `u32::MAX`.
pub fn encode_update(out: &mut Vec<u8>, update: &PartitionUpdate) -> Result<(), TooLarge> {
    encode_partition(
        out,
        &update.key,
        update.deletion,
        update.rows.iter().map(|(c, r)| (&**c, r)),
    )
}
