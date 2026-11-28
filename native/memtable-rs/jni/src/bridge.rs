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

//! The safe bodies of the native methods.
//!
//! Data crosses the boundary only by copy: `GetByteArrayRegion` into a Rust
//! `Vec` on the way in, `SetByteArrayRegion` from one on the way out. No
//! Rust reference into Java memory exists, so the GC may move arrays freely
//! and Java may reuse them as soon as the call returns.
//!
//! Output convention: a method that fills a Java `byte[]` returns the number
//! of bytes written, `0` for "nothing" (absent partition, iterator done), or
//! `-n` if the result needs `n` bytes and the array is shorter; nothing is
//! consumed in that case, so Java grows the array and calls again.

use jni::Env;
use jni::objects::JByteArray;
use jni::sys::{jint, jlong};
use memtable_core::decode_partition;

use crate::error::BoundaryError;
use crate::handles::HANDLES;
use crate::store::Store;

type Result<T> = std::result::Result<T, BoundaryError>;

fn copy_in(env: &Env<'_>, array: &JByteArray<'_>, len: jint) -> Result<Vec<u8>> {
    let len = usize::try_from(len).map_err(|_| BoundaryError::BadArgument("negative length"))?;
    let mut buf = vec![0u8; len];
    array
        .get_region(env, 0, bytemuck::cast_slice_mut(&mut buf))
        .map_err(|e| match e {
            jni::errors::Error::IndexOutOfBounds => {
                BoundaryError::BadArgument("length exceeds array")
            }
            e => e.into(),
        })?;
    Ok(buf)
}

fn copy_out(env: &Env<'_>, array: &JByteArray<'_>, data: &[u8]) -> Result<jint> {
    let n = jint::try_from(data.len())
        .map_err(|_| BoundaryError::BadArgument("result exceeds 2 GiB"))?;
    if data.len() > array.len(env)? {
        return Ok(-n);
    }
    array.set_region(env, 0, bytemuck::cast_slice(data))?;
    Ok(n)
}

/// A cursor over a request buffer.
struct Request<'a>(&'a [u8]);

impl<'a> Request<'a> {
    fn u32(&mut self) -> Result<u32> {
        let (head, tail) = self
            .0
            .split_first_chunk::<4>()
            .ok_or(BoundaryError::BadArgument("request truncated"))?;
        self.0 = tail;
        Ok(u32::from_be_bytes(*head))
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()?;
        self.take(n)
    }

    /// Like `bytes`, but a length of `u32::MAX` (`-1` as a Java int) means
    /// "absent".
    fn optional_bytes(&mut self) -> Result<Option<&'a [u8]>> {
        match self.u32()? {
            u32::MAX => Ok(None),
            n => self.take(n).map(Some),
        }
    }

    fn take(&mut self, n: u32) -> Result<&'a [u8]> {
        let n = usize::try_from(n).map_err(|_| BoundaryError::BadArgument("length"))?;
        let (head, tail) = self
            .0
            .split_at_checked(n)
            .ok_or(BoundaryError::BadArgument("request truncated"))?;
        self.0 = tail;
        Ok(head)
    }

    fn finish(self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(BoundaryError::BadArgument("trailing bytes in request"))
        }
    }
}

/// `create(byte[] request, int len)`: `kind:u32 count:u32 (len:u32 bytes)*`,
/// `kind` 0 for the arena memtable and 1 for the boxed one (see
/// [`crate::store`]), then the shard boundaries.
pub fn create(env: &Env<'_>, request: &JByteArray<'_>, len: jint) -> Result<jlong> {
    let buf = copy_in(env, request, len)?;
    let mut req = Request(&buf);
    let kind = req.u32()?;
    let count = req.u32()?;
    let mut list = Vec::new();
    for _ in 0..count {
        list.push(req.bytes()?.into());
    }
    req.finish()?;
    let store =
        Store::new(kind, list).ok_or(BoundaryError::BadArgument("unknown memtable kind"))??;
    Ok(HANDLES.insert_memtable(store))
}

pub fn release(handle: jlong) -> Result<()> {
    HANDLES.release_memtable(handle)
}

/// `put(long handle, byte[] update, int len)`: one partition record.
/// Returns the change in owned bytes.
pub fn put(env: &Env<'_>, handle: jlong, update: &JByteArray<'_>, len: jint) -> Result<jlong> {
    let buf = copy_in(env, update, len)?;
    let update = decode_partition(&buf)?;
    HANDLES.with_memtable(handle, |m| Ok(m.apply(update)?))
}

/// `read(long handle, byte[] request, int len, byte[] out)`. The request is
/// `key_len:u32 key start_len:u32 start end_len:i32 end`, with
/// `end_len = -1` for an unbounded end.
pub fn read(
    env: &Env<'_>,
    handle: jlong,
    request: &JByteArray<'_>,
    len: jint,
    out: &JByteArray<'_>,
) -> Result<jint> {
    let buf = copy_in(env, request, len)?;
    let mut req = Request(&buf);
    let key = req.bytes()?;
    let start = req.bytes()?;
    let end = req.optional_bytes()?;
    req.finish()?;
    let mut result = Vec::new();
    let found = HANDLES.with_memtable(handle, |m| Ok(m.read(key, start, end, &mut result)?))?;
    if found {
        copy_out(env, out, &result)
    } else {
        Ok(0)
    }
}

pub fn owned_bytes(handle: jlong) -> Result<jlong> {
    HANDLES.with_memtable(handle, |m| Ok(m.owned_bytes()))
}

pub fn partition_count(handle: jlong) -> Result<jlong> {
    HANDLES.with_memtable(handle, |m| {
        jlong::try_from(m.partition_count()).map_err(|_| BoundaryError::BadArgument("count"))
    })
}

pub fn freeze(handle: jlong) -> Result<()> {
    HANDLES.with_memtable(handle, |m| Ok(m.freeze()?))
}

/// Opens a flush iterator; the memtable must be frozen.
pub fn open_flush(handle: jlong) -> Result<jlong> {
    let it = HANDLES.with_memtable(handle, |m| Ok(m.flush_iter()?))?;
    Ok(HANDLES.insert_iterator(it))
}

/// Fills `out` with whole partition records (see the module's output
/// convention).
pub fn flush_next(env: &Env<'_>, iterator: jlong, out: &JByteArray<'_>) -> Result<jint> {
    let capacity = out.len(env)?;
    let it = HANDLES.iterator(iterator)?;
    let mut it = it
        .lock()
        .map_err(|_| memtable_core::MemtableError::Poisoned)?;
    let mut batch = Vec::new();
    let count = it.next_batch(&mut batch, capacity)?;
    if count == 0 && !batch.is_empty() {
        // The next record alone does not fit; it was not consumed.
        let n = jint::try_from(batch.len())
            .map_err(|_| BoundaryError::BadArgument("record exceeds 2 GiB"))?;
        return Ok(-n);
    }
    copy_out(env, out, &batch)
}

pub fn close_flush(iterator: jlong) -> Result<()> {
    HANDLES.close_iterator(iterator)
}

/// `long liveObjects()`: memtables in the high 32 bits, iterators in the low.
pub fn live_objects() -> jlong {
    let (m, i) = HANDLES.live_counts();
    let m = jlong::try_from(m).unwrap_or(jlong::from(u32::MAX));
    let i = jlong::try_from(i).unwrap_or(jlong::from(u32::MAX));
    (m << 32) | i
}

/// `void panicForTest()`: proves a panic becomes a Java exception.
pub fn panic_for_test() -> Result<()> {
    panic!("deliberate panic from panicForTest");
}
