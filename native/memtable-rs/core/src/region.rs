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

//! Append-only storage in a few large chunks, addressed by indices.
//!
//! The point is the cost of throwing it away: a [`Region`] or [`Pool`] that
//! holds a million items is a few dozen `Vec`s, so dropping it is a few dozen
//! frees instead of a million. Nothing is ever removed or moved; an item
//! that is no longer referenced (an overwritten value, say) stays until the
//! whole store is dropped. That waste is what [`Region::used_bytes`] minus a
//! walk of the live data measures.
//!
//! Items are reached by index, not by reference, so there is no lifetime
//! tying the data structure to its storage and no `unsafe`: an index is
//! checked by ordinary slice indexing. An index is only ever produced by the
//! store it indexes, so an out-of-range index is a bug, and it panics (which
//! the memtable turns into a poisoned shard) rather than reading the wrong
//! item.
//!
//! Chunks start small and grow by a quarter each time, up to
//! [`MAX_CHUNK_BYTES`], so a store that holds little costs little (a memtable
//! has hundreds of shards, each with several stores) and a large one has few
//! chunks. The growth factor trades chunk count against slack: the last chunk
//! is partly empty, and with doubling it is half of everything allocated
//! (27-44% slack measured in the accounting test); with 1.25 it is a fifth. A
//! chunk's `Vec` is created with its final capacity and never grows, so
//! nothing is ever copied.

use std::mem::size_of;

/// The first chunk of a store, in bytes.
pub const MIN_CHUNK_BYTES: usize = 1 << 10;
/// Chunks stop doubling at this size; a single slice larger than a quarter
/// of it gets a chunk of its own.
pub const MAX_CHUNK_BYTES: usize = 1 << 20;

/// A store ran out of indices (see [`crate::MemtableError::Full`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Full;

/// The location of a slice in a [`Region`]: 12 bytes, `Copy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Span {
    chunk: u32,
    start: u32,
    len: u32,
}

impl Span {
    /// The empty slice; valid in every region.
    pub const EMPTY: Span = Span {
        chunk: 0,
        start: 0,
        len: 0,
    };

    /// Number of items.
    #[must_use]
    pub fn len(self) -> usize {
        self.len as usize
    }

    /// The first `n` items of this span. `n` must not exceed `len`.
    #[must_use]
    pub fn prefix(self, n: usize) -> Span {
        let n = u32::try_from(n).unwrap_or(u32::MAX).min(self.len);
        Span { len: n, ..self }
    }
}

/// Elements per chunk for a store of `T` whose chunks are `bytes` large
/// (at least one).
fn elements(bytes: usize, size: usize) -> usize {
    (bytes / size.max(1)).max(1)
}

/// The capacity of the chunk after one of `capacity`, at most `max`.
fn grow(capacity: usize, max: usize) -> usize {
    (capacity + (capacity / 4).max(1)).min(max)
}

/// Append-only storage of slices of `T`.
#[derive(Debug)]
pub struct Region<T> {
    chunks: Vec<Vec<T>>,
    /// The chunk new slices go into, if any.
    current: Option<usize>,
    /// Capacity, in elements, of the next regular chunk.
    next_capacity: usize,
    allocated: usize,
    used: usize,
}

impl<T> Default for Region<T> {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            current: None,
            next_capacity: elements(MIN_CHUNK_BYTES, size_of::<T>()),
            allocated: 0,
            used: 0,
        }
    }
}

impl<T: Copy> Region<T> {
    /// Copies `items` in and returns where they are.
    ///
    /// # Errors
    /// [`Full`] if the region already has `u32::MAX` chunks or `items` is
    /// longer than `u32::MAX`.
    pub fn push(&mut self, items: &[T]) -> Result<Span, Full> {
        if items.is_empty() {
            return Ok(Span::EMPTY);
        }
        let len = u32::try_from(items.len()).map_err(|_| Full)?;
        let max_elements = elements(MAX_CHUNK_BYTES, size_of::<T>());
        let index = if items.len() > max_elements / 4 {
            // A large slice gets an exact chunk of its own; the current
            // chunk stays current.
            self.new_chunk(items.len())?
        } else {
            match self.current {
                Some(i) if self.chunks[i].capacity() - self.chunks[i].len() >= items.len() => i,
                _ => {
                    let capacity = self.next_capacity;
                    self.next_capacity = grow(capacity, max_elements);
                    let i = self.new_chunk(capacity)?;
                    self.current = Some(i);
                    i
                }
            }
        };
        let chunk = &mut self.chunks[index];
        let start = u32::try_from(chunk.len()).map_err(|_| Full)?;
        chunk.extend_from_slice(items);
        self.used += items.len();
        Ok(Span {
            chunk: u32::try_from(index).map_err(|_| Full)?,
            start,
            len,
        })
    }

    fn new_chunk(&mut self, capacity: usize) -> Result<usize, Full> {
        let index = self.chunks.len();
        u32::try_from(index).map_err(|_| Full)?;
        self.chunks.push(Vec::with_capacity(capacity));
        self.allocated += capacity;
        Ok(index)
    }

    /// The items at `span`.
    #[must_use]
    pub fn get(&self, span: Span) -> &[T] {
        if span.len == 0 {
            return &[];
        }
        let start = span.start as usize;
        &self.chunks[span.chunk as usize][start..start + span.len as usize]
    }

    /// The items at `span`, for overwriting in place.
    pub fn get_mut(&mut self, span: Span) -> &mut [T] {
        if span.len == 0 {
            return &mut [];
        }
        let start = span.start as usize;
        &mut self.chunks[span.chunk as usize][start..start + span.len as usize]
    }
}

impl<T> Region<T> {
    /// Bytes allocated for this region: chunk capacities and the chunk list.
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        self.allocated * size_of::<T>() + self.chunks.capacity() * size_of::<Vec<T>>()
    }

    /// Bytes of items ever pushed (live or not).
    #[must_use]
    pub fn used_bytes(&self) -> usize {
        self.used * size_of::<T>()
    }

    /// Number of chunks (each one heap allocation).
    #[must_use]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}

/// Bits of a [`Pool`] index that address the slot within a chunk.
const SLOT_BITS: u32 = 16;
const SLOT_MASK: u32 = (1 << SLOT_BITS) - 1;
/// Chunk numbers stop one short of the top, so `u32::MAX` is never an index.
const MAX_POOL_CHUNKS: usize = (1 << (32 - SLOT_BITS)) - 1;

/// An index that no [`Pool`] ever returns.
pub const NIL: u32 = u32::MAX;

/// Append-only storage of single items addressed by a 4-byte index (chunk
/// number in the high 16 bits, slot in the low 16): at most 65,535 chunks of
/// at most 65,536 items.
#[derive(Debug)]
pub struct Pool<T> {
    chunks: Vec<Vec<T>>,
    next_capacity: usize,
    allocated: usize,
    len: usize,
}

impl<T> Default for Pool<T> {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            next_capacity: elements(MIN_CHUNK_BYTES, size_of::<T>()).min(Self::max_capacity()),
            allocated: 0,
            len: 0,
        }
    }
}

impl<T> Pool<T> {
    const fn max_capacity() -> usize {
        let by_size = MAX_CHUNK_BYTES
            / if size_of::<T>() == 0 {
                1
            } else {
                size_of::<T>()
            };
        let by_index = 1 << SLOT_BITS;
        if by_size < by_index {
            by_size
        } else {
            by_index
        }
    }

    /// Stores `item` and returns its index, never [`NIL`].
    ///
    /// # Errors
    /// [`Full`] after 65,535 full chunks.
    pub fn push(&mut self, item: T) -> Result<u32, Full> {
        let needs_chunk = self.chunks.last().is_none_or(|c| c.len() == c.capacity());
        if needs_chunk {
            if self.chunks.len() >= MAX_POOL_CHUNKS {
                return Err(Full);
            }
            let capacity = self.next_capacity;
            self.next_capacity = grow(capacity, Self::max_capacity());
            self.chunks.push(Vec::with_capacity(capacity));
            self.allocated += capacity;
        }
        let chunk_index = self.chunks.len() - 1;
        let chunk = &mut self.chunks[chunk_index];
        let slot = chunk.len();
        chunk.push(item);
        self.len += 1;
        // Both fit: chunk_index < MAX_POOL_CHUNKS < 2^16, slot < 2^16.
        let chunk_index = u32::try_from(chunk_index).map_err(|_| Full)?;
        let slot = u32::try_from(slot).map_err(|_| Full)?;
        Ok((chunk_index << SLOT_BITS) | slot)
    }

    #[must_use]
    pub fn get(&self, index: u32) -> &T {
        &self.chunks[(index >> SLOT_BITS) as usize][(index & SLOT_MASK) as usize]
    }

    pub fn get_mut(&mut self, index: u32) -> &mut T {
        &mut self.chunks[(index >> SLOT_BITS) as usize][(index & SLOT_MASK) as usize]
    }

    /// Number of items stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Bytes allocated: chunk capacities and the chunk list.
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        self.allocated * size_of::<T>() + self.chunks.capacity() * size_of::<Vec<T>>()
    }

    /// Bytes of items stored.
    #[must_use]
    pub fn used_bytes(&self) -> usize {
        self.len * size_of::<T>()
    }

    #[must_use]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_round_trips_and_grows_by_doubling() {
        let mut r = Region::<u8>::default();
        let mut spans = Vec::new();
        for i in 0..if cfg!(miri) { 300 } else { 5000u32 } {
            let v: Vec<u8> = (0..i % 300)
                .map(|j| u8::try_from((i + j) % 251).unwrap())
                .collect();
            spans.push((r.push(&v).unwrap(), v));
        }
        for (s, v) in &spans {
            assert_eq!(r.get(*s), &v[..]);
        }
        assert_eq!(
            r.used_bytes(),
            spans.iter().map(|(_, v)| v.len()).sum::<usize>()
        );
        assert!(r.allocated_bytes() >= r.used_bytes());
        // ~750 KB in chunks of 1 KiB growing by a quarter: about 26 chunks.
        assert!(r.chunk_count() < 32, "{} chunks", r.chunk_count());
        assert!(r.used_bytes() * 10 >= r.allocated_bytes() * 7);
        assert_eq!(r.get(Span::EMPTY), &[] as &[u8]);
    }

    #[test]
    fn large_slices_get_their_own_chunk() {
        let mut r = Region::<u8>::default();
        let small = r.push(b"abc").unwrap();
        let big = vec![7u8; MAX_CHUNK_BYTES / 2];
        let b = r.push(&big).unwrap();
        let after = r.push(b"de").unwrap();
        assert_eq!(r.get(b), &big[..]);
        assert_eq!(r.get(small), b"abc");
        // The small slices share the first chunk.
        assert_eq!(after.chunk, small.chunk);
        r.get_mut(after).copy_from_slice(b"xy");
        assert_eq!(r.get(after), b"xy");
        assert_eq!(r.get(after.prefix(1)), b"x");
    }

    #[test]
    fn pool_indices_are_stable_and_never_nil() {
        let mut p = Pool::<u64>::default();
        let n: u64 = if cfg!(miri) { 2_000 } else { 200_000 };
        let ids: Vec<u32> = (0..n).map(|i| p.push(i * 3).unwrap()).collect();
        for (i, id) in ids.iter().enumerate() {
            assert_ne!(*id, NIL);
            assert_eq!(*p.get(*id), u64::try_from(i).unwrap() * 3);
        }
        *p.get_mut(ids[7]) = 1;
        assert_eq!(*p.get(ids[7]), 1);
        assert_eq!(p.len(), usize::try_from(n).unwrap());
        assert!(p.chunk_count() < 40, "{} chunks", p.chunk_count());
    }
}
