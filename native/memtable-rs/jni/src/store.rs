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

//! The two memtable implementations behind one type, chosen when Java
//! creates a memtable: [`arena`] (the default) or [`boxed`] (spike 1's, kept
//! so the two can be measured in the same build).

use std::sync::Arc;

use memtable_core::{MemtableError, PartitionUpdate, arena, boxed};

/// A memtable as the handle table holds it. Cloning shares it.
#[derive(Clone, Debug)]
pub enum Store {
    Arena(Arc<arena::Memtable>),
    Boxed(Arc<boxed::Memtable>),
}

/// A flush iterator over either kind.
#[derive(Debug)]
pub enum StoreFlush {
    Arena(arena::FlushIter),
    Boxed(boxed::FlushIter),
}

/// The `kind` field of a create request.
pub const KIND_ARENA: u32 = 0;
pub const KIND_BOXED: u32 = 1;

impl Store {
    /// A memtable of `kind`, or `None` for an unknown kind.
    pub fn new(kind: u32, boundaries: Vec<Box<[u8]>>) -> Option<Result<Self, MemtableError>> {
        match kind {
            KIND_ARENA => Some(arena::Memtable::new(boundaries).map(|m| Self::Arena(Arc::new(m)))),
            KIND_BOXED => Some(boxed::Memtable::new(boundaries).map(|m| Self::Boxed(Arc::new(m)))),
            _ => None,
        }
    }

    pub fn apply(&self, update: PartitionUpdate) -> Result<i64, MemtableError> {
        match self {
            Self::Arena(m) => m.apply(update),
            Self::Boxed(m) => m.apply(update),
        }
    }

    pub fn read(
        &self,
        key: &[u8],
        start: &[u8],
        end: Option<&[u8]>,
        out: &mut Vec<u8>,
    ) -> Result<bool, MemtableError> {
        match self {
            Self::Arena(m) => m.read(key, start, end, out),
            Self::Boxed(m) => m.read(key, start, end, out),
        }
    }

    pub fn owned_bytes(&self) -> i64 {
        match self {
            Self::Arena(m) => m.owned_bytes(),
            Self::Boxed(m) => m.owned_bytes(),
        }
    }

    pub fn partition_count(&self) -> usize {
        match self {
            Self::Arena(m) => m.partition_count(),
            Self::Boxed(m) => m.partition_count(),
        }
    }

    pub fn freeze(&self) -> Result<(), MemtableError> {
        match self {
            Self::Arena(m) => m.freeze(),
            Self::Boxed(m) => m.freeze(),
        }
    }

    pub fn flush_iter(&self) -> Result<StoreFlush, MemtableError> {
        match self {
            Self::Arena(m) => m.flush_iter().map(StoreFlush::Arena),
            Self::Boxed(m) => m.flush_iter().map(StoreFlush::Boxed),
        }
    }
}

impl StoreFlush {
    pub fn next_batch(
        &mut self,
        out: &mut Vec<u8>,
        max_bytes: usize,
    ) -> Result<usize, MemtableError> {
        match self {
            Self::Arena(it) => it.next_batch(out, max_bytes),
            Self::Boxed(it) => it.next_batch(out, max_bytes),
        }
    }
}
