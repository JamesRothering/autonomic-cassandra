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

//! Errors shared by the memtable implementations.

use std::fmt;

use crate::codec::TooLarge;

/// Errors from memtable operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemtableError {
    /// A write arrived after [`crate::Memtable::freeze`].
    Frozen,
    /// A flush iterator was requested before [`crate::Memtable::freeze`].
    NotFrozen,
    /// A thread panicked while holding a shard's write lock.
    Poisoned,
    /// Shard boundaries were not strictly increasing.
    BadBoundaries,
    /// A partition is too large for the wire format.
    TooLarge,
    /// A shard's storage reached an addressing limit (more than about 2^32
    /// items of one kind in one shard). The write that hit it may have been
    /// partly applied, so the shard refuses every later operation, like
    /// [`MemtableError::Poisoned`].
    Full,
}

impl fmt::Display for MemtableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Frozen => "memtable is frozen for flush; writes are not accepted",
            Self::NotFrozen => "memtable must be frozen before it is flushed",
            Self::Poisoned => "a writer panicked; the memtable is unusable",
            Self::BadBoundaries => "shard boundaries must be strictly increasing",
            Self::TooLarge => "partition too large for the wire format",
            Self::Full => "a memtable shard reached its addressing limit and is unusable",
        })
    }
}

impl std::error::Error for MemtableError {}

impl From<TooLarge> for MemtableError {
    fn from(_: TooLarge) -> Self {
        Self::TooLarge
    }
}
