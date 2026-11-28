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

//! A spike of a Cassandra memtable in safe Rust.
//!
//! Java encodes partition keys and clusterings with Cassandra's
//! byte-comparable encoding, so this crate orders everything by plain byte
//! comparison and needs no per-type comparators. It stores regular cells,
//! cell tombstones, row deletions and partition deletions, and reconciles
//! them by Cassandra's rules ([`model`]). Data enters and leaves in one
//! binary format ([`codec`]).
//!
//! Two concurrent stores implement the same operations: [`arena`] (the
//! default, exported as [`Memtable`]), which keeps each shard's data in a few
//! large chunks so that discarding it is cheap, and [`boxed`], the first
//! version, with one heap allocation per key, row and value, kept for
//! comparison and as an independent implementation for the tests.
//!
//! Not supported (see `docs/spike/rust-memtable.md`): range tombstones,
//! static rows, complex (collection) columns, counters.

#![forbid(unsafe_code)]

pub mod arena;
pub mod boxed;
mod btree;
pub mod codec;
mod error;
pub mod model;
mod region;
mod rows;

pub use arena::{FlushIter, Memtable, Space};
pub use codec::{DecodeError, PartitionUpdate, decode_partition, decode_partitions, encode_update};
pub use error::MemtableError;
pub use model::{Cell, DeletionTime, LivenessInfo, Row, RowDeletion};
