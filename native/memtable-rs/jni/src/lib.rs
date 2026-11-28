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

//! The JNI boundary of the memtable spike: the native methods of
//! `org.apache.cassandra.db.memtable.rust.NativeMemtable`.
//!
//! * [`exports`] holds the `extern "system"` entry points. It is the only
//!   module allowed `unsafe` (the `#[unsafe(no_mangle)]` attribute); each
//!   entry point only forwards to [`bridge`].
//! * [`bridge`] does the work: it upgrades the JNI environment with
//!   `EnvUnowned::with_env`, which wraps the call in `catch_unwind`, copies
//!   arguments out of Java arrays, calls the core, copies results back and
//!   maps errors and panics to Java exceptions ([`error`]).
//! * [`handles`] maps opaque `long` handles to memtables and flush
//!   iterators. Java never sees a pointer.

#![deny(unsafe_code)]

// With the default (glibc) allocator, freeing a flushed memtable on the flush
// thread while writers allocate on theirs measured ~0.4 µs per free (each free
// locks the arena of the thread that allocated the block). mimalloc frees
// cross-thread blocks without that lock; it is optional so both can be measured.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod bridge;
mod error;
pub mod exports;
#[cfg(feature = "ffm-probe")]
mod ffm_probe;
mod handles;
