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

//! Opaque handles for objects Java holds.
//!
//! A handle is a positive `i64` drawn from a counter and never reused, so a
//! stale or forged handle finds nothing and becomes an exception, never a
//! use-after-free. Objects are reference counted (`Arc`):
//!
//! * the memtable table holds one reference until Java calls `release`;
//! * each flush iterator holds another, through its [`FlushIter`];
//!
//! so a memtable's memory is freed only when Java has released it *and*
//! closed every iterator over it, in either order.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use memtable_core::{FlushIter, Memtable};

use crate::error::BoundaryError;

/// Handle tables. Shared by all JNI threads: `Sync` because every field is
/// an atomic or behind a lock.
pub struct Handles {
    next: AtomicI64,
    memtables: RwLock<HashMap<i64, Arc<Memtable>>>,
    /// Each iterator has its own mutex, so the table lock is held only for
    /// the lookup, not while a batch is encoded.
    iterators: Mutex<HashMap<i64, Arc<Mutex<FlushIter>>>>,
}

pub static HANDLES: std::sync::LazyLock<Handles> = std::sync::LazyLock::new(|| Handles {
    next: AtomicI64::new(1),
    memtables: RwLock::default(),
    iterators: Mutex::default(),
});

// The tables hold no invariant that a panic elsewhere could break (an insert
// or remove either happened or not), so a poisoned lock is recovered rather
// than propagated: poisoning here must not take down every memtable.

impl Handles {
    fn next_handle(&self) -> i64 {
        // Relaxed: only uniqueness matters. 2^63 handles cannot run out.
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    pub fn insert_memtable(&self, m: Memtable) -> i64 {
        let h = self.next_handle();
        self.memtables
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(h, Arc::new(m));
        h
    }

    /// Runs `f` on the memtable `h`, holding the table's read lock (not an
    /// extra reference count) for the duration; `release` waits for it.
    pub fn with_memtable<R>(
        &self,
        h: i64,
        f: impl FnOnce(&Arc<Memtable>) -> Result<R, BoundaryError>,
    ) -> Result<R, BoundaryError> {
        let table = self
            .memtables
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let m = table.get(&h).ok_or(BoundaryError::BadHandle(h))?;
        f(m)
    }

    /// Drops the table's reference. The memtable is freed now, or when its
    /// last flush iterator is closed.
    pub fn release_memtable(&self, h: i64) -> Result<(), BoundaryError> {
        let removed = self
            .memtables
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&h);
        // Dropped here, outside the lock: freeing a large memtable takes time.
        removed.map(drop).ok_or(BoundaryError::BadHandle(h))
    }

    pub fn insert_iterator(&self, it: FlushIter) -> i64 {
        let h = self.next_handle();
        self.iterators
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(h, Arc::new(Mutex::new(it)));
        h
    }

    pub fn iterator(&self, h: i64) -> Result<Arc<Mutex<FlushIter>>, BoundaryError> {
        self.iterators
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&h)
            .cloned()
            .ok_or(BoundaryError::BadHandle(h))
    }

    pub fn close_iterator(&self, h: i64) -> Result<(), BoundaryError> {
        let removed = self
            .iterators
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&h);
        removed.map(drop).ok_or(BoundaryError::BadHandle(h))
    }

    /// Live object counts, for leak checks in tests.
    pub fn live_counts(&self) -> (usize, usize) {
        let m = self
            .memtables
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        let i = self
            .iterators
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        (m, i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frozen_memtable() -> Memtable {
        let m = Memtable::new(vec![]).unwrap();
        m.freeze().unwrap();
        m
    }

    /// These tests use the process-wide table, so they check their own
    /// handles rather than global counts (tests run in parallel).
    #[test]
    fn handles_are_unique_and_stale_handles_fail() {
        let a = HANDLES.insert_memtable(Memtable::new(vec![]).unwrap());
        let b = HANDLES.insert_memtable(Memtable::new(vec![]).unwrap());
        assert_ne!(a, b);
        HANDLES.release_memtable(a).unwrap();
        assert!(matches!(HANDLES.release_memtable(a), Err(BoundaryError::BadHandle(h)) if h == a));
        assert!(HANDLES.with_memtable(a, |_| Ok(())).is_err());
        assert!(HANDLES.with_memtable(b, |_| Ok(())).is_ok());
        HANDLES.release_memtable(b).unwrap();
        assert!(HANDLES.iterator(-1).is_err());
        assert!(HANDLES.close_iterator(0).is_err());
    }

    #[test]
    fn iterator_keeps_memtable_alive_after_release() {
        let h = HANDLES.insert_memtable(frozen_memtable());
        let weak = HANDLES.with_memtable(h, |m| Ok(Arc::downgrade(m))).unwrap();
        let it = HANDLES.with_memtable(h, |m| Ok(m.flush_iter()?)).unwrap();
        let ih = HANDLES.insert_iterator(it);
        HANDLES.release_memtable(h).unwrap();
        assert!(weak.upgrade().is_some(), "freed while an iterator was open");
        let mut out = Vec::new();
        let n = HANDLES
            .iterator(ih)
            .unwrap()
            .lock()
            .unwrap()
            .next_batch(&mut out, 1024)
            .unwrap();
        assert_eq!(n, 0);
        HANDLES.close_iterator(ih).unwrap();
        assert!(
            weak.upgrade().is_none(),
            "not freed after release and close"
        );
    }

    #[test]
    fn concurrent_lookups_and_releases() {
        let handles: Vec<i64> = (0..8)
            .map(|_| HANDLES.insert_memtable(Memtable::new(vec![]).unwrap()))
            .collect();
        std::thread::scope(|s| {
            for &h in &handles {
                s.spawn(move || {
                    for _ in 0..if cfg!(miri) { 10 } else { 1000 } {
                        let _ = HANDLES.with_memtable(h, |m| Ok(m.owned_bytes()));
                    }
                    HANDLES.release_memtable(h).unwrap();
                });
            }
        });
        for h in handles {
            assert!(HANDLES.with_memtable(h, |_| Ok(())).is_err());
        }
    }
}
