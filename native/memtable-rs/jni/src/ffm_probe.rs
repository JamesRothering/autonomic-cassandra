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

//! Benchmark probes for `bench/ffm/FfmProbe.java` (JDK 25): the same work
//! reached through the FFM API (`extern "C"`) and through JNI. Built only with
//! the `ffm-probe` feature; not part of the memtable's interface.

#![allow(unsafe_code)]

use jni::EnvUnowned;
use jni::objects::{JByteArray, JClass};
use jni::sys::{jint, jlong};

use crate::error::{BoundaryError, ThrowJavaException};

/// FFM: returns its argument.
// SAFETY: a C-ABI symbol with a unique name; FfmProbe declares it as
// `(JAVA_LONG) -> JAVA_LONG`, which is `i64 -> i64`.
#[unsafe(no_mangle)]
pub extern "C" fn memtable_ffm_nop(x: i64) -> i64 {
    x
}

/// FFM: sums `len` bytes at `ptr`.
///
/// # Safety
/// `ptr` must be valid for reads of `len` bytes, not mutated during the
/// call. (An `unsafe fn`, so that no safe Rust caller can pass a bad pointer.)
// SAFETY (attribute): a C-ABI symbol with a unique name; FfmProbe declares it
// as `(ADDRESS, JAVA_LONG) -> JAVA_LONG`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn memtable_ffm_sum(ptr: *const u8, len: i64) -> i64 {
    let Ok(len) = usize::try_from(len) else {
        return -1;
    };
    if ptr.is_null() {
        return -1;
    }
    // SAFETY: the function's contract; its only caller, FfmProbe, passes a
    // MemorySegment of at least `len` bytes that it does not mutate during the
    // call (native memory from a confined Arena, or with
    // Linker.Option.critical(true) a heap array the GC holds still). The slice
    // does not outlive the call.
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    bytes.iter().map(|&b| i64::from(b)).sum()
}

// SAFETY: signature contract as in `exports.rs`; Java: `static native long nopRaw(long x)` in class `FfmProbe`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_FfmProbe_nopRaw<'l>(
    _env: EnvUnowned<'l>,
    _class: JClass<'l>,
    x: jlong,
) -> jlong {
    x
}

// SAFETY: signature contract as in `exports.rs`; Java: `static native long sumRaw(byte[] b, int n)` in class `FfmProbe`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_FfmProbe_sumRaw<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    array: JByteArray<'l>,
    len: jint,
) -> jlong {
    env.with_env(|env| {
        let len =
            usize::try_from(len).map_err(|_| BoundaryError::BadArgument("negative length"))?;
        let mut buf = vec![0u8; len];
        array.get_region(env, 0, bytemuck::cast_slice_mut(&mut buf))?;
        Ok::<_, BoundaryError>(buf.iter().map(|&b| i64::from(b)).sum())
    })
    .resolve::<ThrowJavaException>()
}
