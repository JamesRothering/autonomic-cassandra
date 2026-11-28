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

//! The exported native methods of
//! `org.apache.cassandra.db.memtable.rust.NativeMemtable`.
//!
//! This is the only module that may use `unsafe`, and it uses it only for
//! `#[unsafe(no_mangle)]`. The JVM finds each function by its mangled name
//! and calls it with the C ABI and the argument types of the Java
//! declaration; nothing checks that the Rust signature matches. Every
//! function below therefore carries the same obligation, stated once here
//! and referenced by each `SAFETY` comment:
//!
//! **SAFETY (signature contract):** the name is unique in the process (it
//! carries the full Java class name), and the parameter and return types
//! match the `native` declaration in `NativeMemtable.java` one for one:
//! `JNIEnv*`, `jclass` (static method), then each Java parameter
//! (`long` -> `jlong`, `int` -> `jint`, `byte[]` -> `JByteArray`, which is
//! `#[repr(transparent)]` over `jobject`), and the Java return type.
//! `NativeMemtableTest` calls every method, so a missing or misspelt
//! symbol fails that test with `UnsatisfiedLinkError`; a wrong parameter
//! type would not be detected and is the residual risk a reviewer must
//! check by eye.
//!
//! Each body calls `with_env`, which catches panics, and resolves the outcome
//! with [`ThrowJavaException`], so no panic or error unwinds into the JVM.

#![allow(unsafe_code)]

use jni::EnvUnowned;
use jni::objects::{JByteArray, JClass};
use jni::sys::{jint, jlong};

use crate::bridge;
use crate::error::{BoundaryError, ThrowJavaException};

// SAFETY: signature contract (module docs); Java: `static native long create(byte[] boundaries, int len)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_create<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    boundaries: JByteArray<'l>,
    len: jint,
) -> jlong {
    env.with_env(|env| bridge::create(env, &boundaries, len))
        .resolve::<ThrowJavaException>()
}

// SAFETY: signature contract (module docs); Java: `static native void release(long handle)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_release<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    handle: jlong,
) {
    env.with_env(|_| bridge::release(handle))
        .resolve::<ThrowJavaException>();
}

// SAFETY: signature contract (module docs); Java: `static native long put(long handle, byte[] update, int len)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_put<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    handle: jlong,
    update: JByteArray<'l>,
    len: jint,
) -> jlong {
    env.with_env(|env| bridge::put(env, handle, &update, len))
        .resolve::<ThrowJavaException>()
}

// SAFETY: signature contract (module docs); Java: `static native int read(long handle, byte[] request, int len, byte[] out)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_read<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    handle: jlong,
    request: JByteArray<'l>,
    len: jint,
    out: JByteArray<'l>,
) -> jint {
    env.with_env(|env| bridge::read(env, handle, &request, len, &out))
        .resolve::<ThrowJavaException>()
}

// SAFETY: signature contract (module docs); Java: `static native long ownedBytes(long handle)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_ownedBytes<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    handle: jlong,
) -> jlong {
    env.with_env(|_| bridge::owned_bytes(handle))
        .resolve::<ThrowJavaException>()
}

// SAFETY: signature contract (module docs); Java: `static native long partitionCount(long handle)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_partitionCount<
    'l,
>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    handle: jlong,
) -> jlong {
    env.with_env(|_| bridge::partition_count(handle))
        .resolve::<ThrowJavaException>()
}

// SAFETY: signature contract (module docs); Java: `static native void freeze(long handle)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_freeze<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    handle: jlong,
) {
    env.with_env(|_| bridge::freeze(handle))
        .resolve::<ThrowJavaException>();
}

// SAFETY: signature contract (module docs); Java: `static native long openFlush(long handle)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_openFlush<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    handle: jlong,
) -> jlong {
    env.with_env(|_| bridge::open_flush(handle))
        .resolve::<ThrowJavaException>()
}

// SAFETY: signature contract (module docs); Java: `static native int flushNext(long iterator, byte[] out)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_flushNext<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    iterator: jlong,
    out: JByteArray<'l>,
) -> jint {
    env.with_env(|env| bridge::flush_next(env, iterator, &out))
        .resolve::<ThrowJavaException>()
}

// SAFETY: signature contract (module docs); Java: `static native void closeFlush(long iterator)`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_closeFlush<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    iterator: jlong,
) {
    env.with_env(|_| bridge::close_flush(iterator))
        .resolve::<ThrowJavaException>();
}

// SAFETY: signature contract (module docs); Java: `static native long liveObjects()`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_liveObjects<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
) -> jlong {
    env.with_env(|_| Ok::<_, BoundaryError>(bridge::live_objects()))
        .resolve::<ThrowJavaException>()
}

// SAFETY: signature contract (module docs); Java: `static native void panicForTest()`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_panicForTest<
    'l,
>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
) {
    env.with_env(|_| bridge::panic_for_test())
        .resolve::<ThrowJavaException>();
}

// SAFETY: signature contract (module docs); Java: `static native long nopRaw(long x)`.
// For measuring the bare JNI transition: it does not upgrade the environment,
// and its body (returning its argument) cannot panic.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_nopRaw<'l>(
    _env: EnvUnowned<'l>,
    _class: JClass<'l>,
    x: jlong,
) -> jlong {
    x
}

// SAFETY: signature contract (module docs); Java: `static native long nop(long x)`.
// For measuring the transition plus `with_env` and the error policy.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_cassandra_db_memtable_rust_NativeMemtable_nop<'l>(
    mut env: EnvUnowned<'l>,
    _class: JClass<'l>,
    x: jlong,
) -> jlong {
    env.with_env(|_| Ok::<_, BoundaryError>(x))
        .resolve::<ThrowJavaException>()
}
