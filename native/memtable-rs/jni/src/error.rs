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

//! Errors at the boundary and how they become Java exceptions.

use std::any::Any;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};

use jni::Env;
use jni::errors::ErrorPolicy;
use jni::jni_str;
use jni::strings::JNIString;
use memtable_core::{DecodeError, MemtableError};

#[derive(Debug)]
pub enum BoundaryError {
    /// A JNI call failed; usually a Java exception is already pending.
    Jni(jni::errors::Error),
    /// No live object has this handle.
    BadHandle(i64),
    /// An argument violates the documented contract.
    BadArgument(&'static str),
    Decode(DecodeError),
    Memtable(MemtableError),
}

impl fmt::Display for BoundaryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Jni(e) => write!(f, "JNI error: {e}"),
            Self::BadHandle(h) => write!(f, "no live native object has handle {h}"),
            Self::BadArgument(what) => write!(f, "bad argument: {what}"),
            Self::Decode(e) => write!(f, "malformed record: {e}"),
            Self::Memtable(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BoundaryError {}

impl From<jni::errors::Error> for BoundaryError {
    fn from(e: jni::errors::Error) -> Self {
        Self::Jni(e)
    }
}

impl From<DecodeError> for BoundaryError {
    fn from(e: DecodeError) -> Self {
        Self::Decode(e)
    }
}

impl From<MemtableError> for BoundaryError {
    fn from(e: MemtableError) -> Self {
        Self::Memtable(e)
    }
}

/// Maps errors to `IllegalArgumentException` (the caller sent bad input) or
/// `IllegalStateException` (the object cannot do this now), and panics to
/// `RuntimeException`. If a Java exception is already pending it is left to
/// propagate. The native method then returns the type's default (0).
pub struct ThrowJavaException;

fn throw(env: &mut Env<'_>, illegal_argument: bool, message: String) {
    if env.exception_check() {
        return;
    }
    let message = JNIString::from(message);
    // If throwing fails there is nothing better to do: the JVM is in trouble
    // (e.g. out of memory), and a pending error will surface anyway.
    let _ = if illegal_argument {
        env.throw_new(jni_str!("java/lang/IllegalArgumentException"), message)
    } else {
        env.throw_new(jni_str!("java/lang/IllegalStateException"), message)
    };
}

impl<T: Default> ErrorPolicy<T, BoundaryError> for ThrowJavaException {
    type Captures<'unowned_env_local: 'native_method, 'native_method> = ();

    fn on_error<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        (): &mut (),
        err: BoundaryError,
    ) -> jni::errors::Result<T> {
        let illegal_argument = matches!(
            err,
            BoundaryError::BadArgument(_) | BoundaryError::Decode(_)
        );
        throw(env, illegal_argument, err.to_string());
        Ok(T::default())
    }

    fn on_panic<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        (): &mut (),
        payload: Box<dyn Any + Send + 'static>,
    ) -> jni::errors::Result<T> {
        let message = payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_owned())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "non-string panic payload".to_owned());
        // A payload's own Drop may panic; contain that and leak the second
        // payload rather than unwind into the JVM (as jni's own policy does).
        if let Err(second) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            std::mem::forget(second);
        }
        if !env.exception_check() {
            let _ = env.throw_new(
                jni_str!("java/lang/RuntimeException"),
                JNIString::from(format!("Rust panic: {message}")),
            );
        }
        Ok(T::default())
    }
}
