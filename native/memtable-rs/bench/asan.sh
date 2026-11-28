#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# AddressSanitizer run of the JNI layer inside a real JVM: builds the library with -Zsanitizer=address (nightly,
# std rebuilt with the sanitizer), preloads GCC's ASan runtime into the JVM (a cdylib does not carry the runtime),
# then runs NativeMemtableFuzzTest directly and RustMemtableDifferentialTest through ant.
# JVM-friendly options: the JVM uses SIGSEGV internally (handle_segv=0), and leak checking is meaningless in a JVM.
set -euo pipefail
cd "$(dirname "$0")/.."
ASAN_RT=${ASAN_RT:-/usr/lib/x86_64-linux-gnu/libasan.so.8}
RUSTFLAGS="-Zsanitizer=address" cargo +nightly build -Zbuild-std --target x86_64-unknown-linux-gnu -p memtable-jni --target-dir target/asan
lib=$PWD/target/asan/x86_64-unknown-linux-gnu/debug/libmemtable_jni.so
cd ../..
export LD_PRELOAD=$ASAN_RT
export ASAN_OPTIONS=detect_leaks=0:handle_segv=0:allow_user_segv_handler=1:detect_stack_use_after_return=0:abort_on_error=1
cp="build/apache-cassandra-7.0-SNAPSHOT.jar:build/test/classes:test/conf:build/lib/jars/*:build/test/lib/jars/*"
"${JAVA_HOME:-$HOME/.local/opt/jdk-17}/bin/java" -Xcheck:jni -Drust_memtable.fuzz_iterations="${FUZZ_ITERATIONS:-200000}" \
  -Dcassandra.rust_memtable.library="$lib" -cp "$cp" org.junit.runner.JUnitCore org.apache.cassandra.db.memtable.rust.NativeMemtableFuzzTest
ANT_ARGS=-Dno-build-accord=true ant testsome -Dtest.name=org.apache.cassandra.db.memtable.rust.RustMemtableDifferentialTest \
  "-Dtest.jvm.args=-Dcassandra.rust_memtable.library=$lib"
