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

# Throws random and corrupted input at every native method (NativeMemtableFuzzTest) under -Xcheck:jni, which makes
# the JVM report misuse of the JNI API (e.g. a JNI call with an exception pending) as a warning or fatal error.
set -euo pipefail
cd "$(dirname "$0")/../../.."
cp="build/apache-cassandra-7.0-SNAPSHOT.jar:build/test/classes:test/conf:build/lib/jars/*:build/test/lib/jars/*"
"${JAVA_HOME:-$HOME/.local/opt/jdk-17}/bin/java" -Xcheck:jni -Drust_memtable.fuzz_iterations="${FUZZ_ITERATIONS:-1000000}" \
  -Dcassandra.rust_memtable.library="${RUST_LIB:-$PWD/native/memtable-rs/target/release/libmemtable_jni.so}" \
  -cp "$cp" org.junit.runner.JUnitCore org.apache.cassandra.db.memtable.rust.NativeMemtableFuzzTest
