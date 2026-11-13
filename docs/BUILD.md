<!--
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
#
-->

# Baseline build of Apache Cassandra trunk

This documents a baseline `ant jar` build of trunk (version reported as `7.0-SNAPSHOT`) on a small machine, plus single-test smoke runs. Measured on 2026-11-12.

## Prerequisites

- JDK 17 and Apache Ant 1.10.18, installed under `~/.local` (no sudo, no system packages).
- Source the toolchain before any `java` or `ant` command:

  ```bash
  source ~/.local/env-autonomic.sh
  ```

  This sets `JAVA_HOME=~/.local/opt/jdk-17`, `ANT_HOME=~/.local/opt/ant`, and prepends both `bin` directories to `PATH`.
- `build.xml` declares `java.supported=11,17,21` with default 11. This baseline uses 17.
- Network access to Maven Central and `services.gradle.org` is needed on the first build. The build downloads dependencies, Gradle 8.5 (for the Accord module), and related artifacts.

## Build

Command used (no flags, no `-s`, no `--clean`):

```bash
cd /home/james/autonomic-cassandra
source ~/.local/env-autonomic.sh
ant -f build.xml jar
```

The equivalent helper script is `.build/build-jars.sh` (which runs `ant jar` and accepts `-s/--summary` and `-c/--clean`). It was not used for the baseline so that the full log was captured directly.

Flags to skip docs, checkstyle or rat: none were needed for `jar`. `jar` does not depend on `check`, `rat-check`, or `gen-asciidoc`, so no skip flags were passed. The full `ant check` (rat, checkstyle, asciidoc) is a separate target and was not run.

Result (first build, cold `~/.m2` and no `build/` directory):

- `BUILD SUCCESSFUL`, exit 0
- Wall-clock time: **6 min 43 s** (403 s), including the first-time dependency and Gradle downloads.
- Compile warnings are only `[removal]` deprecation warnings (e.g. `ThreadGroup.setDaemon`, `AccessController`). They are not errors.

Subsequent builds without changes are much faster because `build/` and `~/.m2` are populated. A re-run was not measured.

## Running tests

Per `AGENTS.md`, `CONTRIBUTING.md` and `.build/README.md`, the repo wrapper for test runs is `.build/run-tests.sh` (`-a <type>`, `-t <class regexp>`, `-s` for a summary). The underlying Ant targets are documented in `build.xml`. Ant commands below use `-Dno-build-test=true`, which is what `run-tests.sh` sets so that already-built test classes are not rebuilt.

### Single unit test class

```bash
source ~/.local/env-autonomic.sh
cd /home/james/autonomic-cassandra
ant testsome -Dno-build-test=true -Dtest.name=org.apache.cassandra.utils.ByteBufferUtilTest
```

Equivalent wrapper (requires the jar to exist): `.build/run-tests.sh -s -a test -t ByteBufferUtilTest`.

### Single method

```bash
ant testsome -Dno-build-test=true \
  -Dtest.name=org.apache.cassandra.utils.ByteBufferUtilTest \
  -Dtest.methods=<methodName>[,<methodName>...]
```

`-t` in `run-tests.sh` matches class names only, so use `ant testsome` with `-Dtest.methods` for individual methods.

### In-JVM distributed (junit) test class

Not run here. Documented in `build.xml` (around line 2136) and in `.build/run-tests.sh -a jvm-dtest`:

```bash
ant test-jvm-dtest-some -Dtest.name=org.apache.cassandra.distributed.test.ResourceLeakTest
ant test-jvm-dtest-some -Dtest.name=org.apache.cassandra.distributed.test.ResourceLeakTest -Dtest.methods=looperTest
```

Wrapper form: `.build/run-tests.sh -a jvm-dtest -t BooleanTest`. Note that `run-tests.sh` for `jvm-dtest` may build the dtest jar and needs the main jar present. Do not run the full `jvm-dtest` suite (hours).

## Smoke test results

All run with `ant testsome -Dno-build-test=true -Dtest.name=<class>` after the build above.

| Test class | Result (verbatim JUnit summary) | Ant wall time |
|---|---|---|
| `org.apache.cassandra.utils.ByteBufferUtilTest` | `Tests run: 13, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.75 sec` | ~3 s (ant reports 2 s) |
| `org.apache.cassandra.utils.FBUtilitiesTest` | `Tests run: 22, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 5.452 sec` | ~9 s (ant reports 8 s) |
| `org.apache.cassandra.cql3.SimpleQueryTest` | `Tests run: 19, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 12.039 sec` | ~18 s (ant reports 17 s) |

All three: `BUILD SUCCESSFUL`, exit 0.

## License audit (rat) and new `docs/*.md` files

`rat-check` (in `.build/build-rat.xml`, used by `ant check`) has these relevant properties:

- Its file set is `build/.ratinclude`, generated from `git ls-tree -r HEAD --name-only`. **Only committed files are audited.** Untracked files, such as a new `docs/*.md` before it is committed, are not checked by `ant check` until they are in a commit. Once committed, they are checked.
- Excludes that match Markdown: `**/README*` (so `README.md` at any level is exempt), `src/java/**/*.md`, `doc/modules/**/*`, `.github/pull_request_template.md`, `CHANGES.txt`, `CASSANDRA-14092.txt`, `AGENTS.md`, `CLAUDE.md`, `SECURITY.md`, `NOTICE.txt`, `LICENSE.txt`, and `test/**/*.txt`.
- There is **no** exclude for a generic top-level `docs/*.md` or `docs/**/*.md`. A new `docs/*.md` file is therefore audited, and rat will flag it as an unapproved license if it lacks an ASF header.
- `README.md` at the repo root is exempt because of `**/README*`, so it would not be flagged even without a header. This is by name, so `README.asc` is also exempt.

Conclusion: a new `docs/*.md` file needs the ASF license header (as an HTML comment, as in this file) to pass `rat-check` once committed. A root `README.md` would pass without one, but it is good practice to add the header anyway.

## Resource notes

- Machine: 4 cores, ~5.8 GB RAM, no swap in use. Build and smoke runs completed without memory errors. No `ANT_OPTS` override was set, so Ant used its defaults. The `javac` and test JVM heap sizes come from `build.xml` (e.g. `maxmemory="8g"` in some javac/test targets, `-Xmx8G` in some launchers). This was not tuned, and no OOM was observed.
- Disk after the build (measured 2026-11-12):
  - `build/`: 264 MB
  - `~/.m2`: 143 MB
  - `/home` filesystem: 8.7 GB used of 19 GB, 8.5 GB free (51% used)
- The Gradle 8.5 download for the Accord module lands under `modules/accord` and its `.gradle` directories (excluded from rat).
- The first build spends most of its time downloading. After the build, the jar is at `build/apache-cassandra-7.0-SNAPSHOT.jar`.

## Warnings

- The first build contacts `services.gradle.org` (Gradle 8.5) and Maven Central. Offline builds will fail until the caches are warm.
- Test classes are built under `build/test/classes` (about 3,100 test source files compiled on the first run). Keep `build/` in place between runs, or the `testsome` targets will need `maybe-build-test`.
- `ant check` (rat, checkstyle, asciidoc) was intentionally not run for this baseline.
