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

# Hook budget check (T2.10). Lists the files that differ from the pinned upstream commit, minus
# the files that are ours by definition, and compares the result with the hook table in
# docs/architecture/overview.md section 4.1. Exits 1 and prints the difference on mismatch.
#
# Usage: tools/autonomic/check-hook-budget.sh [base-commit]   (or set BASE_COMMIT)

set -euo pipefail

# Pinned upstream trunk commit (ADR-001).
PINNED_COMMIT=2249e37b51af9fd2a72865bbb56c1197e0d0d1ed
BASE="${1:-${BASE_COMMIT:-$PINNED_COMMIT}}"

# ---------------------------------------------------------------------------------------------
# Expected upstream files modified by hooks. Keep in sync with overview.md section 4.1.
# D6 adds hooks H16 to H21 (FC7); H21 (AlterSchema.java) and StorageProxy.java (H16, H19, H20 of T6.3; H17 of T6.4; H18 follows
# with T6.5) are here.
# ---------------------------------------------------------------------------------------------
EXPECTED_FILES="
conf/cassandra.yaml
src/java/org/apache/cassandra/config/Config.java
src/java/org/apache/cassandra/config/DatabaseDescriptor.java
src/java/org/apache/cassandra/db/virtual/SystemViewsKeyspace.java
src/java/org/apache/cassandra/metrics/CassandraMetricsRegistry.java
src/java/org/apache/cassandra/schema/Schema.java
src/java/org/apache/cassandra/schema/SchemaConstants.java
src/java/org/apache/cassandra/service/CassandraDaemon.java
src/java/org/apache/cassandra/service/StorageProxy.java
src/java/org/apache/cassandra/service/StorageService.java
src/java/org/apache/cassandra/tcm/transformations/AlterSchema.java
src/java/org/apache/cassandra/tcm/transformations/CustomTransformation.java
src/java/org/apache/cassandra/tools/NodeProbe.java
src/java/org/apache/cassandra/tools/nodetool/NodetoolCommand.java
test/distributed/org/apache/cassandra/distributed/impl/Instance.java
test/distributed/org/apache/cassandra/distributed/mock/nodetool/InternalNodeProbe.java
test/resources/nodetool/help/nodetool
test/unit/org/apache/cassandra/config/DatabaseDescriptorRefTest.java
"

# Paths that are ours by definition and excluded from the comparison (extended regexes on the path).
EXCLUDE_PATTERNS=(
    '^src/java/org/apache/cassandra/autonomic/'
    '^src/java/org/apache/cassandra/tools/nodetool/Autonomic\.java$'
    '^test/unit/org/apache/cassandra/autonomic/'
    '^test/distributed/org/apache/cassandra/distributed/test/autonomic/'
    '^test/unit/org/apache/cassandra/tools/nodetool/Autonomic[A-Za-z]*Test\.java$'
    '^test/resources/nodetool/help/autonomic'   # generated fixtures for our command (H15)
    '^docs/'
    '^README\.md$'
    '^tools/autonomic/'
)

cd "$(git rev-parse --show-toplevel)"
git cat-file -e "${BASE}^{commit}" 2>/dev/null || { echo "Base commit $BASE not found" >&2; exit 2; }

# Committed and uncommitted changes to tracked files, relative to the base.
changed=$(git diff --name-only "$BASE" | sort -u)
for p in "${EXCLUDE_PATTERNS[@]}"; do
    changed=$(grep -Ev -- "$p" <<<"$changed" || true)
done

expected=$(grep -v '^$' <<<"$EXPECTED_FILES" | sort -u)

unexpected=$(comm -23 <(grep -v '^$' <<<"$changed") <(echo "$expected"))
missing=$(comm -13 <(grep -v '^$' <<<"$changed") <(echo "$expected"))

echo "Base commit: $BASE"
echo "Changed files outside our paths:"
sed 's/^/  /' <<<"$changed"

if [[ -z "$unexpected" && -z "$missing" ]]; then
    echo "OK: $(grep -c . <<<"$expected") files, matches overview.md section 4.1"
    exit 0
fi

echo "MISMATCH with overview.md section 4.1"
[[ -n "$unexpected" ]] && { echo "Changed but not in the hook table:"; sed 's/^/  + /' <<<"$unexpected"; }
[[ -n "$missing" ]] && { echo "In the hook table but not changed:"; sed 's/^/  - /' <<<"$missing"; }
exit 1
