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

# D6: Adaptive Schema

## Goal

Change the physical layout of a table in response to a hot spot while clients that use the old logical schema continue to get the same results.

## Scope

- **T6.1** Logical-to-physical layout registry, propagated via cluster metadata.
- **T6.2** Coordinator query rewrite: fan-out reads with merge; routed writes and deletes.
- **T6.3** Migration engine: dual-write, backfill, cutover, and rollback.
- **T6.4** Compatibility tests: old-schema queries return identical results before, during, and after migration.

## Acceptance criteria

- [ ] A layout change is recorded in cluster metadata and seen by every node in a three-node in-JVM test.
- [ ] Queries written against the logical schema return identical results before, during, and after a migration, in the compatibility test suite.
- [ ] A migration that fails during backfill is rolled back to the old layout, and no acknowledged write is lost.

Refined in D1. The authoritative task list, with files, dependencies, acceptance criteria and model tier per task, is section 9 of [adaptive-schema.md](../architecture/adaptive-schema.md) (T6.1 to T6.16; T6.17, reverse migration, is the first follow-up after D6 per ADR-025). D6 depends on T2.8 (cluster ledger) specifically. The scope above is the original outline.

Operator guide for partition split (T6.16): [operations/adaptive-schema.md](../operations/adaptive-schema.md).

## Status

Not started.
