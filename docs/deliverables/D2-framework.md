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

# D2: Autonomic Framework

## Goal

Provide the MAPE-K framework skeleton, the knowledge store, the guardrails, and the operator interface, with every autonomic feature off by default.

## Scope

- **T2.1** Core interfaces (Sensor, Detector, Policy, Actuator), a `cassandra.yaml` configuration section (off by default), and daemon lifecycle wiring.
- **T2.2** Knowledge store: a `system_autonomic` keyspace and `system_views.autonomic_*` virtual tables.
- **T2.3** Sensors over existing metrics and top-partitions sampling.
- **T2.4** Guardrails: dry-run, kill switch, rate and blast-radius limits, exposed over JMX.
- **T2.5** `nodetool autonomic status|pause|resume|history`.

## Acceptance criteria

- [ ] With the default configuration, a node starts with no autonomic component active, and no new tables or threads are created beyond the knowledge store schema.
- [ ] The kill switch stops all actuators within one control-loop interval, verified by a unit test.
- [ ] `nodetool autonomic status`, `pause`, `resume`, and `history` work against a single-node in-JVM test.

Refined in D1. The authoritative task list, with files, dependencies, acceptance criteria and model tier per task, is section 13 of [overview.md](../architecture/overview.md) (T2.1 to T2.10), as amended by FC1 to FC10 in section 9.1 of [adaptive-schema.md](../architecture/adaptive-schema.md). The scope above is the original outline.

## Status

Not started.
