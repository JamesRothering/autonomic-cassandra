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

# Autonomic Cassandra

> **[ASSESS.md](ASSESS.md)**: project assessment through D6: code produced, team-effort equivalent, tokens and cost, and what was built.
>
> **Sprint 1 close-out (2026-11-23):** what was missed and why, in the [postmortem](docs/postmortems/2026-11-22-cache-and-memory-near-miss.md) and the [observability coverage audit](docs/audits/observability-coverage.md); the plan for increment 2 in [docs/NEXT-STEPS.md](docs/NEXT-STEPS.md); the rules every session now follows in [docs/process/orchestration-prompt.md](docs/process/orchestration-prompt.md).

Autonomic Cassandra re-envisions Cassandra from first principles as a self-healing, self-correcting, self-diagnosing database. If demand spikes, it adapts elastically. If a hot spot develops, it changes the schema on the fly to mitigate it while keeping clients that depend on the old schema satisfied, at least eventually. The goal is a database that operators can leave running with less hand-tuning, without giving up the safety and predictability that operators expect from Cassandra.

## Status

Pre-alpha. This is a research fork. Its base is Apache Cassandra trunk at commit `2249e37b51af9fd2a72865bbb56c1197e0d0d1ed` (dated 2026-11-12). Nothing here is supported for production use. All autonomic features are off by default.

## How it works

Each node runs a MAPE-K control loop over a shared Knowledge store:

- **Monitor**: sensors read existing core metrics and sampled partition statistics.
- **Analyze**: detectors turn raw signals into diagnoses, such as a hot partition or a compaction backlog.
- **Plan**: policies choose actions for each diagnosis, subject to configuration.
- **Execute**: actuators apply the chosen action, then verify its effect and roll it back if the effect is not as expected.
- **Knowledge**: the shared store holds metrics history, diagnoses, actions, and the audit trail.

Guardrails apply to every stage: dry-run mode (plan and log without acting), a kill switch that stops all autonomic actions, rate limits, and blast-radius limits. Every action is verified after it runs and rolled back if verification fails.

## Repository guide

- [docs/DECISIONS.md](docs/DECISIONS.md): architectural and design decisions log (ADRs).
- [docs/deliverables/README.md](docs/deliverables/README.md): the deliverable plan (D0 to D7) and their status.
- [docs/architecture/](docs/architecture/): architecture specifications (produced in D1).
- [docs/BUILD.md](docs/BUILD.md): how to build and test the fork.
- [docs/operations/autonomic.md](docs/operations/autonomic.md): operator guide for the autonomic layer.
- [README.asc](README.asc): upstream Apache Cassandra README.

## Git conventions

- `main` always builds.
- Each deliverable is developed on its own branch, `deliverable/Dn-<name>`, merged to `main`, and tagged `Dn`.
- The remote `upstream` points to `apache/cassandra`. Upstream changes are pulled in by deliberate rebase onto the pinned base commit.

## License

Apache License 2.0. See [LICENSE.txt](LICENSE.txt).

Autonomic Cassandra is not affiliated with or endorsed by the Apache Software Foundation. "Apache" and "Apache Cassandra" are trademarks of the Apache Software Foundation.
