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

# Deliverables

Work is organized into deliverables D0 to D7. Each deliverable is developed on a branch `deliverable/Dn-<name>`, merged to `main`, and tagged `Dn`. See [ADR-002](../DECISIONS.md) for the rationale.

| ID | Name | Status | Depends on | Doc link |
|----|------|--------|------------|----------|
| D0 | Foundation | Done | none | [D0-foundation.md](D0-foundation.md) |
| D1 | Architecture | Done | D0 | [D1-architecture.md](D1-architecture.md) |
| D2 | Autonomic framework | In progress | D1 | [D2-framework.md](D2-framework.md) |
| D3 | Self-diagnosis | Not started | D2 | [D3-self-diagnosis.md](D3-self-diagnosis.md) |
| D4 | Self-healing | Not started | D3 | [D4-self-healing.md](D4-self-healing.md) |
| D5 | Elasticity | Not started | D4 | [D5-elasticity.md](D5-elasticity.md) |
| D6 | Adaptive schema | Not started | D2 (T2.8) | [D6-adaptive-schema.md](D6-adaptive-schema.md) |
| D7 | Verification | Not started | D4, D5, D6 | [D7-verification.md](D7-verification.md) |
