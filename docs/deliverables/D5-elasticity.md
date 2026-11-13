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

# D5: Elasticity

## Goal

Adapt to load spikes within a node first, then provide a provider interface for adding or removing nodes.

## Scope

- **T5.1** In-node elasticity: thread-pool resizing; adaptive throttling and backpressure.
- **T5.2** Scale-out provider SPI, with a simulated provider and a Kubernetes stub.

## Acceptance criteria

- [ ] Thread-pool resizing stays within configured minimum and maximum sizes under a load spike in a single-node test.
- [ ] Adaptive throttling reduces the rate of accepted requests when latency rises above the SLO and restores it when latency recovers.
- [ ] The simulated provider can add and remove a node in a three-node in-JVM test, and the Kubernetes stub compiles and is covered by a unit test that checks its interface.

Scope and acceptance criteria will be refined after D1.

## Status

Not started.
