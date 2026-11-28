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

# Owner time, sprint 2

Started 2026-11-28 at the owner's request, so that sprint 2's assessment counts the owner's time from a log instead of reconstructing it afterwards (sprint 1's figure was reconstructed episode by episode; `ASSESS.md` section 5.3).

## Protocol

- The owner says **"I'm leaving now"** and **"I'm back"**. The orchestrator replies with the time (Pacific, from the machine clock) and adds a line below.
- A **session** runs from "I'm back" to "I'm leaving now". Its length is the owner's time.
- Each session gets a kind:
  - **design:** discussion, questions, rulings;
  - **review:** reading and checking delivered work;
  - **tending:** a minute or two to say "go ahead" or answer a yes/no; counted, but shown apart, so it is not mistaken for real investment of time;
  - **housekeeping:** work outside the project (GitHub settings and the like); logged, not counted.
- When the owner forgets to say either phrase, the orchestrator estimates from message times, marks the line **estimated**, and asks the owner to confirm at the next return.
- Times are wall clock, Pacific; UTC in brackets for the first line of each day.

## Log

| Date (PT) | From | To | Minutes | Kind | What | Source |
|---|---|---|---|---|---|---|
| Thu 11-26 | 11:43 | 11:48 | 5 | housekeeping | GitHub graph, About box, rename | estimated from message times; not counted |
| Thu 11-26 | 21:05 | 21:15 | 10 | housekeeping | detaching the fork | estimated; not counted (the Montecristo questions at 21:20 belong to another project and are not booked here, owner 2026-11-28) |
| Fri 11-27 | 00:37 | 00:45 | 8 | review | graph check, status, approval of D7-verification, asked for the decisions | estimated; confirmed by the owner 2026-11-28 |
| Fri 11-27 | 20:00 | 21:52 | 112 | design | read the overview and decision #46; correlator; first-principles premises; driver; Rust and Lean; garbage debt; R-1, R-14, R-20; user stories; this protocol; repair; asked for the evaluation harness; Uncle Bob's metrics and the code-quality harness | start estimated (20:00, reading before the first message at 20:14), confirmed by the owner; end: owner left at 21:52 ("OK, I'll be back later") [05:52 UTC 11-28] |
| Sat 11-28 | 00:04 | 00:27 | 23 | design | token use; why the layer never acts (actuators off), degraded_peer blind at 3 nodes; Rust spike: memtable vs sstable reader, JNI boundary risk; approved the overnight memtable spike | logged on arrival and departure ("I'm off to bed now") [08:04-08:27 UTC 11-28] |

**Sprint 2 so far (counted):** about 2 h 23 min (8 min review, 135 min design).
