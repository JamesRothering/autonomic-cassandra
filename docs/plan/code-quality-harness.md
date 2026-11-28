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

# The sieve: a code-quality harness for agent-written code (story 0b)

- **Status:** proposed, 2026-11-28, for the owner to rule on the thresholds and on one new test-only dependency.
- **Owner's request (2026-11-28):** apply Robert C. Martin's ("Uncle Bob") recommendations for metrics on AI-generated code, such as cyclomatic complexity, in a harness that measures and detects problems, requires de-duplication of redundant code, and reduces the AI bloat factor.

## What Uncle Bob recommends

All from secondary sources (blog posts and write-ups of his posts and podcast appearances); his own posts were not read, so the numbers are **unverified**:

- **Do not review agent code line by line; constrain it.** The constraints reported: a cyclomatic complexity ceiling, module size limits, dependency structure analysis, mutation testing and test coverage ([StartupFortune, 2026-07-25](https://startupfortune.com/uncle-bob-martin-says-he-no-longer-reads-ai-generated-code-and-the-developer-world-is-split/)). Accounts differ on whether he reads none of it or reviewed every line ([Medium](https://medium.com/@ivan-stepantsov/uncle-bob-stopped-reading-his-agents-code-8c8544b16b39)).
- **CRAP as the main gate.** CRAP combines a method's complexity with how little of it is tested: `CRAP = complexity² × (1 − coverage)³ + complexity`. A complex method needs thorough tests to pass; a simple one passes untested. Reported thresholds: 4 for humans, 6 for agents, and later "12 was probably okay" ([Sprint and Drift](https://sprintanddrift.substack.com/p/saying-know-to-code), [Howardism](https://www.howardism.dev/articles/reviving-impractical-quality-tools)).
- **Cyclomatic complexity ceiling of 6** for agent code, reported by a Rails team that adopted his approach ([Codeminer42](https://blog.codeminer42.com/stop-reading-ai-code-start-measuring-it-a-rails-playbook/)).
- **Mutation testing,** abandoned around 2000 because a human paid for every run, revived because agents now pay: run the mutation tests, add tests that kill the surviving mutants, repeat ([Howardism](https://www.howardism.dev/articles/reviving-impractical-quality-tools)).
- **Separate agent stages:** specification, development, cleanup, architecture, hardening, mutation testing, each in a deterministic container ([Sprint and Drift](https://sprintanddrift.substack.com/p/saying-know-to-code)).
- **Dependency structure:** his package principles from *Clean Architecture* (from knowledge): no cycles between packages (acyclic dependencies), depend in the direction of stability, stable packages abstract; measured by afferent and efferent coupling, instability `I = Ce / (Ca + Ce)`, abstractness `A`, and distance from the main sequence `D = |A + I − 1|`.
- **The bloat evidence:** one comparison of 642 AI solutions with 107 expert-written ones reports 34% higher cyclomatic complexity and 2.1 times more duplication in the AI code ([Codeminer42](https://blog.codeminer42.com/stop-reading-ai-code-start-measuring-it-a-rails-playbook/); the study itself not read).

## What we measure

| # | Metric | Java tool | Available | Proposed gate for new or changed code |
|---|---|---|---|---|
| 1 | Cyclomatic complexity per method | JaCoCo report (`COMPLEXITY` counter per method); checkstyle `CyclomaticComplexity` | yes: JaCoCo 0.8.8 (`build.xml:157`, target `codecoverage` at `:760`), checkstyle 10.26.1 | ≤ 6 (Bob) |
| 2 | CRAP per method | computed from the JaCoCo XML (complexity and line coverage per method) | yes | ≤ 6 (Bob for agents); owner to choose 6 or 12 |
| 3 | Size: method lines, class lines, parameters, nesting depth | checkstyle `MethodLength`, `FileLength`, `ParameterNumber`, `NestedIfDepth` | yes | method ≤ 40 lines, class ≤ 600, parameters ≤ 5, nesting ≤ 3 (proposed) |
| 4 | Duplication | a token-based detector in the harness (normalised Java tokens, windows of at least 60 tokens) | to write (no new dependency); PMD's CPD would need a new dependency | no new duplicate block; duplication % reported |
| 5 | Dependency structure | `jdeps` (in the JDK) on the compiled classes, package graph | yes | no package cycles; `D` reported per package; layering rules (for example, `plan` may not depend on `execute` internals) |
| 6 | Coverage (line and branch) | JaCoCo | yes | ≥ 80% line, ≥ 70% branch (proposed) |
| 7 | Mutation score | PIT (pitest) | **no**: a new test-only dependency, owner to approve | ≥ 70% killed (proposed); every surviving mutant gets a test or a reason |
| 8 | Bloat indicators | the harness | to write | reported, see below |

**Bloat indicators** (signals for the cleanup stage, not hard gates): unreferenced methods and classes (dead code); comment-to-code ratio far above the module's median (agents over-comment); test lines per production line against the mutation score (tests that kill no mutants are bloat); repeated defensive checks of the same condition along one call path; code rewritten within a few days of being written (git churn, a sign of work that was thrown away). For Rust, later: `clone()` calls and `Arc<Mutex<…>>` per thousand lines, `unwrap()` outside tests, Clippy's `cognitive_complexity` and `too_many_lines`, `cargo-mutants`, `cargo-llvm-cov`.

## How it acts

1. **Baseline, then ratchet.** The first run records today's values for sprint 1's code. From then on new and changed code must meet the gates; existing violations are **quality debt**, listed with their size and paid down, never allowed to grow (the same idea as garbage debt, PRE-19).
2. **A gate on every task.** Workers run the sieve before they report; a failing gate is reported, not worked around (the standing prompt's rule on flaky tests applies).
3. **A cleanup stage by an agent that did not write the code,** as Bob separates stages: it takes the sieve's report (duplicates, CRAP offenders, dead code, bloat signals), de-duplicates, simplifies, deletes dead code, and must leave behaviour unchanged: all tests pass and the mutation score does not fall.
4. **Reported in each gate's review** next to the failure-mode coverage.

## Order

1. Baseline of `C/autonomic/` and its tests: complexity, CRAP, size, duplication, dependency structure, coverage. Runs after the gauntlet's matrix, so the two do not compete for the 4 cores.
2. Owner rulings: thresholds; PIT.
3. Mutation baseline (if PIT is approved).
4. The first cleanup pass on the worst offenders, as a measured experiment: lines removed, duplication and CRAP before and after, tests and mutation score unchanged.

## Rulings asked of the owner

- **Q-1:** CRAP gate 6 (Bob's agent threshold) or 12 (his later view)?
- **Q-2:** add PIT as a test-only dependency (not shipped)? AGENTS.md asks for community approval of dependencies upstream; in this fork the owner decides.
- **Q-3:** size and coverage gates as proposed above?
