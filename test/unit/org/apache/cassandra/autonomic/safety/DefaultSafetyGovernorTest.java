/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.autonomic.safety;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionOutcome;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.FakeKnobActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** One test per check of overview 7.1, in order, the order between them, and the state that feeds them. */
public class DefaultSafetyGovernorTest
{
    private static final VerifySpec SPEC = new VerifySpec(SignalId.of("goal"), Subject.node(), true, 0.1, List.of(), 1000, 1000);
    private static final long MINUTE = TimeUnit.MINUTES.toMillis(1);
    private static final long HOUR = TimeUnit.HOURS.toMillis(1);

    private final AtomicLong now = new AtomicLong(10 * 24 * HOUR);
    private final AutonomicConfig config = new AutonomicConfig();
    private final FakeKnobActuator actuator = new FakeKnobActuator("knob");
    private final FakeKnobActuator clusterActuator = new FakeKnobActuator("cluster_knob");
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(10, 10), now::get);
    private final Map<String, String> durable = new LinkedHashMap<>();
    private final List<String> quarantined = new ArrayList<>();
    private int breakerTrips;
    private long ids;

    // environment switches
    private String gate;
    private boolean actuatorEnabled = true;
    private boolean nodeReady = true;
    private boolean storeAvailable = true;
    private boolean clusterEnabled = true;
    private String clusterNodeState;
    private String clusterBusy;

    private DefaultSafetyGovernor governor;

    @Before
    public void setup()
    {
        config.dry_run = false;
        // limits that would interfere are out of the way unless a test sets them
        config.safety.max_actions_per_hour = 100;
        config.safety.max_actions_per_actuator_per_hour = 100;
        config.safety.max_cluster_actions_per_hour = 100;
        config.safety.breaker_rollbacks = 100;
        clusterActuator.scope = ActionScope.CLUSTER;
        governor = newGovernor();
    }

    private DefaultSafetyGovernor newGovernor()
    {
        DefaultSafetyGovernor.Environment env = new DefaultSafetyGovernor.Environment()
        {
            public AutonomicConfig config() { return config; }
            public long nowMillis() { return now.get(); }
            public String layerGate() { return gate; }
            public Actuator actuator(String name) { return name.equals("knob") ? actuator : name.equals("cluster_knob") ? clusterActuator : null; }
            public boolean actuatorEnabled(String name) { return actuatorEnabled; }
            public boolean nodeReady() { return nodeReady; }
            public boolean storeAvailable() { return storeAvailable; }

            public ClusterGate clusterGate()
            {
                return new ClusterGate()
                {
                    public boolean enabled() { return clusterEnabled; }
                    public java.util.Optional<String> nodeState(Action a) { return java.util.Optional.ofNullable(clusterNodeState); }
                    public java.util.Optional<String> concurrency(Action a) { return java.util.Optional.ofNullable(clusterBusy); }
                };
            }
        };
        DefaultSafetyGovernor.Journal journal = new DefaultSafetyGovernor.Journal()
        {
            public void putState(String key, String value) { durable.put(key, value); }
            public void deleteState(String key) { durable.remove(key); }
            public void quarantined(String policy, long untilMillis) { quarantined.add(policy); }
            public void breakerTripped(int rollbacks, int window) { breakerTrips++; }
        };
        return new DefaultSafetyGovernor(env, journal);
    }

    private UUID id()
    {
        return TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + ids++, 7L);
    }

    private Action action(String table)
    {
        return action(table, null);
    }

    private Action action(String table, String knob)
    {
        return new Action(id(), "knob", ActionScope.NODE, Subject.table("ks", table), knob == null ? Map.of() : Map.of("knob", knob), SPEC);
    }

    private Action clusterAction(String table)
    {
        return new Action(id(), "cluster_knob", ActionScope.CLUSTER, Subject.table("ks", table), Map.of(), SPEC);
    }

    private ActionRecord proposal(Action a, String policy)
    {
        return new ActionRecord(a, ActionState.PROPOSED, now.get(), null, policy, false, null, Map.of(), Map.of(), Map.of(), null, now.get());
    }

    private Decision evaluate(Action a)
    {
        return governor.evaluate(proposal(a, "policy"), view);
    }

    private void outcome(Action a, ActionState state, String reason)
    {
        governor.recordOutcome(a, new ActionOutcome(state, null, reason, Map.of(), Map.of(), now.get(), now.get()));
    }

    /** Approves and completes an action, so that it leaves no reservation. */
    private void run(Action a, String policy, ActionState state, String reason)
    {
        assertEquals(Decision.approve(), governor.evaluate(proposal(a, policy), view));
        outcome(a, state, reason);
    }

    private void assertRejected(String reason, Decision d)
    {
        assertEquals(Decision.Type.REJECT, d.type);
        assertEquals(reason, d.reason);
    }

    @Test
    public void approvesWhenEveryCheckPasses()
    {
        assertEquals(Decision.approve(), evaluate(action("t")));
        assertEquals(1, governor.reservations());
        // the two-argument form of overview 3.4
        assertRejected(Reasons.CONFLICT, governor.evaluate(action("t"), view));
    }

    @Test
    public void check01_killedPausedNotArmed()
    {
        for (String g : new String[]{ Reasons.KILLED, Reasons.PAUSED, Reasons.NOT_ARMED })
        {
            gate = g;
            assertRejected(g, evaluate(action("t")));
        }
        assertEquals(0, governor.reservations());
    }

    @Test
    public void check02_dryRun()
    {
        config.dry_run = true;
        assertRejected(Reasons.DRY_RUN, evaluate(action("t")));
        // dry-run is the default
        assertTrue(new AutonomicConfig().dry_run);
    }

    @Test
    public void check03_actuatorDisabled()
    {
        actuatorEnabled = false;
        assertRejected(Reasons.ACTUATOR_DISABLED, evaluate(action("t")));
        actuatorEnabled = true;
        Action unknown = new Action(id(), "nope", ActionScope.NODE, Subject.node(), Map.of(), SPEC);
        assertRejected(Reasons.ACTUATOR_DISABLED, evaluate(unknown));
    }

    @Test
    public void check03_clusterGate()
    {
        config.safety.cluster_approval = AutonomicConfig.SafetyConfig.APPROVAL_AUTO;
        clusterEnabled = false;
        assertRejected(Reasons.CLUSTER_DISABLED, evaluate(clusterAction("t")));
        // node-scoped actions do not need it
        assertEquals(Decision.approve(), evaluate(action("t")));
        // the default gate, until the ledger exists, is closed
        assertTrue(!ClusterGate.CLOSED.enabled());
    }

    @Test
    public void check04_validation()
    {
        actuator.invalid = "out of range";
        Decision d = evaluate(action("t"));
        assertRejected(Reasons.VALIDATION, d);
        assertEquals("out of range", d.detail);
        actuator.invalid = null;

        // no verify spec, an id that is not a timeuuid, a scope the actuator does not have
        assertRejected(Reasons.VALIDATION, evaluate(new Action(id(), "knob", ActionScope.NODE, Subject.node(), Map.of(), null)));
        assertRejected(Reasons.VALIDATION, evaluate(new Action(UUID.randomUUID(), "knob", ActionScope.NODE, Subject.node(), Map.of(), SPEC)));
        actuator.scope = ActionScope.CLUSTER;
        assertRejected(Reasons.VALIDATION, evaluate(action("t")));
    }

    @Test
    public void check04_irreversibleNeedsCriticalUnlessApprovalIsManual()
    {
        actuator.reversible = false;
        UUID warn = UUID.randomUUID();
        UUID critical = UUID.randomUUID();
        view.setActiveDiagnoses(List.of(new Diagnosis(warn, "x", Subject.node(), Severity.WARN, 1, 0, 0, "", Map.of(), null),
                                        new Diagnosis(critical, "y", Subject.node(), Severity.CRITICAL, 1, 0, 0, "", Map.of(), null)));
        Action a = action("t");
        ActionRecord fromWarn = new ActionRecord(a, ActionState.PROPOSED, now.get(), warn, "policy", false, null, Map.of(), Map.of(), Map.of(), null, now.get());
        ActionRecord fromCritical = new ActionRecord(a, ActionState.PROPOSED, now.get(), critical, "policy", false, null, Map.of(), Map.of(), Map.of(), null, now.get());
        assertRejected(Reasons.VALIDATION, governor.evaluate(fromWarn, view));
        assertRejected(Reasons.VALIDATION, evaluate(a));

        // FC4: waived when an operator approves each action
        config.safety.node_approval = AutonomicConfig.SafetyConfig.APPROVAL_MANUAL;
        assertEquals(Decision.Type.HOLD_FOR_APPROVAL, governor.evaluate(fromWarn, view).type);
        outcome(a, ActionState.REJECTED, null);
        config.safety.node_approval = AutonomicConfig.SafetyConfig.APPROVAL_AUTO;

        assertEquals(Decision.approve(), governor.evaluate(fromCritical, view));
        // rule 10: one per target per day
        outcome(a, ActionState.COMMITTED, null);
        now.addAndGet(23 * HOUR);
        ActionRecord again = new ActionRecord(action("t"), ActionState.PROPOSED, now.get(), critical, "policy", false, null, Map.of(), Map.of(), Map.of(), null, now.get());
        assertRejected(Reasons.COOLDOWN, governor.evaluate(again, view));
    }

    @Test
    public void check05_nodeState()
    {
        nodeReady = false;
        assertRejected(Reasons.NODE_STATE, evaluate(action("t")));
        nodeReady = true;
        // FC3: the lane's own conditions, from the cluster gate
        config.safety.cluster_approval = AutonomicConfig.SafetyConfig.APPROVAL_AUTO;
        clusterNodeState = "sequence in progress";
        Decision d = evaluate(clusterAction("t"));
        assertRejected(Reasons.NODE_STATE, d);
        assertEquals("sequence in progress", d.detail);
    }

    @Test
    public void check06_storeUnavailable()
    {
        storeAvailable = false;
        assertRejected(Reasons.STORE_UNAVAILABLE, evaluate(action("t")));
    }

    @Test
    public void check07_quarantineAfterRepeatedRollbacksOfOnePolicy()
    {
        config.safety.max_actions_per_hour = 100;
        config.safety.max_actions_per_actuator_per_hour = 100;
        config.safety.breaker_rollbacks = 100;
        // default: three rollbacks within an hour
        run(action("a"), "bad", ActionState.ROLLED_BACK, "NEUTRAL");
        now.addAndGet(10 * MINUTE);
        run(action("b"), "bad", ActionState.ROLLED_BACK, "REGRESSED");
        assertTrue(quarantined.isEmpty());
        now.addAndGet(10 * MINUTE);
        run(action("c"), "bad", ActionState.FAILED, Reasons.ROLLBACK_FAILED);
        assertEquals(List.of("bad"), quarantined);
        assertTrue(durable.containsKey("quarantine/bad"));

        assertRejected(Reasons.QUARANTINED, governor.evaluate(proposal(action("d"), "bad"), view));
        // other policies are unaffected
        Action other = action("d");
        assertEquals(Decision.approve(), governor.evaluate(proposal(other, "good"), view));
        outcome(other, ActionState.REJECTED, null);

        // 6h
        now.addAndGet(6 * HOUR - 1);
        assertRejected(Reasons.QUARANTINED, governor.evaluate(proposal(action("e"), "bad"), view));
        now.addAndGet(1);
        assertEquals(Decision.approve(), governor.evaluate(proposal(action("e"), "bad"), view));
    }

    @Test
    public void check07_rollbacksOutsideTheWindowOrCausedByTheOperatorDoNotQuarantine()
    {
        config.safety.max_actions_per_hour = 100;
        config.safety.max_actions_per_actuator_per_hour = 100;
        config.safety.breaker_rollbacks = 100;
        run(action("a"), "p", ActionState.ROLLED_BACK, "NEUTRAL");
        now.addAndGet(31 * MINUTE);
        run(action("b"), "p", ActionState.ROLLED_BACK, "NEUTRAL");
        now.addAndGet(31 * MINUTE);
        run(action("c"), "p", ActionState.ROLLED_BACK, "NEUTRAL");
        run(action("d"), "p", ActionState.ROLLED_BACK, Reasons.KILL);
        run(action("e"), "p", ActionState.ROLLED_BACK, Reasons.SHUTDOWN);
        assertTrue(quarantined.isEmpty());
    }

    /** T6.9 review (T6.13 finding): continuing committed work is not new work: no cooldown, no rule 10. */
    @Test
    public void check08_aContinuationOfCommittedWorkIsExemptFromTheCooldownAndRule10()
    {
        actuator.reversible = false;
        UUID critical = UUID.randomUUID();
        view.setActiveDiagnoses(List.of(new Diagnosis(critical, "y", Subject.node(), Severity.CRITICAL, 1, 0, 0, "", Map.of(), null)));
        Action a = action("t");
        assertEquals(Decision.approve(), governor.evaluate(new ActionRecord(a, ActionState.PROPOSED, now.get(), critical, "policy", false, null, Map.of(),
                                                                            Map.of(), Map.of(), null, now.get()), view));
        // the stopped split: COMMITTED (cleanup pending) puts the irreversible cooldown of a day on the target
        outcome(a, ActionState.COMMITTED, Reasons.COMMITTED_CLEANUP_PENDING);
        now.addAndGet(HOUR);
        assertRejected(Reasons.COOLDOWN, governor.evaluate(new ActionRecord(action("t"), ActionState.PROPOSED, now.get(), critical, "policy", false, null,
                                                                            Map.of(), Map.of(), Map.of(), null, now.get()), view));
        // its continuation (a resume past the commit point) is exempt, without a CRITICAL diagnosis too
        actuator.continuation = true;
        Action resume = action("t");
        assertEquals(Decision.approve(), evaluate(resume));
        outcome(resume, ActionState.COMMITTED, null);
        // a new action on the target is still cooling down
        actuator.continuation = false;
        assertRejected(Reasons.COOLDOWN, governor.evaluate(new ActionRecord(action("t"), ActionState.PROPOSED, now.get(), critical, "policy", false, null,
                                                                            Map.of(), Map.of(), Map.of(), null, now.get()), view));
    }

    @Test
    public void check08_cooldownAfterCommitAndAfterRollback()
    {
        Action a = action("t");
        run(a, "p", ActionState.COMMITTED, null);
        assertRejected(Reasons.COOLDOWN, evaluate(action("t")));
        // keyed by dedupe key
        Action other = action("other");
        assertEquals(Decision.approve(), evaluate(other));
        outcome(other, ActionState.REJECTED, null);
        now.addAndGet(10 * MINUTE - 1);
        assertRejected(Reasons.COOLDOWN, evaluate(action("t")));
        now.addAndGet(1);

        run(action("t"), "p", ActionState.ROLLED_BACK, "NEUTRAL");
        now.addAndGet(30 * MINUTE - 1);
        assertRejected(Reasons.COOLDOWN, evaluate(action("t")));
        now.addAndGet(1);
        assertEquals(Decision.approve(), evaluate(action("t")));
    }

    @Test
    public void check08_cooldownDoublesPerConsecutiveRollbackUpToTheMaximum()
    {
        config.safety.max_actions_per_hour = 100;
        config.safety.max_actions_per_actuator_per_hour = 100;
        config.safety.breaker_rollbacks = 100;
        config.safety.quarantine_after_rollbacks = 100;
        long[] expectedMinutes = { 30, 60, 120, 240, 360, 360 };
        for (long minutes : expectedMinutes)
        {
            run(action("t"), "p", ActionState.ROLLED_BACK, "NEUTRAL");
            now.addAndGet(minutes * MINUTE - 1);
            assertRejected(Reasons.COOLDOWN, evaluate(action("t")));
            now.addAndGet(1);
        }
        // a commit ends the streak
        run(action("t"), "p", ActionState.COMMITTED, null);
        now.addAndGet(10 * MINUTE);
        run(action("t"), "p", ActionState.ROLLED_BACK, "NEUTRAL");
        now.addAndGet(30 * MINUTE);
        assertEquals(Decision.approve(), evaluate(action("t")));
    }

    @Test
    public void check08_killAndSupersededCoolDownWithoutCountingAsRollback()
    {
        run(action("t"), "p", ActionState.ROLLED_BACK, Reasons.KILL);
        now.addAndGet(30 * MINUTE);
        run(action("t"), "p", ActionState.SUPERSEDED, Reasons.OVERRIDDEN);
        now.addAndGet(30 * MINUTE - 1);
        assertRejected(Reasons.COOLDOWN, evaluate(action("t")));
        now.addAndGet(1);
        // the first real rollback still gets the base cooldown
        run(action("t"), "p", ActionState.ROLLED_BACK, "NEUTRAL");
        now.addAndGet(30 * MINUTE);
        assertEquals(Decision.approve(), evaluate(action("t")));
        assertEquals(0, breakerTrips);
    }

    @Test
    public void check09_conflictOnDedupeKeyOrKnob()
    {
        config.safety.node_approval = AutonomicConfig.SafetyConfig.APPROVAL_MANUAL;
        Action held = action("t", "x");
        assertEquals(Decision.Type.HOLD_FOR_APPROVAL, evaluate(held).type);
        // same dedupe key
        assertRejected(Reasons.CONFLICT, evaluate(action("t")));
        // different target, same knob
        assertRejected(Reasons.CONFLICT, evaluate(action("u", "x")));
        // different target and knob: no conflict
        assertEquals(Decision.Type.HOLD_FOR_APPROVAL, evaluate(action("u", "y")).type);
        // released
        outcome(held, ActionState.REJECTED, null);
        assertEquals(Decision.Type.HOLD_FOR_APPROVAL, evaluate(action("t", "x")).type);
    }

    @Test
    public void check10_oneNodeActionInFlight()
    {
        Action first = action("a");
        assertEquals(Decision.approve(), evaluate(first));
        assertRejected(Reasons.CONCURRENCY, evaluate(action("b")));
        outcome(first, ActionState.COMMITTED, null);
        assertEquals(Decision.approve(), evaluate(action("b")));
    }

    @Test
    public void check10_clusterLane()
    {
        config.safety.cluster_approval = AutonomicConfig.SafetyConfig.APPROVAL_AUTO;
        // a node action in flight does not block a cluster action
        assertEquals(Decision.approve(), evaluate(action("a")));
        clusterBusy = "lane TOPOLOGY is claimed";
        assertRejected(Reasons.CONCURRENCY, evaluate(clusterAction("t")));
        clusterBusy = null;
        assertEquals(Decision.approve(), evaluate(clusterAction("t")));
        assertRejected(Reasons.CONCURRENCY, evaluate(clusterAction("u")));
    }

    @Test
    public void check11_ratePerActuator()
    {
        // default 2 per actuator per hour, sliding
        config.safety.max_actions_per_actuator_per_hour = 2;
        run(action("a"), "p", ActionState.COMMITTED, null);
        now.addAndGet(20 * MINUTE);
        run(action("b"), "p", ActionState.COMMITTED, null);
        Decision d = evaluate(action("c"));
        assertRejected(Reasons.RATE, d);
        assertEquals("actuator", d.detail);
        now.addAndGet(40 * MINUTE);
        // the first approval left the window
        assertEquals(Decision.approve(), evaluate(action("c")));
    }

    @Test
    public void check11_ratePerNodeAndForClusterActions()
    {
        config.safety.max_actions_per_hour = 6;
        for (int i = 0; i < 6; i++)
            run(action("t" + i), "p", ActionState.COMMITTED, null);
        Decision d = evaluate(action("t7"));
        assertRejected(Reasons.RATE, d);
        assertEquals("node", d.detail);
        now.addAndGet(HOUR);

        config.safety.cluster_approval = AutonomicConfig.SafetyConfig.APPROVAL_AUTO;
        config.safety.max_actions_per_hour = 100;
        config.safety.max_cluster_actions_per_hour = 2;
        for (int i = 0; i < 2; i++)
            run(clusterAction("c" + i), "p", ActionState.COMMITTED, null);
        d = evaluate(clusterAction("c3"));
        assertRejected(Reasons.RATE, d);
        assertEquals("cluster", d.detail);
        // a rejected proposal does not consume the rate
        assertEquals(Decision.approve(), evaluate(action("n")));
    }

    @Test
    public void check12_blastRadiusIsDelegatedToTheActuator()
    {
        actuator.blastRadius = "step of 50% exceeds 25%";
        Decision d = evaluate(action("t"));
        assertRejected(Reasons.BLAST_RADIUS, d);
        assertEquals("step of 50% exceeds 25%", d.detail);
    }

    @Test
    public void check13_approvalAutoForNodeManualForClusterByDefault()
    {
        Action node = action("t");
        assertEquals(Decision.approve(), evaluate(node));
        Action cluster = clusterAction("t");
        Decision d = evaluate(cluster);
        assertEquals(Decision.Type.HOLD_FOR_APPROVAL, d.type);
        assertEquals(Reasons.APPROVAL, d.reason);

        // operator approval evaluates checks 1 to 12 again
        gate = Reasons.PAUSED;
        assertRejected(Reasons.PAUSED, governor.approve(proposal(cluster, "policy"), view));
        assertEquals(1, governor.reservations());
        gate = null;

        Action second = clusterAction("u");
        assertEquals(Decision.Type.HOLD_FOR_APPROVAL, evaluate(second).type);
        assertEquals(Decision.approve(), governor.approve(proposal(second, "policy"), view));
        // an unknown mode is treated as manual
        config.safety.node_approval = "sometimes";
        outcome(node, ActionState.REJECTED, null);
        assertEquals(Decision.Type.HOLD_FOR_APPROVAL, evaluate(action("z")).type);
    }

    /** Every check fails at once; removing the causes one at a time exposes the checks in the order of 7.1. */
    @Test
    public void checksAreEvaluatedInOrder()
    {
        config.safety.node_approval = AutonomicConfig.SafetyConfig.APPROVAL_MANUAL;
        config.safety.quarantine_after_rollbacks = 1;
        config.safety.max_actions_per_hour = 2;
        config.safety.max_actions_per_actuator_per_hour = 100;
        config.safety.breaker_rollbacks = 100;

        // quarantine policy "bad" and put TABLE:ks.cool in cooldown
        Action rolledBack = action("cool");
        assertEquals(Decision.Type.HOLD_FOR_APPROVAL, governor.evaluate(proposal(rolledBack, "bad"), view).type);
        assertEquals(Decision.approve(), governor.approve(proposal(rolledBack, "bad"), view));
        outcome(rolledBack, ActionState.ROLLED_BACK, "NEUTRAL");
        // a held proposal on knob x, and an action in flight on knob y; two approvals in the rate window
        Action heldOnX = action("held", "x");
        assertEquals(Decision.Type.HOLD_FOR_APPROVAL, evaluate(heldOnX).type);
        Action inFlight = action("flying", "y");
        assertEquals(Decision.Type.HOLD_FOR_APPROVAL, evaluate(inFlight).type);
        assertEquals(Decision.approve(), governor.approve(proposal(inFlight, "policy"), view));

        gate = Reasons.KILLED;
        config.dry_run = true;
        actuatorEnabled = false;
        actuator.invalid = "invalid";
        nodeReady = false;
        storeAvailable = false;
        actuator.blastRadius = "too large";

        List<String> seen = new ArrayList<>();
        Runnable[] fixes = {
            () -> gate = null,
            () -> config.dry_run = false,
            () -> actuatorEnabled = true,
            () -> actuator.invalid = null,
            () -> nodeReady = true,
            () -> storeAvailable = true,
            null, // quarantine: propose under another policy
            null, // cooldown: propose for another target
            () -> outcome(heldOnX, ActionState.REJECTED, null),
            () -> outcome(inFlight, ActionState.COMMITTED, null),
            () -> config.safety.max_actions_per_hour = 100,
            () -> actuator.blastRadius = null,
            () -> config.safety.node_approval = AutonomicConfig.SafetyConfig.APPROVAL_AUTO,
        };
        String policy = "bad";
        String table = "cool";
        for (int i = 0; i < fixes.length; i++)
        {
            Action candidate = action(table, "x");
            Decision d = governor.evaluate(proposal(candidate, policy), view);
            seen.add(d.reason);
            if (d.type != Decision.Type.REJECT)
                outcome(candidate, ActionState.REJECTED, null);
            if (i == 6)
                policy = "good";
            else if (i == 7)
                table = "warm";
            else
                fixes[i].run();
        }
        assertEquals(List.of(Reasons.KILLED, Reasons.DRY_RUN, Reasons.ACTUATOR_DISABLED, Reasons.VALIDATION, Reasons.NODE_STATE,
                             Reasons.STORE_UNAVAILABLE, Reasons.QUARANTINED, Reasons.COOLDOWN, Reasons.CONFLICT, Reasons.CONCURRENCY,
                             Reasons.RATE, Reasons.BLAST_RADIUS, Reasons.APPROVAL), seen);
    }

    @Test
    public void circuitBreakerTripsOnRollbackRatioAndIsClearedByReset()
    {
        config.safety.max_actions_per_hour = 100;
        config.safety.max_actions_per_actuator_per_hour = 100;
        config.safety.quarantine_after_rollbacks = 100;
        // default: 4 of the last 6
        config.safety.breaker_rollbacks = 4;
        String[] outcomes = { "R", "C", "R", "C", "R" };
        for (int i = 0; i < outcomes.length; i++)
            run(action("t" + i), "p", outcomes[i].equals("R") ? ActionState.ROLLED_BACK : ActionState.COMMITTED, outcomes[i].equals("R") ? "NEUTRAL" : null);
        assertEquals(0, breakerTrips);
        run(action("t5"), "p", ActionState.FAILED, Reasons.ROLLBACK_FAILED);
        assertEquals(1, breakerTrips);

        governor.resetBreaker();
        for (int i = 6; i < 9; i++)
            run(action("t" + i), "p", ActionState.ROLLED_BACK, "NEUTRAL");
        assertEquals(1, breakerTrips);
        run(action("t9"), "p", ActionState.ROLLED_BACK, "NEUTRAL");
        assertEquals(2, breakerTrips);
    }

    @Test
    public void cooldownsAndQuarantinesSurviveRestartThroughLocalState()
    {
        config.safety.quarantine_after_rollbacks = 1;
        run(action("t"), "bad", ActionState.ROLLED_BACK, "NEUTRAL");
        assertEquals(2, durable.size());

        Map<String, String> stored = new LinkedHashMap<>(durable);
        governor = newGovernor();
        stored.forEach(governor::restoreState);
        assertRejected(Reasons.QUARANTINED, governor.evaluate(proposal(action("u"), "bad"), view));
        assertRejected(Reasons.COOLDOWN, governor.evaluate(proposal(action("t"), "good"), view));
        // the streak is restored too: the next rollback doubles
        now.addAndGet(30 * MINUTE);
        run(action("t"), "good", ActionState.ROLLED_BACK, "NEUTRAL");
        now.addAndGet(59 * MINUTE);
        assertRejected(Reasons.COOLDOWN, governor.evaluate(proposal(action("t"), "third"), view));

        // expired and unreadable entries are dropped from the store
        now.addAndGet(100 * HOUR);
        governor = newGovernor();
        durable.put("cooldown/garbage", "x");
        new LinkedHashMap<>(durable).forEach(governor::restoreState);
        assertEquals(Collections.emptyMap(), durable);
        assertEquals(Decision.approve(), governor.evaluate(proposal(action("t"), "bad"), view));
    }

    @Test
    public void restartCooldownAndRestoredApprovals()
    {
        governor.recordRestart("knob:TABLE:ks.t");
        assertRejected(Reasons.COOLDOWN, evaluate(action("t")));
        config.safety.max_actions_per_actuator_per_hour = 2;

        governor.restoreApproval(action("a"), now.get() - 10 * MINUTE);
        governor.restoreApproval(action("b"), now.get() - 20 * MINUTE);
        governor.restoreApproval(action("c"), now.get() - 2 * HOUR);
        assertRejected(Reasons.RATE, evaluate(action("d")));
    }
}
