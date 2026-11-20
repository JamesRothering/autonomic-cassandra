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
package org.apache.cassandra.autonomic.execute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.autonomic.safety.Decision;
import org.apache.cassandra.autonomic.safety.DefaultSafetyGovernor;
import org.apache.cassandra.autonomic.safety.Reasons;
import org.apache.cassandra.autonomic.safety.SafetyGovernor;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Every edge of the state machine of overview 7.2, with a fake actuator, a fake clock, an in-memory store and an
 * actuator "thread" that only runs when the test steps it.
 */
public class ActionExecutorTest
{
    private static final SignalId GOAL = SignalId.of("goal");
    private static final SignalId GUARD = SignalId.of("guard");
    private static final Subject NODE = Subject.node();
    private static final long WINDOW = 10_000;
    // goal: lower is better, 10%; guard: lower is better, tolerance 10% (abort at 20%)
    private static final VerifySpec SPEC = new VerifySpec(GOAL, NODE, true, 0.10, List.of(new VerifySpec.Guard(GUARD, NODE, true, 0.10)), WINDOW, WINDOW);

    private final AtomicLong now = new AtomicLong(TimeUnit.DAYS.toMillis(100));
    private final AutonomicConfig config = new AutonomicConfig();
    private final ObservationStore observations = new ObservationStore(1000, 100);
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(observations, now::get);
    private final InMemoryKnowledgeStore store = new InMemoryKnowledgeStore();
    private final ActuatorRegistry registry = new ActuatorRegistry();
    private final FakeKnobActuator knob = new FakeKnobActuator("knob");
    private final List<String> failures = new ArrayList<>();
    private final List<String> breaker = new ArrayList<>();
    private long ids;

    // host switches
    private volatile String gate;
    private volatile boolean killed;

    private ManualScheduler scheduler;
    private DefaultSafetyGovernor governor;
    private ActionExecutor executor;

    @Before
    public void setup()
    {
        config.dry_run = false;
        config.safety.max_actions_per_hour = 100;
        config.safety.max_actions_per_actuator_per_hour = 100;
        config.safety.quarantine_after_rollbacks = 100;
        registry.register(knob);
        registry.setEnabled("knob", true);
        newExecutor();
        seedBaseline();
    }

    /** A new governor and executor over the same store and registry: what a restart of the layer creates. */
    private void newExecutor()
    {
        scheduler = new ManualScheduler();
        governor = new DefaultSafetyGovernor(new DefaultSafetyGovernor.Environment()
        {
            public AutonomicConfig config() { return config; }
            public long nowMillis() { return now.get(); }
            public String layerGate() { return gate; }
            public Actuator actuator(String name) { return registry.get(name); }
            public boolean actuatorEnabled(String name) { return registry.isEnabled(name, config); }
            public boolean nodeReady() { return true; }
            public boolean storeAvailable() { return store.isAvailable(); }
        }, new DefaultSafetyGovernor.Journal()
        {
            public void putState(String key, String value) { store.putState(key, value, now.get()); }
            public void deleteState(String key) { store.deleteState(key); }
            public void quarantined(String policy, long untilMillis) {}
            public void breakerTripped(int rollbacks, int window) { breaker.add(rollbacks + "/" + window); }
        });
        executor = newExecutor(governor);
    }

    private ActionExecutor newExecutor(SafetyGovernor gov)
    {
        return new ActionExecutor(now::get, () -> config, gov, store, registry, scheduler, new ActionExecutor.Host()
        {
            public String gate() { return gate; }
            public boolean killed() { return killed; }
            public void actionFailed(ActionRecord record, String detail) { failures.add(record.id() + " " + detail); }
        }, () -> view);
    }

    private void sample(SignalId signal, double value, long agoMillis)
    {
        observations.accept(new Observation(signal, NODE, now.get() - agoMillis, value));
    }

    private void seedBaseline()
    {
        for (int i = 1; i <= 3; i++)
        {
            sample(GOAL, 100, i * 1000);
            sample(GUARD, 10, i * 1000);
        }
    }

    /** Lets the observation window pass with the given means. */
    private void observe(double goal, double guard)
    {
        now.addAndGet(WINDOW);
        for (int i = 1; i <= 3; i++)
        {
            sample(GOAL, goal, i * 1000);
            sample(GUARD, guard, i * 1000);
        }
    }

    private Action action(String table, long value)
    {
        return new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(now.get() * 1000 + ids++, 9L), "knob", ActionScope.NODE, Subject.table("ks", table),
                          Map.of("value", String.valueOf(value)), SPEC);
    }

    private ActionRecord propose(Action action)
    {
        return propose(action, UUID.randomUUID());
    }

    private ActionRecord propose(Action action, UUID diagnosis)
    {
        return executor.propose(new Plan(UUID.randomUUID(), diagnosis, "policy", List.of(action), "because it is hot"), action, view);
    }

    /** Proposes, applies and returns the record, which must be VERIFYING. */
    private ActionRecord applied(Action action)
    {
        assertEquals(ActionState.APPROVED, propose(action).state);
        scheduler.runAll();
        ActionRecord r = record(action);
        assertEquals(ActionState.VERIFYING, r.state);
        return r;
    }

    private ActionRecord record(Action action)
    {
        return executor.record(action.id).orElseThrow(() -> new AssertionError("no record of " + action.id));
    }

    /** Control tick followed by whatever it asked the actuator thread to do. */
    private void tick()
    {
        executor.onControlTick(view);
        scheduler.runAll();
    }

    private void assertTerminal(Action action, ActionState state, String reason)
    {
        ActionRecord r = record(action);
        assertEquals(state, r.state);
        assertEquals(reason, r.stateReason);
        assertEquals("the store has the terminal record", r, store.actions.get(action.id));
        assertFalse("inflight key is deleted", store.state.containsKey("inflight/" + action.id));
        assertEquals("no dead-man task is left", 0, scheduler.activeTimers());
        assertTrue(executor.liveActions().isEmpty());
        assertEquals("the governor's reservation is released", 0, governor.reservations());
    }

    // ---- happy path and verdicts ----

    @Test
    public void improvedCommits()
    {
        Action a = action("t", 128);
        assertEquals(ActionState.APPROVED, propose(a).state);
        assertEquals(64, knob.value);
        assertEquals(1, executor.queuedActions());

        scheduler.runAll();
        ActionRecord verifying = record(a);
        assertEquals(ActionState.VERIFYING, verifying.state);
        assertEquals(128, knob.value);
        assertEquals(Map.of("previous", "64", "set", "128"), verifying.undo);
        assertEquals(100.0, verifying.baseline.get("goal@NODE"), 1e-9);
        assertEquals(10.0, verifying.baseline.get("guard@NODE"), 1e-9);
        // write-ahead: the inflight key and the undo are durable while verifying
        assertTrue(store.state.get("inflight/" + a.id).endsWith("|NODE|policy|knob:TABLE:ks.t"));
        assertEquals(verifying, store.actions.get(a.id));
        // rule 9: dead-man at observation window + verify_grace
        assertEquals(1, scheduler.activeTimers());
        assertEquals(WINDOW + 30_000, scheduler.timers.get(0).delayMillis);

        // before the window has passed nothing happens
        now.addAndGet(WINDOW - 1);
        tick();
        assertEquals(ActionState.VERIFYING, record(a).state);
        now.addAndGet(1 - WINDOW);

        observe(80, 10);
        tick();
        assertTerminal(a, ActionState.COMMITTED, null);
        ActionRecord committed = record(a);
        assertEquals(Verdict.IMPROVED, committed.verdict);
        assertEquals(80.0, committed.result.get("goal@NODE"), 1e-9);
        assertEquals(128, knob.value);
        assertEquals(0, knob.count("rollback"));
        assertEquals(List.of("->PROPOSED", "PROPOSED>APPROVED", "APPROVED>APPLYING", "APPLYING>VERIFYING", "VERIFYING>COMMITTED"),
                     store.transitions(a.id));
        assertTrue(store.actionEvents(a.id).get(0).detail.contains("because it is hot"));
        assertEquals(1, executor.recentActions(1000).size());
    }

    @Test
    public void neutralRegressedAndInconclusiveRollBack()
    {
        config.safety.breaker_rollbacks = 100;
        double[][] observed = { { 95, 10 }, { 150, 10 }, { 80, 11.5 } };
        String[] reasons = { "NEUTRAL", "REGRESSED", "REGRESSED" };
        for (int i = 0; i < observed.length; i++)
        {
            Action a = action("t" + i, 128);
            applied(a);
            observe(observed[i][0], observed[i][1]);
            tick();
            assertTerminal(a, ActionState.ROLLED_BACK, reasons[i]);
            assertEquals(Verdict.valueOf(reasons[i]), record(a).verdict);
            assertEquals(64, knob.value);
            assertEquals(List.of("->PROPOSED", "PROPOSED>APPROVED", "APPROVED>APPLYING", "APPLYING>VERIFYING", "VERIFYING>ROLLING_BACK", "ROLLING_BACK>ROLLED_BACK"),
                         store.transitions(a.id));
            seedBaselineAgain();
        }
        // no samples in the window: INCONCLUSIVE
        Action a = action("quiet", 128);
        applied(a);
        now.addAndGet(WINDOW);
        tick();
        assertTerminal(a, ActionState.ROLLED_BACK, "INCONCLUSIVE");
        assertEquals(64, knob.value);
    }

    private void seedBaselineAgain()
    {
        now.addAndGet(WINDOW * 2);
        seedBaseline();
    }

    @Test
    public void actuatorVerdictIsUsedAndAThrowingVerifyIsInconclusive()
    {
        knob.verdict = Verdict.IMPROVED;
        Action a = action("t", 128);
        applied(a);
        now.addAndGet(WINDOW);
        tick();
        assertTerminal(a, ActionState.COMMITTED, null);

        seedBaselineAgain();
        FakeKnobActuator throwing = new FakeKnobActuator("throwing")
        {
            @Override
            public Verdict verify(Action action, Applied applied, Map<String, Double> baseline, KnowledgeView knowledge)
            {
                throw new IllegalStateException("verify");
            }
        };
        registry.register(throwing);
        registry.setEnabled("throwing", true);
        Action b = new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(now.get() * 1000, 1L), "throwing", ActionScope.NODE, NODE, Map.of("value", "1"), SPEC);
        assertEquals(ActionState.APPROVED, propose(b).state);
        scheduler.runAll();
        observe(50, 10);
        tick();
        assertTerminal(b, ActionState.ROLLED_BACK, "INCONCLUSIVE");
        assertEquals(64, throwing.value);
    }

    // ---- rejection before anything is applied ----

    @Test
    public void dryRunRecordsRejectedWithTheFullPlan()
    {
        config.dry_run = true;
        Action a = action("t", 128);
        ActionRecord r = propose(a);
        assertEquals(ActionState.REJECTED, r.state);
        assertEquals(Reasons.DRY_RUN, r.stateReason);
        assertTrue(r.dryRun);
        assertEquals(0, scheduler.pending());
        ActionRecord stored = store.actions.get(a.id);
        assertEquals(r, stored);
        assertEquals(Map.of("value", "128"), stored.action.params);
        assertEquals("policy", stored.policy);
        assertEquals(List.of("->PROPOSED", "PROPOSED>REJECTED"), store.transitions(a.id));
        assertTrue(store.actionEvents(a.id).get(0).detail.contains("because it is hot"));
        assertEquals(64, knob.value);
    }

    @Test
    public void identicalRejectionsAreRecordedOncePerInterval()
    {
        config.dry_run = true;
        assertEquals(ActionState.REJECTED, propose(action("t", 128)).state);
        for (int i = 0; i < 10; i++)
        {
            now.addAndGet(15_000);
            assertNull(propose(action("t", 128)));
        }
        assertEquals(1, store.actions.size());
        assertEquals(Long.valueOf(11), executor.rejectionCounts().get(Reasons.DRY_RUN));
        // another target, and the same target later, are recorded
        assertEquals(ActionState.REJECTED, propose(action("u", 128)).state);
        now.addAndGet(ActionExecutor.REPEATED_REJECTION_INTERVAL_MILLIS);
        assertEquals(ActionState.REJECTED, propose(action("t", 128)).state);
        assertEquals(3, store.actions.size());
    }

    @Test
    public void insufficientBaselineRejects()
    {
        now.addAndGet(WINDOW * 5);
        sample(GOAL, 100, 1000);
        sample(GOAL, 100, 2000);
        Action a = action("t", 128);
        ActionRecord r = propose(a);
        assertEquals(ActionState.REJECTED, r.state);
        assertEquals(Reasons.INSUFFICIENT_BASELINE, r.stateReason);
        assertEquals(0, governor.reservations());
        assertEquals(0, knob.count("apply"));
    }

    @Test
    public void pausedOrKilledBetweenApprovalAndPickupIsNotApplied()
    {
        for (String g : new String[]{ Reasons.PAUSED, Reasons.KILLED })
        {
            Action a = action("t" + g, 128);
            assertEquals(ActionState.APPROVED, propose(a).state);
            gate = g;
            scheduler.runAll();
            assertTerminal(a, ActionState.REJECTED, g);
            assertEquals(List.of("->PROPOSED", "PROPOSED>APPROVED", "APPROVED>REJECTED"), store.transitions(a.id));
            gate = null;
        }
        assertEquals(0, knob.count("apply"));
    }

    @Test
    public void executorBusyWhenTheHandOffQueueIsFull()
    {
        // a governor that approves everything, so that only the queue bound stands in the way
        SafetyGovernor permissive = new SafetyGovernor()
        {
            public Decision evaluate(ActionRecord proposal, KnowledgeView knowledge) { return Decision.approve(); }
            public Decision approve(ActionRecord proposal, KnowledgeView knowledge) { return Decision.approve(); }
            public void recordOutcome(Action action, ActionOutcome outcome) {}
        };
        executor = newExecutor(permissive);
        List<Action> actions = new ArrayList<>();
        for (int i = 0; i < ActionExecutor.QUEUE_CAPACITY; i++)
        {
            Action a = action("t" + i, 128);
            actions.add(a);
            assertEquals(ActionState.APPROVED, propose(a).state);
        }
        ActionRecord r = propose(action("one_too_many", 128));
        assertEquals(ActionState.REJECTED, r.state);
        assertEquals(Reasons.EXECUTOR_BUSY, r.stateReason);
        assertEquals(ActionExecutor.QUEUE_CAPACITY, executor.queuedActions());

        // the queue drains and accepts again
        gate = Reasons.PAUSED;
        scheduler.runAll();
        gate = null;
        assertEquals(0, executor.queuedActions());
        assertEquals(ActionState.APPROVED, propose(action("later", 128)).state);

        // a scheduler that refuses the task is reported the same way
        scheduler.rejectTasks = true;
        r = propose(action("refused", 128));
        assertEquals(Reasons.EXECUTOR_BUSY, r.stateReason);
        assertEquals(1, executor.queuedActions());
    }

    // ---- failures of the actuator ----

    @Test
    public void applyThrowingBeforeAnyEffectIsRolledBackWithoutCallingRollback()
    {
        knob.failApply = FakeKnobActuator.Fail.BEFORE_EFFECT;
        Action a = action("t", 128);
        assertEquals(ActionState.APPROVED, propose(a).state);
        scheduler.runAll();
        assertTerminal(a, ActionState.ROLLED_BACK, Reasons.APPLY_FAILED);
        assertEquals(0, knob.count("rollback"));
        assertEquals(64, knob.value);
        assertEquals(List.of("->PROPOSED", "PROPOSED>APPROVED", "APPROVED>APPLYING", "APPLYING>ROLLING_BACK", "ROLLING_BACK>ROLLED_BACK"),
                     store.transitions(a.id));
        assertTrue(failures.isEmpty());
        // it counts as a rollback: the dedupe key is in the longer cooldown
        now.addAndGet(TimeUnit.MINUTES.toMillis(29));
        seedBaseline();
        knob.failApply = FakeKnobActuator.Fail.NONE;
        assertEquals(Reasons.COOLDOWN, propose(action("t", 128)).stateReason);
    }

    @Test
    public void applyThrowingAfterPartialEffectIsRolledBackFromTheRecordedUndo()
    {
        knob.failApply = FakeKnobActuator.Fail.AFTER_PARTIAL_EFFECT;
        Action a = action("t", 128);
        assertEquals(ActionState.APPROVED, propose(a).state);
        scheduler.runAll();
        assertEquals(1, knob.count("rollback"));
        assertEquals("the partial change is undone", 64, knob.value);
        assertTerminal(a, ActionState.ROLLED_BACK, Reasons.APPLY_FAILED);
    }

    @Test
    public void rollbackThrowingFailsTheActionAndReportsToTheHost()
    {
        Action a = action("t", 128);
        applied(a);
        knob.failRollback = true;
        observe(100, 10);
        tick();
        assertTerminal(a, ActionState.FAILED, Reasons.ROLLBACK_FAILED);
        assertEquals(128, knob.value);
        assertEquals(1, failures.size());
        assertEquals(List.of("->PROPOSED", "PROPOSED>APPROVED", "APPROVED>APPLYING", "APPLYING>VERIFYING", "VERIFYING>ROLLING_BACK", "ROLLING_BACK>FAILED"),
                     store.transitions(a.id));
    }

    @Test
    public void rollbackThrowingAfterAFailedApplyAlsoFails()
    {
        knob.failApply = FakeKnobActuator.Fail.AFTER_PARTIAL_EFFECT;
        knob.failRollback = true;
        Action a = action("t", 128);
        propose(a);
        scheduler.runAll();
        assertTerminal(a, ActionState.FAILED, Reasons.ROLLBACK_FAILED);
        assertEquals(1, failures.size());
    }

    // ---- operator override ----

    @Test
    public void operatorOverrideSupersedesAtVerification()
    {
        Action a = action("t", 128);
        applied(a);
        knob.value = 256;
        observe(200, 10);
        tick();
        assertTerminal(a, ActionState.SUPERSEDED, Reasons.OVERRIDDEN);
        assertEquals("the operator's value is left alone", 256, knob.value);
        assertEquals(0, knob.count("rollback"));
        assertEquals(0, knob.count("verify"));
        assertEquals("VERIFYING>SUPERSEDED", store.transitions(a.id).get(4));
    }

    @Test
    public void operatorOverrideSupersedesInsteadOfRollingBackOnKillAndDeadline()
    {
        config.safety.breaker_rollbacks = 100;
        Action a = action("t", 128);
        applied(a);
        knob.value = 256;
        executor.rollbackAll(Reasons.KILL);
        scheduler.runAll();
        assertTerminal(a, ActionState.SUPERSEDED, Reasons.OVERRIDDEN);
        assertEquals(256, knob.value);

        seedBaselineAgain();
        Action b = action("u", 512);
        applied(b);
        knob.value = 1024;
        assertEquals(1, scheduler.fireTimers());
        assertTerminal(b, ActionState.SUPERSEDED, Reasons.OVERRIDDEN);
        assertEquals(1024, knob.value);
        assertEquals(0, knob.count("rollback"));
    }

    // ---- rollback without a verdict ----

    @Test
    public void deadManRollsBackWhenTheControlThreadNeverTicks()
    {
        Action a = action("t", 128);
        applied(a);
        // no onControlTick at all; only the task the actuator thread scheduled for itself
        assertEquals(1, scheduler.fireTimers());
        assertTerminal(a, ActionState.ROLLED_BACK, Reasons.DEADLINE);
        assertEquals(64, knob.value);
        assertNull(record(a).verdict);
    }

    @Test
    public void verificationCancelsTheDeadMan()
    {
        Action a = action("t", 128);
        applied(a);
        observe(50, 10);
        tick();
        assertTerminal(a, ActionState.COMMITTED, null);
        assertTrue(scheduler.timers.get(0).cancelled);
        assertEquals(0, scheduler.fireTimers());
        assertEquals(128, knob.value);
    }

    @Test
    public void guardBeyondAbortToleranceRollsBackEarly()
    {
        Action a = action("t", 128);
        applied(a);
        // 15% worse: beyond tolerance but not beyond abort tolerance; the window has not passed
        now.addAndGet(4000);
        for (int i = 1; i <= 3; i++)
            sample(GUARD, 11.5, i * 1000);
        tick();
        assertEquals(ActionState.VERIFYING, record(a).state);
        // mean since apply now 25% worse
        now.addAndGet(2000);
        for (int i = 1; i <= 3; i++)
            sample(GUARD, 13.5, i * 500);
        tick();
        assertTerminal(a, ActionState.ROLLED_BACK, Reasons.EARLY_ABORT);
        assertEquals(64, knob.value);
        assertEquals(0, knob.count("verify"));
    }

    @Test
    public void killDuringVerifyingRollsBack()
    {
        Action a = action("t", 128);
        applied(a);
        killed = true;
        gate = Reasons.KILLED;
        assertFalse(executor.rollbackAll(Reasons.KILL).isDone());
        scheduler.runAll();
        assertTerminal(a, ActionState.ROLLED_BACK, Reasons.KILL);
        assertEquals(64, knob.value);
        // a verification requested earlier finds nothing to do
        tick();
        assertEquals(ActionState.ROLLED_BACK, record(a).state);
    }

    @Test
    public void killDuringApplyingRollsBackAsSoonAsApplyReturns()
    {
        knob.duringApply = () -> {
            killed = true;
            gate = Reasons.KILLED;
        };
        Action a = action("t", 128);
        assertEquals(ActionState.APPROVED, propose(a).state);
        scheduler.runAll();
        assertTerminal(a, ActionState.ROLLED_BACK, Reasons.KILL);
        assertEquals(64, knob.value);
        assertEquals(1, knob.count("rollback"));
        assertEquals(List.of("->PROPOSED", "PROPOSED>APPROVED", "APPROVED>APPLYING", "APPLYING>ROLLING_BACK", "ROLLING_BACK>ROLLED_BACK"),
                     store.transitions(a.id));
        assertEquals("never verified, so no dead-man was scheduled", 0, scheduler.timers.size());
    }

    @Test
    public void killCompletesTheFutureAndRollsBackNothingWhenIdle()
    {
        java.util.concurrent.Future<Void> done = executor.rollbackAll(Reasons.KILL);
        scheduler.runAll();
        assertTrue(done.isDone());
    }

    // ---- irreversible actuators (rule 10) ----

    @Test
    public void irreversibleActuatorCommitsWithTheVerdictRecorded()
    {
        knob.reversible = false;
        UUID diagnosis = UUID.randomUUID();
        view.setActiveDiagnoses(List.of(new Diagnosis(diagnosis, "x", NODE, Severity.CRITICAL, 1, 0, 0, "", Map.of(), null)));
        Action a = action("t", 128);
        assertEquals(ActionState.APPROVED, propose(a, diagnosis).state);
        scheduler.runAll();
        observe(100, 10);
        tick();
        assertTerminal(a, ActionState.COMMITTED, Reasons.IRREVERSIBLE);
        assertEquals(Verdict.NEUTRAL, record(a).verdict);
        assertEquals(0, knob.count("rollback"));

        // without a CRITICAL diagnosis it is not selected at all
        now.addAndGet(TimeUnit.DAYS.toMillis(2));
        seedBaseline();
        assertEquals(Reasons.VALIDATION, propose(action("t", 256)).stateReason);
    }

    @Test
    public void irreversibleActuatorIsCommittedNotRolledBackOnKill()
    {
        knob.reversible = false;
        UUID diagnosis = UUID.randomUUID();
        view.setActiveDiagnoses(List.of(new Diagnosis(diagnosis, "x", NODE, Severity.CRITICAL, 1, 0, 0, "", Map.of(), null)));
        Action a = action("t", 128);
        propose(a, diagnosis);
        scheduler.runAll();
        executor.rollbackAll(Reasons.KILL);
        scheduler.runAll();
        assertTerminal(a, ActionState.COMMITTED, Reasons.IRREVERSIBLE);
        assertEquals(128, knob.value);
        assertEquals(0, knob.count("rollback"));
    }

    // ---- knowledge store unavailable ----

    @Test
    public void storeUnavailableMeansNoApproval()
    {
        store.failWrites = true;
        // the first proposal finds out: its PROPOSED record cannot be written
        ActionRecord r = propose(action("t", 128));
        assertEquals(Reasons.STORE_UNAVAILABLE, r.stateReason);
        // from then on governor check 6 rejects, and each tick probes the store again
        for (int i = 0; i < 3; i++)
            assertEquals(Reasons.STORE_UNAVAILABLE, propose(action("t", 128)).stateReason);
        assertEquals(0, scheduler.pending());
        assertEquals(0, governor.reservations());
        assertEquals(0, knob.count("apply"));
        assertTrue(store.actions.isEmpty());

        // the probe that succeeds re-opens the gate
        store.failWrites = false;
        assertEquals(Reasons.STORE_UNAVAILABLE, propose(action("t", 128)).stateReason);
        assertEquals(ActionState.APPROVED, propose(action("t", 128)).state);
    }

    @Test
    public void storeFailingBeforeApplyMeansNothingIsApplied()
    {
        Action a = action("t", 128);
        assertEquals(ActionState.APPROVED, propose(a).state);
        store.failWrites = true;
        scheduler.runAll();
        ActionRecord r = record(a);
        assertEquals(ActionState.REJECTED, r.state);
        assertEquals(Reasons.STORE_UNAVAILABLE, r.stateReason);
        assertEquals(0, knob.count("apply"));
        assertEquals(0, governor.reservations());
    }

    @Test
    public void storeFailingAfterApplyRollsBackAtOnce()
    {
        knob.duringApply = () -> store.failWrites = true;
        Action a = action("t", 128);
        assertEquals(ActionState.APPROVED, propose(a).state);
        scheduler.runAll();
        ActionRecord r = record(a);
        assertEquals(ActionState.ROLLED_BACK, r.state);
        assertEquals(Reasons.STORE_UNAVAILABLE, r.stateReason);
        assertEquals("the undo could not be persisted, so the change is not kept", 64, knob.value);
        assertEquals(1, knob.count("rollback"));
    }

    @Test
    public void rollbacksStillRunWhileTheStoreIsUnavailable()
    {
        Action a = action("t", 128);
        applied(a);
        store.failWrites = true;
        observe(100, 10);
        tick();
        ActionRecord r = record(a);
        assertEquals(ActionState.ROLLED_BACK, r.state);
        assertEquals("NEUTRAL", r.stateReason);
        assertEquals(64, knob.value);
        assertEquals(0, governor.reservations());

        // and the dead-man rollback too
        store.failWrites = false;
        store.putState("probe", "x", now.get());
        seedBaselineAgain();
        now.addAndGet(TimeUnit.HOURS.toMillis(1));
        seedBaseline();
        Action b = action("u", 128);
        applied(b);
        store.failWrites = true;
        assertEquals(1, scheduler.fireTimers());
        assertEquals(ActionState.ROLLED_BACK, record(b).state);
        assertEquals(64, knob.value);
        // the stale inflight key is what recovery cleans up
        assertTrue(store.state.containsKey("inflight/" + b.id));
    }

    @Test
    public void staleRowsAndInflightKeysAreWrittenOnceTheStoreIsWritableAgain()
    {
        Action a = action("t", 128);
        applied(a);
        assertEquals(ActionState.VERIFYING, store.actions.get(a.id).state);
        assertTrue(store.state.containsKey("inflight/" + a.id));
        store.failWrites = true;
        observe(100, 10);
        tick();
        assertEquals(ActionState.ROLLED_BACK, record(a).state);
        // on disk: the row before the failure, and the inflight key
        assertEquals(ActionState.VERIFYING, store.actions.get(a.id).state);
        assertTrue(store.state.containsKey("inflight/" + a.id));

        // the store recovers: the next successful write retries the stale ones
        store.failWrites = false;
        assertEquals(Reasons.STORE_UNAVAILABLE, propose(action("u", 128)).stateReason);
        assertEquals(ActionState.ROLLED_BACK, store.actions.get(a.id).state);
        assertEquals("NEUTRAL", store.actions.get(a.id).stateReason);
        assertFalse(store.state.containsKey("inflight/" + a.id));
    }

    // ---- manual approval ----

    @Test
    public void manualApprovalHoldsUntilTheOperatorApproves()
    {
        config.safety.node_approval = AutonomicConfig.SafetyConfig.APPROVAL_MANUAL;
        Action a = action("t", 128);
        ActionRecord held = propose(a);
        assertEquals(ActionState.PROPOSED, held.state);
        assertEquals(0, scheduler.pending());
        assertEquals(List.of("->PROPOSED"), store.transitions(a.id));
        // the same problem proposed again conflicts with the held proposal
        assertEquals(Reasons.CONFLICT, propose(action("t", 128)).stateReason);
        tick();
        assertEquals(ActionState.PROPOSED, record(a).state);

        assertEquals(ActionState.APPROVED, executor.approve(a.id).state);
        assertEquals("operator", store.actionEvents(a.id).get(1).detail);
        scheduler.runAll();
        assertEquals(ActionState.VERIFYING, record(a).state);
        assertEquals(128, knob.value);
        try
        {
            executor.approve(a.id);
            fail();
        }
        catch (IllegalArgumentException expected)
        {
        }
    }

    @Test
    public void approvalEvaluatesTheOtherChecksAgain()
    {
        config.safety.node_approval = AutonomicConfig.SafetyConfig.APPROVAL_MANUAL;
        Action a = action("t", 128);
        propose(a);
        gate = Reasons.PAUSED;
        ActionRecord r = executor.approve(a.id);
        assertEquals(ActionState.REJECTED, r.state);
        assertEquals(Reasons.PAUSED, r.stateReason);
        assertEquals(0, governor.reservations());
        assertEquals(0, scheduler.pending());
    }

    @Test
    public void operatorRejectionAndProposalTtl()
    {
        config.safety.node_approval = AutonomicConfig.SafetyConfig.APPROVAL_MANUAL;
        Action a = action("t", 128);
        propose(a);
        assertEquals(Reasons.OPERATOR_REJECTED, executor.reject(a.id).stateReason);
        assertEquals(0, governor.reservations());

        Action b = action("u", 128);
        propose(b);
        now.addAndGet(TimeUnit.HOURS.toMillis(1) - 1);
        tick();
        assertEquals(ActionState.PROPOSED, record(b).state);
        now.addAndGet(1);
        tick();
        ActionRecord expired = record(b);
        assertEquals(ActionState.REJECTED, expired.state);
        assertEquals(Reasons.PROPOSAL_TTL, expired.stateReason);
        assertEquals(List.of("->PROPOSED", "PROPOSED>REJECTED"), store.transitions(b.id));
        assertEquals(0, governor.reservations());
        assertEquals(0, knob.count("apply"));

        // approval that arrives after the ttl, before any tick noticed
        seedBaseline();
        Action c = action("v", 128);
        propose(c);
        now.addAndGet(TimeUnit.HOURS.toMillis(1));
        assertEquals(Reasons.PROPOSAL_TTL, executor.approve(c.id).stateReason);
    }

    // ---- cluster seam ----

    @Test
    public void clusterScopedActionIsNotExecutedWithoutAClusterExecution()
    {
        FakeKnobActuator cluster = new FakeKnobActuator("cluster_knob");
        cluster.scope = ActionScope.CLUSTER;
        registry.register(cluster);
        registry.setEnabled("cluster_knob", true);
        Action a = new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(now.get() * 1000, 3L), "cluster_knob", ActionScope.CLUSTER, Subject.cluster(), Map.of("value", "1"), SPEC);
        // the production governor already refuses: no ledger, so the cluster gate is closed
        assertEquals(Reasons.CLUSTER_DISABLED, propose(a).stateReason);
        assertEquals(0, cluster.count("apply"));
    }

    // ---- restart recovery (overview 7.6) ----

    @Test
    public void restartMarksInflightActionRolledBackWithoutCallingRollbackAndAppliesCooldown()
    {
        Action a = action("t", 128);
        applied(a);
        assertTrue(store.state.containsKey("inflight/" + a.id));
        int rollbacks = knob.count("rollback");

        // the process dies here: a new executor and governor over the same store
        newExecutor();
        ActionExecutor.Recovery recovery = executor.recover(Set.of());
        assertTrue(recovery.scanned);
        assertTrue(recovery.complete);
        assertEquals(1, recovery.rolledBack);
        assertNull(recovery.paused);
        ActionRecord stored = store.actions.get(a.id);
        assertEquals(ActionState.ROLLED_BACK, stored.state);
        assertEquals(Reasons.RESTART, stored.stateReason);
        assertEquals("VERIFYING>ROLLED_BACK", store.transitions(a.id).get(4));
        assertFalse(store.state.containsKey("inflight/" + a.id));
        assertEquals("a restart has restored the knob already", rollbacks, knob.count("rollback"));
        assertEquals(stored, executor.record(a.id).get());

        // post-restart cooldown on the dedupe key
        seedBaseline();
        assertEquals(Reasons.COOLDOWN, propose(action("t", 128)).stateReason);
        now.addAndGet(TimeUnit.MINUTES.toMillis(30));
        seedBaseline();
        assertEquals(ActionState.APPROVED, propose(action("t", 128)).state);

        // recovery is idempotent
        assertEquals(0, executor.recover(Set.of()).rolledBack);
    }

    @Test
    public void restartClosesProposalsThatNeverReachedTheActuatorThread()
    {
        config.safety.node_approval = AutonomicConfig.SafetyConfig.APPROVAL_MANUAL;
        Action held = action("t", 128);
        propose(held);
        newExecutor();
        assertTrue(executor.recover(Set.of()).complete);
        assertEquals(ActionState.REJECTED, store.actions.get(held.id).state);
        assertEquals(Reasons.RESTART, store.actions.get(held.id).stateReason);
    }

    @Test
    public void recoveryReportsAFailedScanAndAPersistedPause()
    {
        Action a = action("t", 128);
        applied(a);
        store.putState(ActionExecutor.PAUSED_KEY, "KILLED|because", now.get());
        newExecutor();
        store.failScan = true;
        ActionExecutor.Recovery recovery = executor.recover(Set.of());
        assertFalse(recovery.scanned);
        assertFalse(recovery.complete);
        assertEquals(ActionState.VERIFYING, store.actions.get(a.id).state);

        store.failScan = false;
        recovery = executor.recover(Set.of());
        assertTrue(recovery.complete);
        assertEquals("KILLED|because", recovery.paused);
        assertEquals(ActionState.ROLLED_BACK, store.actions.get(a.id).state);
    }

    @Test
    public void recoveryIsIncompleteWhileTheStoreCannotBeWritten()
    {
        Action a = action("t", 128);
        applied(a);
        newExecutor();
        store.failWrites = true;
        ActionExecutor.Recovery recovery = executor.recover(Set.of());
        assertTrue(recovery.scanned);
        assertFalse(recovery.complete);
        store.failWrites = false;
        assertTrue(executor.recover(Set.of()).complete);
        assertEquals(ActionState.ROLLED_BACK, store.actions.get(a.id).state);
    }

    @Test
    public void actionLeftAppliedByAStopInTheSameProcessIsFailedNotRolledBack()
    {
        Action a = action("t", 128);
        applied(a);
        newExecutor();
        assertTrue(executor.recover(Set.of(a.id)).complete);
        assertEquals(ActionState.FAILED, store.actions.get(a.id).state);
        assertEquals(Reasons.STOPPED_WITHOUT_ROLLBACK, store.actions.get(a.id).stateReason);
        assertEquals(1, failures.size());
    }

    // ---- stall marker ----

    @Test
    public void stalledStepIsVisibleWhileAStepRuns()
    {
        String[] seen = new String[2];
        knob.duringApply = () -> {
            seen[0] = executor.stalledStep(30_000);
            now.addAndGet(30_001);
            seen[1] = executor.stalledStep(30_000);
        };
        Action a = action("t", 128);
        propose(a);
        scheduler.runAll();
        assertNull(seen[0]);
        assertEquals("start " + a.id, seen[1]);
        assertNull(executor.stalledStep(30_000));
    }
}
