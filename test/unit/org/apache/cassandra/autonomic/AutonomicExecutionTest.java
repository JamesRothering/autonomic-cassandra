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
package org.apache.cassandra.autonomic;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.FakeKnobActuator;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.KnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.autonomic.safety.Reasons;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The executor and governor wired into {@link AutonomicService}, with the real actuator thread. The clock is fake;
 * control ticks happen only when the test calls {@code controlTick()}; the only real time is the dead-man delay.
 */
public class AutonomicExecutionTest
{
    private static final SignalId GOAL = SignalId.of("goal");
    private static final Subject NODE = Subject.node();
    private static final long WINDOW = 50;
    private static final VerifySpec SPEC = new VerifySpec(GOAL, NODE, true, 0.10, List.of(), 10_000, WINDOW);

    private final AtomicLong now = new AtomicLong(TimeUnit.DAYS.toMillis(200));
    private final InMemoryKnowledgeStore store = new InMemoryKnowledgeStore();
    private final FakeKnobActuator knob = new FakeKnobActuator("knob");
    private AutonomicService service;
    private AutonomicConfig saved;
    private long ids;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        saved = new AutonomicConfig();
        saved.dry_run = c.dry_run;
        saved.startup_grace = c.startup_grace;
        saved.analyze_interval = c.analyze_interval;
        saved.monitor_interval = c.monitor_interval;
        saved.safety.verify_grace = c.safety.verify_grace;
        saved.safety.breaker_rollbacks = c.safety.breaker_rollbacks;
        saved.safety.breaker_window = c.safety.breaker_window;
        saved.safety.node_approval = c.safety.node_approval;
        saved.safety.max_actions_per_actuator_per_hour = c.safety.max_actions_per_actuator_per_hour;
        c.enabled = true;
        c.dry_run = false;
        c.startup_grace = new DurationSpec.IntMillisecondsBound("0ms");
        // the scheduled loops stay out of the way: the test drives the control tick itself
        c.analyze_interval = new DurationSpec.IntMillisecondsBound("1h");
        c.safety.verify_grace = new DurationSpec.IntSecondsBound("0s");
        c.safety.max_actions_per_actuator_per_hour = 100;
        service = newService(store);
    }

    @After
    public void teardown() throws Exception
    {
        service.shutdownBlocking();
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        c.enabled = false;
        c.dry_run = saved.dry_run;
        c.startup_grace = saved.startup_grace;
        c.analyze_interval = saved.analyze_interval;
        c.monitor_interval = saved.monitor_interval;
        c.safety.verify_grace = saved.safety.verify_grace;
        c.safety.breaker_rollbacks = saved.safety.breaker_rollbacks;
        c.safety.breaker_window = saved.safety.breaker_window;
        c.safety.node_approval = saved.safety.node_approval;
        c.safety.max_actions_per_actuator_per_hour = saved.safety.max_actions_per_actuator_per_hour;
    }

    private AutonomicService newService(KnowledgeStore s)
    {
        AutonomicService svc = new AutonomicService(now::get, () -> true, s);
        svc.registerActuator(knob);
        svc.setActuatorEnabled("knob", true);
        return svc;
    }

    private void startAndArm()
    {
        service.start();
        service.controlTick();
        assertTrue(service.isArmed());
        seedBaseline();
    }

    private void seedBaseline()
    {
        for (int i = 1; i <= 3; i++)
            service.store().accept(new Observation(GOAL, NODE, now.get() - i * 100, 100));
    }

    /** Lets the observation window pass, on the fake clock, with the given mean of the goal. */
    private void observe(double goal)
    {
        now.addAndGet(WINDOW);
        for (int i = 1; i <= 3; i++)
            service.store().accept(new Observation(GOAL, NODE, now.get() - i * 10, goal));
    }

    private Action action(String table, long value)
    {
        return new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(now.get() * 1000 + ids++, 5L), "knob", ActionScope.NODE, Subject.table("ks", table),
                          Map.of("value", String.valueOf(value)), SPEC);
    }

    private ActionRecord propose(Action a)
    {
        return service.executor().propose(new Plan(UUID.randomUUID(), UUID.randomUUID(), "policy", List.of(a), "test"), a, service.view());
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
                fail("timed out waiting for " + what);
            Thread.sleep(5);
        }
    }

    private ActionState stateOf(Action a)
    {
        return service.executor().record(a.id).map(r -> r.state).orElse(null);
    }

    /** Waits for the state and, if it is terminal, for the governor to have been told (it is told last). */
    private void awaitState(Action a, ActionState state) throws InterruptedException
    {
        await(a.dedupeKey + " " + state + ", is " + stateOf(a), () -> stateOf(a) == state);
        if (state.isTerminal())
            await("governor outcome", () -> service.governor().reservations() == 0);
    }

    /** Proposes and waits until the actuator thread has applied the action. */
    private void apply(Action a) throws InterruptedException
    {
        assertEquals(ActionState.APPROVED, propose(a).state);
        awaitState(a, ActionState.VERIFYING);
    }

    private Diagnosis selfDiagnosis(String type)
    {
        for (Diagnosis d : service.view().activeDiagnoses())
            if (d.type.equals(type))
                return d;
        return null;
    }

    @Test
    public void detectorPolicyGovernorExecutorPipelineCommits() throws Exception
    {
        UUID diagnosisId = UUID.randomUUID();
        Action a = action("t", 128);
        AtomicBoolean proposeIt = new AtomicBoolean(true);
        service.registerDetector(new Detector()
        {
            public String name() { return "d"; }
            public Set<SignalId> requires() { return Set.of(GOAL); }
            public List<Diagnosis> analyze(KnowledgeView k)
            {
                return List.of(new Diagnosis(diagnosisId, "slow", NODE, Severity.WARN, 1, 1, k.nowMillis(), "slow", Map.of(), null));
            }
        });
        service.registerPolicy(new Policy()
        {
            public String name() { return "speed_up"; }
            public Set<String> handles() { return Set.of("slow"); }
            public Optional<Plan> plan(Diagnosis d, KnowledgeView k)
            {
                return proposeIt.getAndSet(false) ? Optional.of(new Plan(UUID.randomUUID(), d.id, name(), List.of(a), "p99 over SLO")) : Optional.empty();
            }
        });
        service.start();
        seedBaseline();
        // this tick arms the layer and produces the plan; the default plan sink is the executor
        service.controlTick();
        awaitState(a, ActionState.VERIFYING);
        assertEquals(128, knob.value);
        assertEquals("1", service.getStatus().get("in_flight_actions"));
        assertEquals(1, service.getInFlightActions().size());
        assertEquals(1, service.view().recentActions(1000).size());

        observe(50);
        service.controlTick();
        awaitState(a, ActionState.COMMITTED);
        ActionRecord committed = store.actions.get(a.id);
        assertEquals(Verdict.IMPROVED, committed.verdict);
        assertEquals("speed_up", committed.policy);
        assertEquals(diagnosisId, committed.diagnosisId);
        assertEquals(128, knob.value);
        assertEquals("0", service.getStatus().get("in_flight_actions"));
        assertEquals(ActionState.COMMITTED.name(), service.getHistory(10).get(0).get("state"));
        assertEquals(5, service.getActionEvents(a.id.toString()).size());
        assertEquals(AutonomicService.State.RUNNING, service.state());
    }

    @Test
    public void dryRunIsTheDefaultAndAppliesNothing() throws Exception
    {
        DatabaseDescriptor.getAutonomicConfig().dry_run = true;
        startAndArm();
        Action a = action("t", 128);
        ActionRecord r = propose(a);
        assertEquals(ActionState.REJECTED, r.state);
        assertEquals(Reasons.DRY_RUN, r.stateReason);
        assertEquals(64, knob.value);
        // the toggle
        service.setDryRun(false);
        apply(action("t", 128));
    }

    @Test
    public void deadManRollsBackAtTheDeadlineWithoutAnyControlTick() throws Exception
    {
        startAndArm();
        Action a = action("t", 128);
        apply(a);
        assertEquals(128, knob.value);
        // the control thread never ticks again: only the task on the actuator thread can do this
        awaitState(a, ActionState.ROLLED_BACK);
        assertEquals(Reasons.DEADLINE, store.actions.get(a.id).stateReason);
        assertEquals(64, knob.value);
        assertFalse(store.state.containsKey("inflight/" + a.id));
    }

    @Test
    public void deadManRollsBackWhileTheControlThreadIsBlocked() throws Exception
    {
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean block = new AtomicBoolean();
        service.registerDetector(new Detector()
        {
            public String name() { return "blocking"; }
            public Set<SignalId> requires() { return Set.of(); }
            public List<Diagnosis> analyze(KnowledgeView k)
            {
                if (block.get())
                {
                    blocked.countDown();
                    try
                    {
                        release.await(30, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                    }
                }
                return List.of();
            }
        });
        startAndArm();
        Action a = action("t", 128);
        apply(a);
        block.set(true);
        Thread control = new Thread(service::controlTick, "blocked-control");
        control.start();
        try
        {
            assertTrue(blocked.await(10, TimeUnit.SECONDS));
            awaitState(a, ActionState.ROLLED_BACK);
            assertEquals(Reasons.DEADLINE, store.actions.get(a.id).stateReason);
            assertEquals(64, knob.value);
        }
        finally
        {
            release.countDown();
            control.join(10_000);
        }
    }

    @Test
    public void killRollsBackInFlightActionsAndSurvivesRestart() throws Exception
    {
        // long enough that only the kill can end the verification
        DatabaseDescriptor.getAutonomicConfig().safety.verify_grace = new DurationSpec.IntSecondsBound("600s");
        startAndArm();
        Action a = action("t", 128);
        apply(a);

        service.kill("operator says stop");
        // no control tick: well within one analyze_interval
        awaitState(a, ActionState.ROLLED_BACK);
        assertEquals(Reasons.KILL, store.actions.get(a.id).stateReason);
        assertEquals(64, knob.value);
        assertEquals(AutonomicService.State.PAUSED, service.state());
        assertEquals("true", service.getStatus().get("killed"));
        assertEquals("operator says stop", service.getStatus().get("pause_reason"));
        assertEquals("KILLED|operator says stop", store.state.get("paused"));
        now.addAndGet(TimeUnit.HOURS.toMillis(2));
        seedBaseline();
        assertEquals(Reasons.KILLED, propose(action("u", 128)).stateReason);

        // restart: a new service over the same store does not re-arm into RUNNING
        service.shutdownBlocking();
        service = newService(store);
        startAndArm();
        assertEquals(AutonomicService.State.PAUSED, service.state());
        assertEquals("true", service.getStatus().get("killed"));
        assertEquals("operator says stop", service.getStatus().get("pause_reason"));
        assertEquals(Reasons.KILLED, propose(action("v", 128)).stateReason);

        service.resume();
        assertEquals(AutonomicService.State.RUNNING, service.state());
        assertFalse(store.state.containsKey("paused"));
        assertEquals("false", service.getStatus().get("killed"));
        apply(action("w", 128));
    }

    @Test
    public void persistedPauseSurvivesRestartAndResumeClearsIt() throws Exception
    {
        startAndArm();
        service.pause("maintenance");
        assertEquals("PAUSED|maintenance", store.state.get("paused"));
        assertEquals(Reasons.PAUSED, propose(action("t", 128)).stateReason);

        service.shutdownBlocking();
        service = newService(store);
        service.start();
        // read during STARTING, before the layer is armed
        assertEquals("true", service.getStatus().get("paused"));
        service.controlTick();
        assertTrue(service.isArmed());
        assertEquals(AutonomicService.State.PAUSED, service.state());
        assertEquals("maintenance", service.getStatus().get("pause_reason"));
        seedBaseline();
        assertEquals(Reasons.PAUSED, propose(action("u", 128)).stateReason);

        service.resume();
        service.shutdownBlocking();
        service = newService(store);
        startAndArm();
        assertEquals(AutonomicService.State.RUNNING, service.state());
    }

    @Test
    public void pausedLayerStillFinishesVerification() throws Exception
    {
        DatabaseDescriptor.getAutonomicConfig().safety.verify_grace = new DurationSpec.IntSecondsBound("600s");
        startAndArm();
        Action a = action("t", 128);
        apply(a);
        service.pause("hold on");
        observe(100);
        service.controlTick();
        awaitState(a, ActionState.ROLLED_BACK);
        assertEquals("NEUTRAL", store.actions.get(a.id).stateReason);
        assertEquals(64, knob.value);
    }

    @Test
    public void circuitBreakerPausesTheLayerAndResumeClearsIt() throws Exception
    {
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        c.safety.verify_grace = new DurationSpec.IntSecondsBound("600s");
        c.safety.breaker_rollbacks = 2;
        c.safety.breaker_window = 3;
        startAndArm();
        for (int i = 0; i < 2; i++)
        {
            Action a = action("t" + i, 128);
            apply(a);
            observe(100);
            service.controlTick();
            awaitState(a, ActionState.ROLLED_BACK);
            now.addAndGet(20_000);
            seedBaseline();
        }
        await("circuit breaker pause", () -> service.state() == AutonomicService.State.PAUSED);
        assertEquals(Reasons.CIRCUIT_BREAKER, service.getStatus().get("pause_reason"));
        assertEquals("PAUSED|CIRCUIT_BREAKER", store.state.get("paused"));
        Diagnosis d = selfDiagnosis(AutonomicService.CIRCUIT_BREAKER);
        assertNotNull(d);
        assertEquals(Severity.CRITICAL, d.severity);
        assertEquals(Reasons.PAUSED, propose(action("x", 128)).stateReason);

        service.resume();
        assertEquals(AutonomicService.State.RUNNING, service.state());
        assertNull(selfDiagnosis(AutonomicService.CIRCUIT_BREAKER));
        // the window was cleared: one more rollback does not trip it again
        Action a = action("y", 128);
        apply(a);
        observe(100);
        service.controlTick();
        awaitState(a, ActionState.ROLLED_BACK);
        assertEquals(AutonomicService.State.RUNNING, service.state());
    }

    @Test
    public void failedRollbackPausesTheLayerAndRaisesActionFailed() throws Exception
    {
        DatabaseDescriptor.getAutonomicConfig().safety.verify_grace = new DurationSpec.IntSecondsBound("600s");
        startAndArm();
        Action a = action("t", 128);
        apply(a);
        knob.failRollback = true;
        observe(100);
        service.controlTick();
        awaitState(a, ActionState.FAILED);
        await("self pause", () -> service.state() == AutonomicService.State.PAUSED);
        assertEquals(Reasons.ACTION_FAILED, service.getStatus().get("pause_reason"));
        Diagnosis d = selfDiagnosis(AutonomicService.ACTION_FAILED);
        assertNotNull(d);
        assertEquals(Severity.CRITICAL, d.severity);
        assertEquals(a.id.toString(), d.evidence.get("action"));
        // the diagnosis is persisted by the control thread
        service.controlTick();
        assertTrue(store.diagnoses.containsKey(d.id));
    }

    @Test
    public void restartWithInflightRowMarksItRolledBackAndAppliesCooldown() throws Exception
    {
        // what a crash leaves behind: a VERIFYING row and its inflight key
        Action a = action("t", 128);
        ActionRecord verifying = new ActionRecord(a, ActionState.VERIFYING, now.get(), UUID.randomUUID(), "policy", false, null,
                                                  Map.of("previous", "64", "set", "128"), Map.of("goal@NODE", 100.0), Map.of(), null, now.get());
        store.saveAction(verifying);
        store.putState("inflight/" + a.id, "v1|" + now.get() + "|NODE|policy|" + a.dedupeKey, now.get());
        now.addAndGet(60_000);

        startAndArm();
        ActionRecord recovered = store.actions.get(a.id);
        assertEquals(ActionState.ROLLED_BACK, recovered.state);
        assertEquals(Reasons.RESTART, recovered.stateReason);
        assertFalse(store.state.containsKey("inflight/" + a.id));
        assertEquals(0, knob.count("rollback"));
        assertEquals("true", service.getStatus().get("recovered"));
        assertEquals("1", service.getStatus().get("cooldowns"));
        assertEquals(Reasons.COOLDOWN, propose(action("t", 128)).stateReason);
        assertEquals(1, service.view().recentActions(TimeUnit.HOURS.toMillis(1)).stream().filter(r -> r.id().equals(a.id)).count());
    }

    @Test
    public void layerDoesNotArmWhileTheInflightScanFails() throws Exception
    {
        store.failScan = true;
        service.start();
        for (int i = 0; i < 3; i++)
        {
            service.controlTick();
            assertFalse(service.isArmed());
        }
        assertEquals(AutonomicService.State.STARTING, service.state());
        assertEquals("false", service.getStatus().get("recovered"));
        // resume does not bypass recovery
        service.resume();
        service.controlTick();
        assertFalse(service.isArmed());

        store.failScan = false;
        service.controlTick();
        assertTrue(service.isArmed());
        assertEquals(AutonomicService.State.RUNNING, service.state());
    }

    @Test
    public void stalledActuatorThreadPausesTheLayer() throws Exception
    {
        CountDownLatch inApply = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        knob.duringApply = () -> {
            inApply.countDown();
            try
            {
                release.await(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        };
        DatabaseDescriptor.getAutonomicConfig().safety.verify_grace = new DurationSpec.IntSecondsBound("600s");
        startAndArm();
        Action a = action("t", 128);
        try
        {
            assertEquals(ActionState.APPROVED, propose(a).state);
            assertTrue(inApply.await(10, TimeUnit.SECONDS));
            service.controlTick();
            assertEquals(AutonomicService.State.RUNNING, service.state());

            now.addAndGet(DatabaseDescriptor.getAutonomicConfig().safety.action_step_timeout.toMilliseconds() + 1);
            service.controlTick();
            assertEquals(AutonomicService.State.PAUSED, service.state());
            assertEquals(Reasons.ACTUATOR_STALLED, service.getStatus().get("pause_reason"));
            assertNotNull(service.getStatus().get("actuator_stalled_in"));
            Diagnosis d = selfDiagnosis(AutonomicService.LOOP_STALLED);
            assertNotNull(d);
            assertEquals("actuator", d.evidence.get("loop"));
        }
        finally
        {
            release.countDown();
        }
        awaitState(a, ActionState.VERIFYING);
    }

    @Test
    public void disablingTheLayerRollsBackWhatIsInFlight() throws Exception
    {
        DatabaseDescriptor.getAutonomicConfig().safety.verify_grace = new DurationSpec.IntSecondsBound("600s");
        startAndArm();
        Action a = action("t", 128);
        apply(a);
        service.setEnabled(false);
        assertEquals(64, knob.value);
        assertEquals(ActionState.ROLLED_BACK, store.actions.get(a.id).state);
        assertEquals(Reasons.SHUTDOWN, store.actions.get(a.id).stateReason);
        assertFalse(store.state.containsKey("inflight/" + a.id));
    }

    @Test
    public void manualApprovalThroughTheMBean() throws Exception
    {
        DatabaseDescriptor.getAutonomicConfig().safety.node_approval = AutonomicConfig.SafetyConfig.APPROVAL_MANUAL;
        DatabaseDescriptor.getAutonomicConfig().safety.verify_grace = new DurationSpec.IntSecondsBound("600s");
        startAndArm();
        Action a = action("t", 128);
        assertEquals(ActionState.PROPOSED, propose(a).state);
        Action b = action("u", 128);
        assertEquals(ActionState.PROPOSED, propose(b).state);
        assertEquals("2", service.getStatus().get("awaiting_approval"));
        service.approve(a.id.toString());
        awaitState(a, ActionState.VERIFYING);
        service.reject(b.id.toString());
        assertEquals(Reasons.OPERATOR_REJECTED, store.actions.get(b.id).stateReason);
        try
        {
            service.revert();
            fail("revert is a stub until T4.1");
        }
        catch (UnsupportedOperationException expected)
        {
        }
        assertEquals("NODE enabled", service.getActuators().get("knob"));
    }

    @Test
    public void staleOpenDiagnosesOfAPreviousRunAreClearedAtStart() throws Exception
    {
        Diagnosis stale = new Diagnosis(UUID.randomUUID(), "old", NODE, Severity.WARN, 1, now.get() - 5000, now.get() - 1000, "old", Map.of("a", "b"), null);
        Diagnosis alreadyCleared = new Diagnosis(UUID.randomUUID(), "older", NODE, Severity.WARN, 1, now.get() - 5000, now.get() - 1000, "older", Map.of(), null);
        store.upsertDiagnosis(stale);
        store.clearDiagnosis(alreadyCleared, now.get());
        service.start();
        assertTrue(store.diagnoses.get(stale.id).cleared);
        assertEquals(Reasons.RESTART, store.diagnoses.get(stale.id).diagnosis.evidence.get("cleared_reason"));
        assertEquals("b", store.diagnoses.get(stale.id).diagnosis.evidence.get("a"));
        assertNull(store.diagnoses.get(alreadyCleared.id).diagnosis.evidence.get("cleared_reason"));
        assertEquals(1, service.getDiagnosisHistory(10).stream().filter(m -> m.get("id").equals(stale.id.toString()) && m.get("state").equals("CLEARED")).count());
    }
}
