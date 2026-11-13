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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.FakeKnobActuator;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.KnowledgeStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.autonomic.safety.Reasons;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** What the executor leaves in the real {@code system_autonomic} tables: action rows, the audit trail, durable flags. */
public class ActionAuditTest extends CQLTester
{
    private static final SignalId GOAL = SignalId.of("goal");
    private static final Subject NODE = Subject.node();
    private static final long WINDOW = 50;
    private static final VerifySpec SPEC = new VerifySpec(GOAL, NODE, true, 0.10, List.of(), 10_000, WINDOW);

    private final AtomicLong now = new AtomicLong(System.currentTimeMillis());
    private final FakeKnobActuator knob = new FakeKnobActuator("knob");
    private final KnowledgeStore store = new KnowledgeStore();
    private AutonomicService service;
    private AutonomicConfig saved;

    @Before
    public void setupService()
    {
        for (String table : new String[]{ "diagnoses", "actions", "action_events", "local_state" })
            execute("TRUNCATE system_autonomic." + table);
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        saved = new AutonomicConfig();
        saved.dry_run = c.dry_run;
        saved.startup_grace = c.startup_grace;
        saved.analyze_interval = c.analyze_interval;
        saved.monitor_interval = c.monitor_interval;
        saved.safety.verify_grace = c.safety.verify_grace;
        c.enabled = true;
        c.dry_run = false;
        c.startup_grace = new DurationSpec.IntMillisecondsBound("0ms");
        c.analyze_interval = new DurationSpec.IntMillisecondsBound("1h");
        c.safety.verify_grace = new DurationSpec.IntSecondsBound("600s");
        service = newService();
    }

    @After
    public void teardownService() throws Exception
    {
        service.shutdownBlocking();
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        c.enabled = false;
        c.dry_run = saved.dry_run;
        c.startup_grace = saved.startup_grace;
        c.analyze_interval = saved.analyze_interval;
        c.monitor_interval = saved.monitor_interval;
        c.safety.verify_grace = saved.safety.verify_grace;
    }

    private AutonomicService newService()
    {
        AutonomicService svc = new AutonomicService(now::get, () -> true, store);
        svc.registerActuator(knob);
        svc.setActuatorEnabled("knob", true);
        return svc;
    }

    private void startAndArm()
    {
        service.start();
        service.controlTick();
        assertTrue(service.isArmed());
        for (int i = 1; i <= 3; i++)
            service.store().accept(new Observation(GOAL, NODE, now.get() - i * 100, 100));
    }

    private void observe(double goal)
    {
        now.addAndGet(WINDOW);
        for (int i = 1; i <= 3; i++)
            service.store().accept(new Observation(GOAL, NODE, now.get() - i * 10, goal));
    }

    private Action action(String table)
    {
        return new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), "knob", ActionScope.NODE, Subject.table("ks", table), Map.of("value", "128"), SPEC);
    }

    private ActionRecord propose(Action a)
    {
        return service.executor().propose(new Plan(UUID.randomUUID(), UUID.randomUUID(), "policy", List.of(a), "p99 over SLO"), a, service.view());
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

    private void awaitState(Action a, ActionState state) throws InterruptedException
    {
        await(a.dedupeKey + " " + state, () -> service.executor().record(a.id).map(r -> r.state == state).orElse(false));
        if (state.isTerminal())
            await("governor outcome", () -> service.governor().reservations() == 0);
    }

    private List<String> events(Action a)
    {
        List<String> out = new ArrayList<>();
        for (KnowledgeStore.ActionEvent e : store.actionEvents(a.id))
            out.add((e.fromState == null ? "-" : e.fromState) + '>' + e.toState);
        return out;
    }

    private ActionRecord row(Action a)
    {
        return store.getAction(a.id, now.get()).orElseThrow(() -> new AssertionError("no row for " + a.id));
    }

    private static UntypedResultSet localState(String key)
    {
        return QueryProcessor.executeInternal("SELECT value FROM system_autonomic.local_state WHERE key = ?", key);
    }

    @Test
    public void happyPathIsAudited() throws Exception
    {
        startAndArm();
        Action a = action("happy");
        assertEquals(ActionState.APPROVED, propose(a).state);
        awaitState(a, ActionState.VERIFYING);

        // write-ahead state while verifying
        ActionRecord verifying = row(a);
        assertEquals(ActionState.VERIFYING, verifying.state);
        assertEquals(Map.of("previous", "64", "set", "128"), verifying.undo);
        assertEquals(100.0, verifying.baseline.get("goal@NODE"), 1e-9);
        assertEquals(1, localState("inflight/" + a.id).size());

        observe(50);
        service.controlTick();
        awaitState(a, ActionState.COMMITTED);
        ActionRecord committed = row(a);
        assertEquals(ActionState.COMMITTED, committed.state);
        assertEquals(Verdict.IMPROVED, committed.verdict);
        assertEquals(50.0, committed.result.get("goal@NODE"), 1e-9);
        assertEquals("policy", committed.policy);
        assertFalse(committed.dryRun);
        assertEquals(List.of("->PROPOSED", "PROPOSED>APPROVED", "APPROVED>APPLYING", "APPLYING>VERIFYING", "VERIFYING>COMMITTED"), events(a));
        assertTrue(store.actionEvents(a.id).get(0).detail.contains("p99 over SLO"));
        assertEquals(0, localState("inflight/" + a.id).size());
        // the commit cooldown is durable
        assertEquals(1, localState("cooldown/" + a.dedupeKey).size());
        assertEquals(128, knob.value);

        // the operator's view of it
        Map<String, String> latest = service.getHistory(5).get(0);
        assertEquals(a.id.toString(), latest.get("id"));
        assertEquals("COMMITTED", latest.get("state"));
        assertEquals(5, service.getActionEvents(a.id.toString()).size());
    }

    @Test
    public void rollbackPathIsAudited() throws Exception
    {
        startAndArm();
        Action a = action("worse");
        assertEquals(ActionState.APPROVED, propose(a).state);
        awaitState(a, ActionState.VERIFYING);
        observe(150);
        service.controlTick();
        awaitState(a, ActionState.ROLLED_BACK);
        ActionRecord rolledBack = row(a);
        assertEquals(ActionState.ROLLED_BACK, rolledBack.state);
        assertEquals("REGRESSED", rolledBack.stateReason);
        assertEquals(Verdict.REGRESSED, rolledBack.verdict);
        assertEquals(List.of("->PROPOSED", "PROPOSED>APPROVED", "APPROVED>APPLYING", "APPLYING>VERIFYING", "VERIFYING>ROLLING_BACK", "ROLLING_BACK>ROLLED_BACK"),
                     events(a));
        assertEquals(0, localState("inflight/" + a.id).size());
        assertEquals(64, knob.value);
        assertEquals(0, store.writeFailures());
    }

    @Test
    public void dryRunRejectionIsAudited()
    {
        DatabaseDescriptor.getAutonomicConfig().dry_run = true;
        startAndArm();
        Action a = action("dry");
        propose(a);
        ActionRecord rejected = row(a);
        assertEquals(ActionState.REJECTED, rejected.state);
        assertEquals(Reasons.DRY_RUN, rejected.stateReason);
        assertTrue(rejected.dryRun);
        assertEquals(Map.of("value", "128"), rejected.action.params);
        assertEquals(List.of("->PROPOSED", "PROPOSED>REJECTED"), events(a));
    }

    @Test
    public void restartRecoversAnInflightRowAndKeepsThePause() throws Exception
    {
        startAndArm();
        Action a = action("crash");
        assertEquals(ActionState.APPROVED, propose(a).state);
        awaitState(a, ActionState.VERIFYING);
        service.pause("before the crash");

        // simulate the process dying: stop the threads without the orderly rollback, then "restart" with yaml values
        AutonomicService dead = service;
        UntypedResultSet inflight = localState("inflight/" + a.id);
        assertEquals(1, inflight.size());
        String inflightValue = inflight.one().getString("value");
        dead.shutdownBlocking();
        // the orderly stop rolled back and removed the key; put back what a crash would have left
        store.saveAction(row(a).withState(ActionState.VERIFYING, null, now.get()));
        store.putState("inflight/" + a.id, inflightValue, now.get());
        knob.value = 64;
        int rollbacks = knob.count("rollback");

        service = newService();
        startAndArm();
        ActionRecord recovered = row(a);
        assertEquals(ActionState.ROLLED_BACK, recovered.state);
        assertEquals(Reasons.RESTART, recovered.stateReason);
        List<String> events = events(a);
        assertEquals("VERIFYING>ROLLED_BACK", events.get(events.size() - 1));
        assertEquals(0, localState("inflight/" + a.id).size());
        assertEquals(rollbacks, knob.count("rollback"));
        assertEquals(AutonomicService.State.PAUSED, service.state());
        assertEquals("before the crash", service.getStatus().get("pause_reason"));

        service.resume();
        assertEquals(0, localState("paused").size());
        assertEquals(Reasons.COOLDOWN, propose(action("crash")).stateReason);
    }
}
