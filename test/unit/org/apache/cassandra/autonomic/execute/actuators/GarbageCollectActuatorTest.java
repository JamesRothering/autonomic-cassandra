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
package org.apache.cassandra.autonomic.execute.actuators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.AbstractActuatorContractTest;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.ActionOutcome;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.Applied;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.autonomic.safety.ClusterGate;
import org.apache.cassandra.autonomic.safety.Decision;
import org.apache.cassandra.autonomic.safety.DefaultSafetyGovernor;
import org.apache.cassandra.autonomic.safety.Reasons;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Contract test plus rule 10 of overview 7.3 through the real governor. */
public class GarbageCollectActuatorTest extends AbstractActuatorContractTest
{
    private static final long HOUR = TimeUnit.HOURS.toMillis(1);
    private static final VerifySpec SPEC = new VerifySpec(SignalId.of("g"), Subject.node(), true, 0.1, List.of(), 1000, 1000);

    private final List<String> compacted = new ArrayList<>();
    private final List<Runnable> queued = new ArrayList<>();
    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final GarbageCollectActuator actuator = new GarbageCollectActuator(new GarbageCollectActuator.Compactor()
    {
        public boolean tableExists(String keyspace, String table)
        {
            return !table.equals("missing");
        }

        public void garbageCollect(String keyspace, String table, String option)
        {
            compacted.add(keyspace + '.' + table + ' ' + option);
        }
    }, queued::add);
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(8, 8), now::get);
    private long ids;

    private Action action(String keyspace, String table)
    {
        return new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + ids++, 5L), GarbageCollectActuator.NAME, ActionScope.NODE,
                          Subject.table(keyspace, table), Map.of(), SPEC);
    }

    private void drain()
    {
        queued.forEach(Runnable::run);
        queued.clear();
    }

    @Override
    protected Actuator actuator()
    {
        return actuator;
    }

    @Override
    protected Action newAction()
    {
        return action("ks", "t" + ids);
    }

    @Override
    protected ActionContext context()
    {
        return new ActionContext()
        {
            public long stepDeadlineMillis() { return Long.MAX_VALUE; }
            public long nowMillis() { return now.get(); }
            public boolean killed() { return false; }
            public void recordUndo(Map<String, String> undo) {}
        };
    }

    @Override
    protected Object snapshot()
    {
        drain();
        return List.copyOf(compacted);
    }

    @Override
    protected void operatorOverride()
    {
    }

    @Test
    public void applyDoesNotWaitForTheCompaction() throws Exception
    {
        Action a = action("ks", "t");
        Applied applied = actuator.apply(a, context());
        assertEquals("TABLE:ks.t", applied.undo.get("target"));
        assertTrue(compacted.isEmpty());
        assertEquals(1, queued.size());
        drain();
        assertEquals(List.of("ks.t ROW"), compacted);
        assertFalse(actuator.reversible());
    }

    @Test
    public void validateRejectsSystemMissingAndRepeatedTargets() throws Exception
    {
        assertFalse(actuator.validate(action("ks", "t"), view).isPresent());
        assertTrue(actuator.validate(action("system", "local"), view).isPresent());
        assertTrue(actuator.validate(action("ks", "missing"), view).get().contains("no table"));
        assertTrue(actuator.validate(new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(2_000_000L, 5L), GarbageCollectActuator.NAME, ActionScope.NODE,
                                                Subject.node(), Map.of(), SPEC), view).isPresent());
        assertTrue(actuator.validate(new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(2_000_001L, 5L), GarbageCollectActuator.NAME, ActionScope.NODE,
                                                Subject.table("ks", "t"), Map.of(GarbageCollectActuator.PARAM_OPTION, "BOGUS"), SPEC), view).isPresent());

        actuator.apply(action("ks", "t"), context());
        assertTrue(actuator.validate(action("ks", "t"), view).get().contains("within the last day"));
        assertFalse(actuator.validate(action("ks", "other"), view).isPresent());
        now.addAndGet(25 * HOUR);
        assertFalse(actuator.validate(action("ks", "t"), view).isPresent());
    }

    /** Rule 10: only at CRITICAL, and once per target per day. */
    @Test
    public void governorAdmitsItOnlyAtCriticalAndOncePerTargetPerDay()
    {
        AutonomicConfig config = new AutonomicConfig();
        config.dry_run = false;
        config.safety.max_actions_per_hour = 100;
        config.safety.max_actions_per_actuator_per_hour = 100;
        DefaultSafetyGovernor governor = new DefaultSafetyGovernor(new DefaultSafetyGovernor.Environment()
        {
            public AutonomicConfig config() { return config; }
            public long nowMillis() { return now.get(); }
            public String layerGate() { return null; }
            public Actuator actuator(String name) { return actuator; }
            public boolean actuatorEnabled(String name) { return true; }
            public boolean nodeReady() { return true; }
            public boolean storeAvailable() { return true; }

            public ClusterGate clusterGate()
            {
                return new ClusterGate()
                {
                    public boolean enabled() { return false; }
                    public Optional<String> nodeState(Action a) { return Optional.empty(); }
                    public Optional<String> concurrency(Action a) { return Optional.empty(); }
                };
            }
        }, new DefaultSafetyGovernor.Journal()
        {
            public void putState(String key, String value) {}
            public void deleteState(String key) {}
            public void quarantined(String policy, long untilMillis) {}
            public void breakerTripped(int rollbacks, int window) {}
        });
        UUID warn = UUID.randomUUID();
        UUID critical = UUID.randomUUID();
        view.setActiveDiagnoses(List.of(new Diagnosis(warn, "tombstone_overload", Subject.table("ks", "t"), Severity.WARN, 1, 0, 0, "", Map.of(), null),
                                        new Diagnosis(critical, "tombstone_overload", Subject.table("ks", "t"), Severity.CRITICAL, 1, 0, 0, "", Map.of(), null)));

        Action a = action("ks", "t");
        Decision below = governor.evaluate(proposal(a, warn), view);
        assertEquals(Reasons.VALIDATION, below.reason);
        assertTrue(below.detail.contains("CRITICAL"));
        assertEquals(Decision.approve(), governor.evaluate(proposal(a, critical), view));
        governor.recordOutcome(a, new ActionOutcome(ActionState.COMMITTED, null, "irreversible", Map.of(), Map.of(), now.get(), now.get()));

        now.addAndGet(23 * HOUR);
        assertEquals(Reasons.COOLDOWN, governor.evaluate(proposal(action("ks", "t"), critical), view).reason);
        // another target is not affected
        Action other = action("ks", "u");
        assertEquals(Decision.approve(), governor.evaluate(proposal(other, critical), view));
        governor.recordOutcome(other, new ActionOutcome(ActionState.REJECTED, null, "x", Map.of(), Map.of(), now.get(), now.get()));
        now.addAndGet(2 * HOUR);
        assertEquals(Decision.approve(), governor.evaluate(proposal(action("ks", "t"), critical), view));
    }

    private ActionRecord proposal(Action a, UUID diagnosis)
    {
        return new ActionRecord(a, ActionState.PROPOSED, now.get(), diagnosis, "policy", false, null, Map.of(), Map.of(), Map.of(), null, now.get());
    }
}
