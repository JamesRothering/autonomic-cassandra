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
package org.apache.cassandra.autonomic.plan.policies;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.AdvertisedSeverityActuator;
import org.apache.cassandra.autonomic.execute.actuators.CacheCapacityActuator;
import org.apache.cassandra.autonomic.execute.actuators.CompactionThroughputActuator;
import org.apache.cassandra.autonomic.execute.actuators.ConcurrentCompactorsActuator;
import org.apache.cassandra.autonomic.execute.actuators.FakeHandle;
import org.apache.cassandra.autonomic.execute.actuators.GarbageCollectActuator;
import org.apache.cassandra.autonomic.execute.actuators.HintThrottleActuator;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator;
import org.apache.cassandra.autonomic.execute.actuators.StreamThroughputActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Each policy returns the expected action and verify spec, and nothing when its knobs are at their limits. */
public class PoliciesTest
{
    private static final Subject NODE = Subject.node();
    private static final Subject TABLE = Subject.table("ks", "t");
    private static final Windows WINDOWS = new Windows(120_000, 300_000);

    private final Map<String, FakeHandle> handles = new HashMap<>();
    private final Map<String, Actuator> actuators = new HashMap<>();
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> 1_000_000L);
    private final CompactionBacklogPolicy backlog = new CompactionBacklogPolicy(actuators::get, name -> WINDOWS);
    private final TombstoneOverloadPolicy tombstones = new TombstoneOverloadPolicy(actuators::get, name -> WINDOWS);
    private final GcPressurePolicy gc = new GcPressurePolicy(actuators::get, name -> WINDOWS);
    private final DegradedSelfPolicy degraded = new DegradedSelfPolicy(actuators::get, name -> WINDOWS);
    private long ids;

    public PoliciesTest()
    {
        knob(CompactionThroughputActuator.NAME, 64, new CompactionThroughputActuator(handle(CompactionThroughputActuator.NAME, 64), Map.of()));
        knob(ConcurrentCompactorsActuator.NAME, 2, new ConcurrentCompactorsActuator(handle(ConcurrentCompactorsActuator.NAME, 2), 4));
        knob(StreamThroughputActuator.NAME, 100, new StreamThroughputActuator(handle(StreamThroughputActuator.NAME, 100), Map.of()));
        knob(HintThrottleActuator.NAME, 1024, new HintThrottleActuator(handle(HintThrottleActuator.NAME, 1024), Map.of()));
        knob(CacheCapacityActuator.ROW, 100, new CacheCapacityActuator(CacheCapacityActuator.ROW, handle(CacheCapacityActuator.ROW, 100), Map.of()));
        knob(CacheCapacityActuator.COUNTER, 0, new CacheCapacityActuator(CacheCapacityActuator.COUNTER, handle(CacheCapacityActuator.COUNTER, 0), Map.of()));
        knob(CacheCapacityActuator.KEY, 200, new CacheCapacityActuator(CacheCapacityActuator.KEY, handle(CacheCapacityActuator.KEY, 200), Map.of()));
        knob(AdvertisedSeverityActuator.NAME, 0, new AdvertisedSeverityActuator(handle(AdvertisedSeverityActuator.NAME, 0)));
        actuators.put(GarbageCollectActuator.NAME, new GarbageCollectActuator(new GarbageCollectActuator.Compactor()
        {
            public boolean tableExists(String keyspace, String table) { return true; }
            public void garbageCollect(String keyspace, String table, String option) {}
        }, Runnable::run));
    }

    private FakeHandle handle(String name, double value)
    {
        FakeHandle h = new FakeHandle(value);
        handles.put(name, h);
        return h;
    }

    private void knob(String name, double value, KnobActuator actuator)
    {
        actuators.put(name, actuator);
    }

    private Diagnosis diagnosis(String type, Subject subject, Severity severity, Map<String, String> evidence)
    {
        return new Diagnosis(TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + ids++, 3L), type, subject, severity, 1, 0, 0, "", evidence, null);
    }

    private Diagnosis diagnosis(String type, Subject subject, Severity severity)
    {
        return diagnosis(type, subject, severity, Map.of());
    }

    private static Action only(Optional<Plan> plan, Diagnosis d, String policy)
    {
        assertTrue("expected a plan", plan.isPresent());
        assertEquals(d.id, plan.get().diagnosisId);
        assertEquals(policy, plan.get().policy);
        assertEquals(1, plan.get().actions.size());
        Action a = plan.get().actions.get(0);
        assertEquals(ActionScope.NODE, a.scope);
        assertEquals(d.id.toString(), a.params.get(KnobActuator.PARAM_DIAGNOSIS));
        assertTrue(a.id.version() == 1 && plan.get().id.version() == 1);
        return a;
    }

    private void atLimit(String name, double value)
    {
        handles.get(name).value = value;
    }

    private static void assertSpec(VerifySpec spec, String goal, Subject subject, String... guards)
    {
        assertEquals(goal, spec.goal.name);
        assertEquals(subject, spec.goalSubject);
        assertTrue(spec.lowerIsBetter);
        assertEquals(0.10, spec.minImprovement, 0);
        assertEquals(120_000, spec.baselineWindowMillis);
        assertEquals(300_000, spec.observationWindowMillis);
        assertEquals(guards.length, spec.guards.size());
        for (int i = 0; i < guards.length; i++)
        {
            assertEquals(guards[i], spec.guards.get(i).signal.name);
            assertTrue(spec.guards.get(i).lowerIsBetter);
            assertEquals(0.25, spec.guards.get(i).tolerance, 0);
        }
    }

    @Test
    public void compactionBacklogRaisesThroughputThenCompactors()
    {
        Diagnosis d = diagnosis("compaction_backlog", NODE, Severity.WARN);
        Action a = only(backlog.plan(d, view), d, "compaction_backlog");
        assertEquals("compaction_throughput", a.actuator);
        assertEquals(NODE, a.target);
        assertEquals("77", a.params.get("value"));
        assertEquals("compaction_throughput:NODE", a.dedupeKey);
        assertSpec(a.verify, "compaction.pending_tasks", NODE, "client.read_latency_p99_micros", "client.write_latency_p99_micros");

        atLimit("compaction_throughput", 256);
        Action b = only(backlog.plan(d, view), d, "compaction_backlog");
        assertEquals("concurrent_compactors", b.actuator);
        assertEquals("3", b.params.get("value"));

        atLimit("concurrent_compactors", 4);
        assertFalse(backlog.plan(d, view).isPresent());
    }

    @Test
    public void compactionBacklogWaitsForAHealthyForeground()
    {
        view.setActiveDiagnoses(List.of(diagnosis("latency_slo_breach", NODE, Severity.WARN)));
        assertFalse(backlog.plan(diagnosis("compaction_backlog", NODE, Severity.WARN), view).isPresent());
    }

    @Test
    public void aRolledBackIncreaseIsNotRepeatedForTheSameDiagnosis() throws Exception
    {
        Diagnosis d = diagnosis("compaction_backlog", NODE, Severity.WARN);
        Action a = only(backlog.plan(d, view), d, "compaction_backlog");
        KnobActuator knob = (KnobActuator) actuators.get("compaction_throughput");
        ActionContext ctx = new ActionContext()
        {
            public long stepDeadlineMillis() { return Long.MAX_VALUE; }
            public long nowMillis() { return 0; }
            public boolean killed() { return false; }
            public void recordUndo(Map<String, String> undo) {}
        };
        knob.rollback(a, knob.apply(a, ctx), ctx);
        // the next knob is tried; the throughput only for a new diagnosis
        assertEquals("concurrent_compactors", only(backlog.plan(d, view), d, "compaction_backlog").actuator);
        Diagnosis later = diagnosis("compaction_backlog", NODE, Severity.WARN);
        assertEquals("compaction_throughput", only(backlog.plan(later, view), later, "compaction_backlog").actuator);
    }

    @Test
    public void anOperatorsValueIsLeftAlone() throws Exception
    {
        Diagnosis d = diagnosis("compaction_backlog", NODE, Severity.WARN);
        Action a = only(backlog.plan(d, view), d, "compaction_backlog");
        ((KnobActuator) actuators.get("compaction_throughput")).apply(a, new ActionContext()
        {
            public long stepDeadlineMillis() { return Long.MAX_VALUE; }
            public long nowMillis() { return 0; }
            public boolean killed() { return false; }
            public void recordUndo(Map<String, String> undo) {}
        });
        atLimit("compaction_throughput", 90);
        assertEquals("concurrent_compactors", only(backlog.plan(d, view), d, "compaction_backlog").actuator);
    }

    @Test
    public void tombstoneOverloadWarnRaisesCompactionThroughput()
    {
        Diagnosis d = diagnosis("tombstone_overload", TABLE, Severity.WARN);
        Action a = only(tombstones.plan(d, view), d, "tombstone_overload");
        assertEquals("compaction_throughput", a.actuator);
        assertEquals("77", a.params.get("value"));
        assertSpec(a.verify, "table.tombstones_scanned_p99", TABLE, "table.read_latency_p99_micros");
        assertEquals(TABLE, a.verify.guards.get(0).subject);

        atLimit("compaction_throughput", 256);
        assertFalse(tombstones.plan(d, view).isPresent());
    }

    @Test
    public void tombstoneOverloadCriticalSelectsTheGarbageCollectingCompactionOnce()
    {
        Diagnosis d = diagnosis("tombstone_overload", TABLE, Severity.CRITICAL);
        Action a = only(tombstones.plan(d, view), d, "tombstone_overload");
        assertEquals("garbage_collect", a.actuator);
        assertEquals(TABLE, a.target);
        assertEquals("ROW", a.params.get("tombstone_option"));
        assertEquals("garbage_collect:TABLE:ks.t", a.dedupeKey);
        assertSpec(a.verify, "table.tombstones_scanned_p99", TABLE, "table.read_latency_p99_micros");

        // one already ran on this table within the day: nothing; another table is unaffected
        view.setActionSource(lookback -> List.of(new ActionRecord(a, ActionState.COMMITTED, 999_000L)));
        assertFalse(tombstones.plan(d, view).isPresent());
        Diagnosis other = diagnosis("tombstone_overload", Subject.table("ks", "u"), Severity.CRITICAL);
        assertEquals("garbage_collect", only(tombstones.plan(other, view), other, "tombstone_overload").actuator);
        // a rejected proposal does not count
        view.setActionSource(lookback -> List.of(new ActionRecord(a, ActionState.REJECTED, 999_000L)));
        assertTrue(tombstones.plan(d, view).isPresent());
    }

    @Test
    public void tombstoneOverloadIgnoresNonTableSubjects()
    {
        assertFalse(tombstones.plan(diagnosis("tombstone_overload", NODE, Severity.CRITICAL), view).isPresent());
    }

    @Test
    public void gcPressureShrinksCachesInOrderAndSkipsUnmanagedOnes()
    {
        Diagnosis d = diagnosis("gc_pressure", NODE, Severity.WARN);
        Action a = only(gc.plan(d, view), d, "gc_pressure");
        assertEquals("row_cache_capacity", a.actuator);
        assertEquals("80", a.params.get("value"));
        assertSpec(a.verify, "gc.time_fraction", NODE, "client.read_latency_p99_micros");

        // the counter cache is configured to 0, so it is skipped
        atLimit("row_cache_capacity", 50);
        Action b = only(gc.plan(d, view), d, "gc_pressure");
        assertEquals("key_cache_capacity", b.actuator);
        assertEquals("160", b.params.get("value"));

        atLimit("key_cache_capacity", 100);
        assertFalse(gc.plan(d, view).isPresent());
    }

    @Test
    public void degradedSelfYieldsBackgroundWorkThenAdvertisesSeverity()
    {
        Diagnosis d = diagnosis("degraded_self", NODE, Severity.WARN,
                                Map.of("client.read_latency_p99_micros", "60000 us (slo 50000 us)", "client.write_latency_p99_micros", "10000 us (slo 20000 us)"));
        Action a = only(degraded.plan(d, view), d, "degraded_self");
        assertEquals("compaction_throughput", a.actuator);
        assertEquals("51", a.params.get("value"));
        assertSpec(a.verify, "client.read_latency_p99_micros", NODE, "client.write_latency_p99_micros");

        atLimit("compaction_throughput", 16);
        assertEquals("stream_throughput", only(degraded.plan(d, view), d, "degraded_self").actuator);
        atLimit("stream_throughput", 25);
        assertEquals("hint_throttle", only(degraded.plan(d, view), d, "degraded_self").actuator);
        atLimit("hint_throttle", 256);
        Action sev = only(degraded.plan(d, view), d, "degraded_self");
        assertEquals("advertised_severity", sev.actuator);
        assertEquals("0.1", sev.params.get("value"));

        atLimit("advertised_severity", 1.0);
        assertFalse(degraded.plan(d, view).isPresent());
    }

    @Test
    public void degradedSelfTargetsTheWorseOperation()
    {
        Diagnosis d = diagnosis("degraded_self", NODE, Severity.WARN,
                                Map.of("client.read_latency_p99_micros", "10000 us (slo 50000 us)", "client.write_latency_p99_micros", "40000 us (slo 20000 us)"));
        VerifySpec spec = only(degraded.plan(d, view), d, "degraded_self").verify;
        assertSpec(spec, "client.write_latency_p99_micros", NODE, "client.read_latency_p99_micros");
        // no evidence: read
        Diagnosis bare = diagnosis("degraded_self", NODE, Severity.WARN);
        assertEquals("client.read_latency_p99_micros", only(degraded.plan(bare, view), bare, "degraded_self").verify.goal.name);
    }

    @Test
    public void everyActionPassesItsActuatorsValidationAndBlastRadius()
    {
        for (Policy p : List.of(backlog, gc, degraded, tombstones))
        {
            String type = p.handles().iterator().next();
            Diagnosis d = diagnosis(type, type.equals("tombstone_overload") ? TABLE : NODE, Severity.WARN);
            Action a = only(p.plan(d, view), d, p.name());
            Actuator actuator = actuators.get(a.actuator);
            assertEquals(Optional.empty(), actuator.validate(a, view));
            assertEquals(Optional.empty(), actuator.blastRadius(a, view, 0.25));
        }
    }

    @Test
    public void buildCreatesThePoliciesWithConfiguredWindows()
    {
        AutonomicConfig config = new AutonomicConfig();
        config.actuators.get("compaction_throughput").put("baseline_window", "30s");
        config.actuators.get("compaction_throughput").put("observation_window", 90_000);
        List<Policy> built = Policies.build(config, actuators::get);
        assertEquals(List.of("compaction_backlog", "tombstone_overload", "gc_pressure", "degraded_self", "repair_overdue"),
                     built.stream().map(Policy::name).collect(java.util.stream.Collectors.toList()));
        Diagnosis d = diagnosis("compaction_backlog", NODE, Severity.WARN);
        VerifySpec spec = only(built.get(0).plan(d, view), d, "compaction_backlog").verify;
        assertEquals(30_000, spec.baselineWindowMillis);
        assertEquals(90_000, spec.observationWindowMillis);
        // other actuators keep the defaults
        Diagnosis g = diagnosis("gc_pressure", NODE, Severity.WARN);
        assertEquals(Windows.DEFAULT_OBSERVATION_MILLIS, only(built.get(2).plan(g, view), g, "gc_pressure").verify.observationWindowMillis);
    }

    @Test
    public void repairOverdueAsksForTheRepairPriorityOfTheHostOnlyWhileAutoRepairIsEnabled()
    {
        RepairOverduePolicy policy = new RepairOverduePolicy(name -> WINDOWS);
        Subject host = Subject.peer("00000000-0000-0000-0000-00000000000a");
        Diagnosis d = diagnosis("repair_overdue", host, Severity.CRITICAL, Map.of("repair_type", "FULL", "auto_repair_enabled", "true"));
        Plan plan = policy.plan(d, view).get();
        assertEquals(d.id, plan.diagnosisId);
        Action a = plan.actions.get(0);
        assertEquals("repair_priority", a.actuator);
        assertEquals(ActionScope.CLUSTER, a.scope);
        assertEquals(host, a.target);
        assertEquals(Map.of("repair_type", "FULL", "host_id", host.detail), a.params);
        assertEquals("repair.overdue_ratio", a.verify.goal.name);
        assertTrue(a.id.version() == 1);

        Diagnosis off = diagnosis("repair_overdue", host, Severity.INFO, Map.of("repair_type", "FULL", "auto_repair_enabled", "false"));
        assertFalse(policy.plan(off, view).isPresent());
    }
}
