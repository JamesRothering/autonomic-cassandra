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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.FakeCluster;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.actuators.PartitionSplitActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutAnnotations;
import org.apache.cassandra.autonomic.layout.LayoutHeader;
import org.apache.cassandra.autonomic.layout.LayoutProfiler;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.LayoutRegistryValue;
import org.apache.cassandra.autonomic.layout.PartitionProbe;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.SplitEvaluation;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.PartitionKeys;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Design 7.2 and 7.4: one fixture per reason the policy does not propose a split, each with the note the document
 * names ({@code layout=not_applicable(<reason>)}); the proposal; the choice of the number of buckets. The reasons that
 * belong to the governor (actuator disabled, {@code dry_run}, cluster actions not enabled, approval not given) are
 * covered by its tests: the policy proposes and the governor decides.
 */
public class PartitionSplitPolicyTest
{
    private static final String KS = "ks";
    private static final String TABLE = "t";
    private static final TableId LOGICAL = TableId.fromUUID(new UUID(1, 1));
    private static final ByteBuffer KEY = Int32Type.instance.decompose(7);
    private static final String KEY_HEX = ByteBufferUtil.bytesToHex(KEY);
    private static final long WINDOW = 60_000;

    private final AutonomicConfig config = new AutonomicConfig();
    private final long[] now = { 10_000_000 };
    private final ObservationStore store = new ObservationStore(16, 64);
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, () -> now[0]);
    private final List<ActionRecord> records = new ArrayList<>();
    private final List<Diagnosis> active = new ArrayList<>();
    private ClusterLedger ledger = ClusterLedger.EMPTY;
    private final Env env = new Env();
    private PartitionSplitPolicy policy;
    private FakeCluster cluster;
    private int gcGrace = 864000;
    private int rf = 2;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /** The node: metadata, the proposer, liveness, the probe and the profile are under the test's control. */
    private final class Env implements PartitionSplitPolicy.Env
    {
        ClusterMetadata metadata;
        boolean preferred = true;
        final List<NodeId> notAlive = new ArrayList<>();
        final List<AsyncPromise<PartitionProbe.Result>> probes = new ArrayList<>();
        LayoutProfiler.Shape shape;
        long paxos;
        boolean watching;
        int probesStarted;

        @Override
        public ClusterMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean preferredProposer()
        {
            return preferred;
        }

        @Override
        public Collection<NodeId> membersNotAlive()
        {
            return notAlive;
        }

        @Override
        public Future<PartitionProbe.Result> probe(TableMetadata table, ByteBuffer key, long maxRows)
        {
            probesStarted++;
            AsyncPromise<PartitionProbe.Result> f = new AsyncPromise<>();
            probes.add(f);
            return f;
        }

        @Override
        public LayoutProfiler.Shape profile(TableMetadata table, ByteBuffer key)
        {
            return watching ? shape : null;
        }

        @Override
        public void watch(TableMetadata table, ByteBuffer key, long windowMillis)
        {
            watching = true;
            assertTrue("the watch outlives the profile window", windowMillis >= config.layout.profile_window.toMilliseconds());
        }

        @Override
        public void unwatch(TableMetadata table, ByteBuffer key)
        {
            watching = false;
        }

        @Override
        public long paxosOperations(TableMetadata table)
        {
            return paxos;
        }

        @Override
        public long writeTimeoutMillis()
        {
            return 2000;
        }
    }

    @Before
    public void setUp()
    {
        LayoutAnnotations.clearAll();
        config.layout.for_evaluations = 2;
        config.layout.profile_window = new org.apache.cassandra.config.DurationSpec.IntMillisecondsBound("1m");
        config.layout.min_samples = 100;
        view.setActionSource(ms -> records);
        view.setLedgerSource(() -> ledger);
        view.setActiveDiagnoses(active);
        cluster = new FakeCluster(3);
        rebuildSchema();
        env.shape = shape(0, 900, 100, 0, 0, 0);
        policy = new PartitionSplitPolicy(config, env);
    }

    @After
    public void tearDown()
    {
        LayoutAnnotations.clearAll();
    }

    // ---- fixtures ----

    private static TableMetadata.Builder table(String name)
    {
        return TableMetadata.builder(KS, name)
                            .addPartitionKeyColumn("p", Int32Type.instance)
                            .addClusteringColumn("c", Int32Type.instance)
                            .addRegularColumn("v", UTF8Type.instance);
    }

    private void rebuildSchema(TableMetadata... extra)
    {
        TableMetadata t = table(TABLE).id(LOGICAL).params(TableParams.builder().gcGraceSeconds(gcGrace).build()).build();
        Tables tables = Tables.of(t);
        for (TableMetadata e : extra)
            tables = tables.with(e);
        ClusterMetadata md = cluster.log.transformer().with(new DistributedSchema(Keyspaces.of(KeyspaceMetadata.create(KS, KeyspaceParams.simple(rf), tables)))).build().metadata;
        env.metadata = md;
    }

    private void withTable(TableMetadata.Builder builder)
    {
        env.metadata = cluster.log.transformer().with(new DistributedSchema(Keyspaces.of(
            KeyspaceMetadata.create(KS, KeyspaceParams.simple(rf), Tables.of(builder.id(LOGICAL).build()))))).build().metadata;
    }

    private static LayoutProfiler.Shape shape(long named, long slices, long rowWrites, long deletes, long cas, long scans)
    {
        return new LayoutProfiler.Shape(named, slices, rowWrites, deletes, cas, scans, 0, 0, 0, 0, 0, 0);
    }

    /** A profile with {@code p} of its 1000 operations touching a single bucket. */
    private static LayoutProfiler.Shape profileWithPointFraction(double p)
    {
        long point = Math.round(1000 * p);
        return shape(point / 2, 1000 - point, point - point / 2, 0, 0, 0);
    }

    private static Diagnosis hot(Map<String, String> evidence)
    {
        return new Diagnosis(UUID.randomUUID(), "hot_partition", Subject.partition(KS, TABLE, KEY_HEX), Severity.WARN, 1, 0, 0, "hot", evidence, null);
    }

    private static Diagnosis hot()
    {
        return hot(Map.of("key_hex", KEY_HEX, "table", KS + '.' + TABLE));
    }

    private void put(SignalId signal, Subject subject, double value)
    {
        store.accept(new Observation(signal, subject, now[0], value));
    }

    private Optional<Plan> tick(Diagnosis d)
    {
        now[0] += 5_000;
        return policy.plan(d, view);
    }

    private static Subject subject()
    {
        return Subject.partition(KS, TABLE, KEY_HEX);
    }

    private String note()
    {
        return LayoutAnnotations.get(subject());
    }

    private void assertNoPlan(String reason, Diagnosis d)
    {
        Optional<Plan> plan = tick(d);
        assertEquals(Optional.empty(), plan);
        assertEquals("layout=" + LayoutAnnotations.notApplicable(reason), "layout=" + note());
    }

    /** Runs the evaluations that every reason before the probe waits for. */
    private Diagnosis open()
    {
        Diagnosis d = hot();
        assertEquals("not looked at before for_evaluations", Optional.empty(), tick(d));
        assertNull(note());
        return d;
    }

    private static PartitionProbe.Result rows(long rows)
    {
        return new PartitionProbe.Result(rows, false, 100);
    }

    /** Through the probe and the profile to the first decision. */
    private Optional<Plan> qualify(Diagnosis d, PartitionProbe.Result probe, LayoutProfiler.Shape profile)
    {
        assertEquals(Optional.empty(), tick(d));
        assertEquals("the probe started", 1, env.probesStarted);
        env.probes.get(0).setSuccess(probe);
        if (probe.rows < config.layout.min_rows_for_split || probe.hitMax)
            return tick(d);
        assertEquals(Optional.empty(), tick(d));
        assertEquals("profiling", note());
        assertTrue(env.watching);
        assertEquals("the window has not passed", Optional.empty(), tick(d));
        now[0] += WINDOW;
        env.shape = profile;
        return tick(d);
    }

    // ---- the way in ----

    @Test
    public void handlesHotPartition()
    {
        assertEquals(java.util.Set.of("hot_partition", "layout.orphaned", "layout.reclaim_due"), policy.handles());
        assertEquals("partition_split", policy.name());
    }

    private static Diagnosis orphaned(String phase, boolean pastLimit)
    {
        return new Diagnosis(UUID.randomUUID(), "layout.orphaned", Subject.partition(KS, TABLE, KEY_HEX), Severity.CRITICAL, 1, 0, 0, "orphaned",
                             Map.of("key_hex", KEY_HEX, "table", KS + '.' + TABLE, "phase", phase, "past_tombstone_deadline", Boolean.toString(pastLimit)),
                             null);
    }

    /** T6.9 review: an orphaned key past its commit point is adopted at once by the preferred proposer, through the governor. */
    @Test
    public void anOrphanedKeyIsAdoptedByThePreferredProposer()
    {
        Diagnosis d = orphaned("CLEANUP", false);
        Optional<Plan> plan = policy.plan(d, view);
        assertTrue(plan.isPresent());
        Action a = plan.get().actions.get(0);
        assertEquals(PartitionSplitActuator.NAME, a.actuator);
        assertEquals(ActionScope.CLUSTER, a.scope);
        assertEquals(PartitionSplitActuator.target(KS, TABLE, KEY_HEX), a.target);
        assertEquals(PartitionSplitActuator.MODE_RESUME, a.params.get(PartitionSplitActuator.PARAM_MODE));
        assertEquals(d.id, plan.get().diagnosisId);
        assertTrue(plan.get().rationale, plan.get().rationale.contains("past its commit point in CLEANUP"));

        // not off the preferred proposer, not with a member down, not while the lane is claimed, not past the limit
        env.preferred = false;
        assertEquals(Optional.empty(), policy.plan(d, view));
        env.preferred = true;
        env.notAlive.add(new NodeId(3));
        assertEquals(Optional.empty(), policy.plan(d, view));
        env.notAlive.clear();
        assertEquals(Optional.empty(), policy.plan(orphaned("CLEANUP", true), view));
        ledger = new ClusterLedger(ClusterLedger.FORMAT_VERSION, true,
                                   Map.of(Lane.SCHEMA_LAYOUT, new Claim(UUID.randomUUID(), "partition_split", "x", new NodeId(1), "CLAIMED", 0, 0, 1, Map.of())),
                                   List.of());
        assertEquals(Optional.empty(), policy.plan(d, view));
        ledger = ClusterLedger.EMPTY;
        assertTrue(policy.plan(orphaned("STEADY", false), view).isPresent());
    }

    @Test
    public void doesNothingBeforeForEvaluationsOrOffThePreferredProposer()
    {
        Diagnosis d = hot();
        assertEquals(Optional.empty(), tick(d));
        env.preferred = false;
        for (int i = 0; i < 5; i++)
            assertEquals(Optional.empty(), tick(d));
        assertNull(note());
        assertEquals(0, env.probesStarted);
    }

    @Test
    public void theKeyComesFromTheEvidenceNotFromTheSubject()
    {
        // the subject says 7; the evidence says 8: the plan is for 8
        Diagnosis d = hot(Map.of("key_hex", ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(8))));
        Plan plan = decide(d, rows(20_000), profileWithPointFraction(0.95)).get();
        assertEquals(ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(8)), plan.actions.get(0).target.detail);
        assertEquals("the plan is for the key of the evidence", ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(8)),
                     PartitionKeys.hex(plan.actions.get(0).target));
    }

    @Test
    public void aDiagnosisWithoutAUsableKeyIsNotApplicable()
    {
        Diagnosis d = hot(Map.of("table", KS + '.' + TABLE));
        tick(d);
        assertEquals(Optional.empty(), tick(d));
        assertEquals("not_applicable(invalid_key)", note());
        d = hot(Map.of("key_hex", "zz"));
        assertEquals(Optional.empty(), tick(d));
        assertEquals("not_applicable(invalid_key)", note());
    }

    private Optional<Plan> decide(Diagnosis d, PartitionProbe.Result probe, LayoutProfiler.Shape profile)
    {
        tick(d);
        return qualify(d, probe, profile);
    }

    // ---- the proposal ----

    @Test
    public void aPendingUnsplitTakesTheLaneBeforeANewSplit()
    {
        put(Signals.PARTITION_READ_SHARE, subject(), 0.5);
        active.add(new Diagnosis(UUID.randomUUID(), "layout.unsplit_pending", Subject.partition(KS, "other", "00"), Severity.INFO, 1, 0, 0, "pending",
                                 Map.of("key_hex", "00", "due_millis", "1"), null));
        view.setActiveDiagnoses(active);
        Diagnosis d = hot();
        for (int i = 0; i < 4; i++)
            assertEquals(Optional.empty(), tick(d));
        assertEquals("nothing is looked at while the unsplit waits", 0, env.probesStarted);
        active.clear();
        view.setActiveDiagnoses(active);
        assertTrue(decide(d, rows(20_000), profileWithPointFraction(0.95)).isPresent());
    }

    @Test
    public void proposesASplitWithTheProfileAndTheEstimates()
    {
        put(Signals.PARTITION_READ_SHARE, subject(), 0.5);
        Diagnosis d = hot();
        Plan plan = decide(d, rows(20_000), profileWithPointFraction(0.95)).get();
        assertEquals(d.id, plan.diagnosisId);
        assertEquals("partition_split", plan.policy);
        assertEquals(1, plan.actions.size());
        Action a = plan.actions.get(0);
        assertEquals(PartitionSplitActuator.NAME, a.actuator);
        assertEquals(ActionScope.CLUSTER, a.scope);
        assertEquals(PartitionSplitActuator.target(KS, TABLE, KEY_HEX), a.target);
        assertEquals("split", a.params.get("mode"));
        assertEquals("share 0.5 / target 0.05 = 10 -> 16", "16", a.params.get("buckets"));
        SplitEvaluation.Baseline baseline = SplitEvaluation.Baseline.fromMap(a.params);
        assertEquals(0.5, baseline.opsShare, 0);
        assertFalse(baseline.sizeOnly);
        assertTrue(plan.rationale, plan.rationale.contains("16 buckets") && plan.rationale.contains("20000 rows") && plan.rationale.contains("p=0.95"));
        assertEquals("proposed(buckets=16)", note());
        assertFalse("the watch of the profile is released", env.watching);
        // the governor's and the operator's path: the actuator's own validation
        assertEquals(1, a.id.version());
    }

    @Test
    public void bucketsAreChosenFromTheShareClampedToAPowerOfTwoBetweenFourAndThirtyTwo()
    {
        // clamp(nextPowerOfTwo(ceil(share / target_share)), 4, 32), default target 0.05; values away from the boundaries
        double[][] table = {
            { 0.03, 4 }, { 0.10, 4 }, { 0.19, 4 }, { 0.21, 8 }, { 0.30, 8 }, { 0.39, 8 }, { 0.41, 16 }, { 0.5, 16 }, { 0.79, 16 }, { 0.81, 32 }, { 1.0, 32 },
        };
        for (double[] row : table)
            assertEquals("share " + row[0], (int) row[1], PartitionSplitPolicy.buckets(row[0], 0.05, 8));
        assertEquals("no measured share: the configured default", 8, PartitionSplitPolicy.buckets(Double.NaN, 0.05, 8));
        assertEquals(32, PartitionSplitPolicy.buckets(0.99, 0.001, 8));
        assertEquals(4, PartitionSplitPolicy.buckets(0.001, 0.5, 8));
    }

    @Test
    public void bucketsFollowTargetShareFromTheConfiguration()
    {
        config.layout.target_share = 0.25;
        put(Signals.PARTITION_WRITE_SHARE, subject(), 0.9);
        Plan plan = decide(hot(), rows(20_000), profileWithPointFraction(0.95)).get();
        assertEquals("ceil(0.9 / 0.25) = 4", "4", plan.actions.get(0).params.get("buckets"));
    }

    @Test
    public void aTableThatAlreadyHasALayoutKeepsItsBucketCount()
    {
        LayoutHeader header = new LayoutHeader(new UUID(9, 9), LOGICAL, TableId.fromUUID(new UUID(2, 2)), KS, TABLE, "t_overflow",
                                               TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, 8, 1);
        useRegistry(new TableLayout(header, 2, Map.of(Int32Type.instance.decompose(99), state(Phase.STEADY)), 3));
        put(Signals.PARTITION_READ_SHARE, subject(), 0.95);
        Plan plan = decide(hot(), rows(20_000), profileWithPointFraction(0.95)).get();
        assertEquals("8", plan.actions.get(0).params.get("buckets"));
    }

    // ---- 7.4: each reason, no plan, the note ----

    @Test
    public void smallHotPartitionShapeC()
    {
        Diagnosis d = open();
        assertEquals(Optional.empty(), tick(d));
        env.probes.get(0).setSuccess(rows(9_999));
        assertNoPlan("small", d);
        assertEquals("the answer is kept: no second probe", 1, env.probesStarted);
        assertNoPlan("small", d);
        assertEquals(1, env.probesStarted);
    }

    @Test
    public void fanOutAboveTwentyPercentShapeG()
    {
        Diagnosis d = open();
        assertEquals(Optional.empty(), qualify(d, rows(20_000), profileWithPointFraction(0.79)));
        assertEquals("not_applicable(fan_out)", note());
    }

    @Test
    public void theFanOutBoundaryIsInclusive()
    {
        Diagnosis d = open();
        assertTrue(qualify(d, rows(20_000), profileWithPointFraction(0.80)).isPresent());
    }

    @Test
    public void anyLwtActivityInTheProfile()
    {
        Diagnosis d = open();
        assertEquals(Optional.empty(), qualify(d, rows(20_000), shape(0, 50, 900, 0, 1, 0)));
        assertEquals("not_applicable(lwt)", note());
    }

    @Test
    public void anyLwtActivityOnTheTable()
    {
        env.paxos = 4;
        assertNoPlan("lwt", open());
        assertEquals("nothing was probed for it", 0, env.probesStarted);
    }

    @Test
    public void everyBlockingFeature()
    {
        withTable(table(TABLE).params(TableParams.builder().cdc(true).build()));
        assertNoPlan("blocked(CDC)", open());
        withTable(table(TABLE).addStaticColumn("s", Int32Type.instance));
        assertNoPlan("blocked(STATIC_COLUMN)", hot());
        withTable(table(TABLE).params(TableParams.builder().gcGraceSeconds(0).build()));
        assertNoPlan("blocked(GC_GRACE_TOO_SMALL)", hot());
        assertEquals(0, env.probesStarted);
    }

    @Test
    public void aTableInTheExclusionList()
    {
        config.layout.excluded_tables = List.of(KS + '.' + TABLE);
        assertNoPlan("excluded", open());
        assertEquals(0, env.probesStarted);
    }

    @Test
    public void anOverflowTableIsNeverSplit()
    {
        TableId other = TableId.fromUUID(new UUID(3, 3));
        LayoutHeader header = new LayoutHeader(new UUID(9, 9), other, LOGICAL, KS, "other", TABLE, TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, 8, 1);
        useRegistry(new TableLayout(header, 2, Map.of(), 3));
        assertNoPlan("overflow_table", open());
    }

    @Test
    public void aKeyThatIsAlreadyInTheLayout()
    {
        useRegistry(layout(LOGICAL, 8, Map.of(KEY, state(Phase.STEADY))));
        assertNoPlan("in_layout", open());
    }

    @Test
    public void registryBoundsReached()
    {
        List<TableLayout> full = new ArrayList<>();
        for (int i = 0; i < LayoutRegistry.MAX_LAYOUTS; i++)
            full.add(layout(TableId.fromUUID(new UUID(50, i)), 8, Map.of(Int32Type.instance.decompose(1), state(Phase.STEADY))));
        useRegistry(full.toArray(new TableLayout[0]));
        assertNoPlan("registry_full", open());

        // the table's own layout holds the most keys it can
        Map<ByteBuffer, KeyState> keys = new HashMap<>();
        for (int i = 0; i < LayoutRegistry.MAX_KEYS; i++)
            keys.put(Int32Type.instance.decompose(1000 + i), state(Phase.STEADY));
        useRegistry(layout(LOGICAL, 8, keys));
        assertNoPlan("registry_full", hot());
    }

    @Test
    public void criticalSaturationOfTheProposer()
    {
        for (String type : new String[]{ "pool_saturation", "compaction_backlog", "disk_pressure", "gc_pressure" })
        {
            active.clear();
            view.setActiveDiagnoses(active);
            LayoutAnnotations.clearAll();
            policy = new PartitionSplitPolicy(config, env);
            Diagnosis d = open();
            active.add(new Diagnosis(UUID.randomUUID(), type, Subject.node(), Severity.CRITICAL, 1, 0, 0, "", Map.of(), null));
            view.setActiveDiagnoses(active);
            assertNoPlan("saturated", d);
        }
        assertEquals(0, env.probesStarted);
        // a WARN does not stop it, nor does another diagnosis
        active.clear();
        active.add(new Diagnosis(UUID.randomUUID(), "pool_saturation", Subject.node(), Severity.WARN, 1, 0, 0, "", Map.of(), null));
        active.add(new Diagnosis(UUID.randomUUID(), "hot_partition", Subject.node(), Severity.CRITICAL, 1, 0, 0, "", Map.of(), null));
        view.setActiveDiagnoses(active);
        LayoutAnnotations.clearAll();
        policy = new PartitionSplitPolicy(config, env);
        Diagnosis d = open();
        assertEquals(Optional.empty(), tick(d));
        assertEquals("the probe started", 1, env.probesStarted);
    }

    @Test
    public void aNodeIsDown()
    {
        env.notAlive.add(cluster.id(2));
        assertNoPlan("node_down", open());
        assertEquals(0, env.probesStarted);
    }

    @Test
    public void aTopologyChangeIsInProgress()
    {
        ClusterMetadata md = env.metadata;
        env.metadata = md.transformer().with(md.inProgressSequences.with(new NodeId(77), Mockito.mock(MultiStepOperation.class))).build().metadata;
        assertNoPlan("topology", open());
        assertEquals(0, env.probesStarted);
    }

    @Test
    public void cooldownAfterAPreviousAbortOfTheSameKeyDoublesEachTime()
    {
        long base = config.safety.cooldown_after_rollback.toMilliseconds();
        Subject target = PartitionSplitActuator.target(KS, TABLE, KEY_HEX);
        record(target, ActionState.ROLLED_BACK, now[0] - 1000);
        assertNoPlan("cooldown", open());
        assertEquals(0, env.probesStarted);

        // one abort, longer ago than the cooldown: free
        records.clear();
        record(target, ActionState.ROLLED_BACK, now[0] - base - 1000);
        policy = new PartitionSplitPolicy(config, env);
        LayoutAnnotations.clearAll();
        Diagnosis d = open();
        assertEquals(Optional.empty(), tick(d));
        assertEquals("not cooling down: the probe started", 1, env.probesStarted);

        // two aborts of the same key at that time: twice the cooldown, still running
        records.clear();
        record(target, ActionState.ROLLED_BACK, now[0] - base - 1000);
        record(target, ActionState.FAILED, now[0] - base - 1000);
        policy = new PartitionSplitPolicy(config, env);
        d = open();
        assertNoPlan("cooldown", d);

        // the abort of another key is not this key's
        records.clear();
        record(PartitionSplitActuator.target(KS, TABLE, "00000008"), ActionState.ROLLED_BACK, now[0] - 1000);
        policy = new PartitionSplitPolicy(config, env);
        LayoutAnnotations.clearAll();
        d = open();
        assertEquals(Optional.empty(), tick(d));
        assertNull(note());
    }

    @Test
    public void noSplitWhereTheBucketsWouldLandOnTheSameNodes()
    {
        // three nodes, RF 3: every bucket lives on the same three nodes
        rf = 3;
        rebuildSchema();
        Diagnosis d = open();
        assertEquals(Optional.empty(), qualify(d, rows(20_000), profileWithPointFraction(0.95)));
        assertEquals("not_applicable(no_spread)", note());
    }

    @Test
    public void whereNothingSpreadsAPartitionThatIsDemonstrablyLargeIsStillSplitForItsSize()
    {
        rf = 3;
        rebuildSchema();
        Diagnosis d = open();
        // 2 million rows of 100 bytes: 200 MB
        Plan plan = qualify(d, rows(2_000_000), profileWithPointFraction(0.95)).get();
        assertTrue(plan.rationale, plan.rationale.contains("expected_benefit=partition_size_only"));
        assertTrue(SplitEvaluation.Baseline.fromMap(plan.actions.get(0).params).sizeOnly);
    }

    @Test
    public void spreadIsDecidedPerDatacenter()
    {
        ClusterMetadata md = cluster.log;
        KeyspaceMetadata two = KeyspaceMetadata.create(KS, KeyspaceParams.simple(2), Tables.none());
        KeyspaceMetadata three = KeyspaceMetadata.create(KS, KeyspaceParams.simple(3), Tables.none());
        assertTrue("3 nodes, RF 2", PartitionSplitPolicy.spreadsLoad(md, two));
        assertFalse("3 nodes, RF 3", PartitionSplitPolicy.spreadsLoad(md, three));
        ClusterMetadata four = FakeCluster.register(md, 4, org.apache.cassandra.tcm.membership.NodeVersion.CURRENT);
        assertTrue("4 nodes, RF 3", PartitionSplitPolicy.spreadsLoad(four, three));
    }

    @Test
    public void backfillThatCannotFitTheDeadline()
    {
        // 20000 rows of 100 bytes, RF 2, at 1 MiB/s is 4 seconds: it fits. A slow throttle does not.
        Diagnosis d = open();
        config.layout.backfill_throughput = new org.apache.cassandra.config.DataRateSpec.LongBytesPerSecondBound("1B/s");
        assertEquals(Optional.empty(), qualify(d, rows(20_000), profileWithPointFraction(0.95)));
        assertEquals("not_applicable(backfill_too_long)", note());
    }

    @Test
    public void notEnoughFreeDiskForTwiceTheEstimate()
    {
        put(Signals.DISK_FREE_BYTES, Subject.node(), 3_000_000);
        Diagnosis d = open();
        assertEquals("2 * 2 000 000 bytes", Optional.empty(), qualify(d, rows(20_000), profileWithPointFraction(0.95)));
        assertEquals("not_applicable(disk)", note());
        // enough
        policy = new PartitionSplitPolicy(config, env);
        env.probes.clear();
        env.probesStarted = 0;
        put(Signals.DISK_FREE_BYTES, Subject.node(), 4_000_001);
        LayoutAnnotations.clearAll();
        Diagnosis again = open();
        assertTrue(qualify(again, rows(20_000), profileWithPointFraction(0.95)).isPresent());
    }

    @Test
    public void aPartitionBeyondTheProbeLimitIsNotProposedAutomatically()
    {
        Diagnosis d = open();
        assertEquals(Optional.empty(), tick(d));
        env.probes.get(0).setSuccess(new PartitionProbe.Result(5_000_000, true, 100));
        assertNoPlan("too_large", d);
    }

    @Test
    public void aProfileWithTooFewOperationsIsNotJudged()
    {
        Diagnosis d = open();
        assertEquals(Optional.empty(), qualify(d, rows(20_000), shape(10, 10, 10, 0, 0, 0)));
        assertEquals("not_applicable(insufficient_profile)", note());
        assertFalse(env.watching);
    }

    @Test
    public void aFailedProbeIsRetriedAfterForEvaluationsEvaluationsNotAtOnce()
    {
        Diagnosis d = open();
        assertEquals(Optional.empty(), tick(d));
        env.probes.get(0).setFailure(new IllegalStateException("replica down"));
        assertNoPlan("probe_failed", d);
        assertEquals(Optional.empty(), tick(d));
        assertEquals(1, env.probesStarted);
        assertEquals(Optional.empty(), tick(d));
        assertEquals("the second probe", 2, env.probesStarted);
    }

    @Test
    public void aDroppedTable()
    {
        Diagnosis d = open();
        env.metadata = cluster.log;
        assertNoPlan("table_dropped", d);
    }

    // ---- one at a time ----

    @Test
    public void nothingIsProposedWhileAMigrationHoldsTheLane()
    {
        Claim claim = new Claim(UUID.randomUUID(), PartitionSplitActuator.NAME, "PARTITION:ks.t:00000009", new NodeId(2), "BACKFILL", 3, now[0], now[0] + 600_000,
                                ImmutableMap.of());
        ledger = ClusterLedger.EMPTY.withEnabled(true).ledger.withClaim(Lane.SCHEMA_LAYOUT, claim, now[0], 0, false).ledger;
        Diagnosis d = open();
        for (int i = 0; i < 4; i++)
            assertEquals(Optional.empty(), tick(d));
        assertEquals(0, env.probesStarted);
        assertNull("it is not a reason of 7.4", note());
    }

    @Test
    public void nothingIsProposedWhileAnActionOfThePolicyIsProposedOrRunning()
    {
        record(PartitionSplitActuator.target(KS, TABLE, "00000009"), ActionState.PROPOSED, now[0]);
        Diagnosis d = open();
        for (int i = 0; i < 4; i++)
            assertEquals(Optional.empty(), tick(d));
        assertEquals(0, env.probesStarted);
    }

    // ---- reclaim (ADR-036) ----

    private static KeyState released(long frozenSince, UUID owner, KeyState.BaseState base)
    {
        return new KeyState(Phase.STEADY, Epoch.create(10), 1000, 1, 1, Epoch.create(10), ImmutableSet.of(), owner, 500, ImmutableSet.of(),
                            base, frozenSince, 0, null, 0);
    }

    private static Diagnosis reclaimDue()
    {
        return new Diagnosis(UUID.randomUUID(), "layout.reclaim_due", Subject.partition(KS, TABLE, KEY_HEX), Severity.INFO, 1, 0, 0, "due",
                             Map.of("key_hex", KEY_HEX, "table", KS + '.' + TABLE), null);
    }

    /** The reclaim is proposed exactly when it is due: not before, not while the key is owned, not in a reverse phase. */
    @Test
    public void aReclaimIsProposedExactlyWhenItIsDueByThePreferredProposer()
    {
        config.layout.reclaim_window = new org.apache.cassandra.config.DurationSpec.LongMillisecondsBound("1h");
        long frozen = now[0];
        useRegistry(layout(LOGICAL, 8, Map.of(KEY, released(frozen, null, KeyState.BaseState.RESIDUE))));
        Diagnosis d = reclaimDue();
        now[0] = frozen + 3_600_000 - 1;
        assertEquals("not yet due", Optional.empty(), policy.plan(d, view));
        now[0] = frozen + 3_600_000;
        Optional<Plan> plan = policy.plan(d, view);
        assertTrue(plan.isPresent());
        Action a = plan.get().actions.get(0);
        assertEquals(PartitionSplitActuator.MODE_RECLAIM, a.params.get(PartitionSplitActuator.PARAM_MODE));
        assertEquals(PartitionSplitActuator.target(KS, TABLE, KEY_HEX), a.target);
        assertEquals(d.id, plan.get().diagnosisId);

        // only the preferred proposer, no member down, no claim on the lane
        env.preferred = false;
        assertEquals(Optional.empty(), policy.plan(d, view));
        env.preferred = true;
        env.notAlive.add(new NodeId(3));
        assertEquals(Optional.empty(), policy.plan(d, view));
        env.notAlive.clear();
        ledger = new ClusterLedger(ClusterLedger.FORMAT_VERSION, true,
                                   Map.of(Lane.SCHEMA_LAYOUT, new Claim(UUID.randomUUID(), "partition_split", "x", new NodeId(1), "CLAIMED", 0, 0, 1, Map.of())),
                                   List.of());
        assertEquals(Optional.empty(), policy.plan(d, view));
        ledger = ClusterLedger.EMPTY;

        // never while owned, already tombstoned, in a forward phase before STEADY, or in any reverse phase
        useRegistry(layout(LOGICAL, 8, Map.of(KEY, released(frozen, new UUID(7, 7), KeyState.BaseState.RESIDUE))));
        assertEquals("owned", Optional.empty(), policy.plan(d, view));
        useRegistry(layout(LOGICAL, 8, Map.of(KEY, released(frozen, null, KeyState.BaseState.TOMBSTONED))));
        assertEquals("tombstoned", Optional.empty(), policy.plan(d, view));
        for (Phase phase : Phase.values())
        {
            if (phase == Phase.STEADY)
                continue;
            KeyState k = new KeyState(phase, Epoch.create(10), 1000, 1, 1, Epoch.create(10), ImmutableSet.of(), new UUID(7, 7), 500, ImmutableSet.of(),
                                      KeyState.BaseState.RESIDUE, frozen, 0, phase.isReverse() ? KeyState.ReverseMode.FAST : null, 0);
            useRegistry(layout(LOGICAL, 8, Map.of(KEY, k)));
            assertEquals(phase.name(), Optional.empty(), policy.plan(d, view));
        }
    }

    // ---- helpers for the registry ----

    private static KeyState state(Phase phase)
    {
        return new KeyState(phase, Epoch.create(10), 1000, 1, 1, Epoch.create(10), ImmutableSet.of(), phase == Phase.STEADY ? null : new UUID(7, 7));
    }

    private static TableLayout layout(TableId logical, int buckets, Map<ByteBuffer, KeyState> keys)
    {
        LayoutHeader header = new LayoutHeader(new UUID(8, logical.asUUID().getLeastSignificantBits()), logical, TableId.fromUUID(new UUID(99, logical.asUUID().getLeastSignificantBits())),
                                               KS, logical == LOGICAL ? TABLE : "x" + logical.asUUID().getLeastSignificantBits(), "overflow" + logical.asUUID().getLeastSignificantBits(),
                                               TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, buckets, 1);
        return new TableLayout(header, 2, keys, 3);
    }

    private void useRegistry(TableLayout... layouts)
    {
        ImmutableMap.Builder<TableId, TableLayout> map = ImmutableMap.builder();
        for (TableLayout l : layouts)
            map.put(l.logical, l);
        env.metadata = env.metadata.transformer().with(LayoutRegistryValue.KEY,
                                                      new LayoutRegistryValue(new LayoutRegistry(LayoutRegistry.FORMAT_VERSION, map.build()))).build().metadata;
    }

    private void record(Subject target, ActionState state, long updated)
    {
        Action a = new Action(UUID.randomUUID(), PartitionSplitActuator.NAME, ActionScope.CLUSTER, target, Map.of(), null);
        records.add(new ActionRecord(a, state, updated, null, "partition_split", false, null, Map.of(), Map.of(), Map.of(), null, updated));
    }
}
