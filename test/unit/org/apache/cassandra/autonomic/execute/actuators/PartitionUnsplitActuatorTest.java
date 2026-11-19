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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.detectors.SplitOutcomeDetectors;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.FakeCluster;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.ActionExecutor;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActuatorRegistry;
import org.apache.cassandra.autonomic.execute.ClusterActionExecution;
import org.apache.cassandra.autonomic.execute.ManualScheduler;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.BasePurge;
import org.apache.cassandra.autonomic.layout.BeginUnsplit;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutEvents;
import org.apache.cassandra.autonomic.layout.LayoutHeader;
import org.apache.cassandra.autonomic.layout.LayoutOwnerSteps;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.LayoutRegistryValue;
import org.apache.cassandra.autonomic.layout.NaturalReplicas;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.RemoveKey;
import org.apache.cassandra.autonomic.layout.ReportBackfill;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.Replica;
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
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.MembershipUtils;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.autonomic.layout.KeyState.BaseState.RESIDUE;
import static org.apache.cassandra.autonomic.layout.KeyState.BaseState.TOMBSTONED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * T6.17f: {@link PartitionUnsplitActuator} against hand-built split states in a {@link FakeCluster} log, with the real
 * layout and ledger transformations and coordinator, a fake epoch watermark, fake purge and backfill reports
 * (committed as {@code BasePurge} and {@code BackfillTask} would), a fake equivalence check, fake published purge
 * reasons and fake data writes. Time is the cluster's fake clock. Every commit that changes the key's phase or removes
 * it is checked for rule O1.
 */
public class PartitionUnsplitActuatorTest
{
    private static final String KS = "ks";
    private static final String TABLE = "t";
    private static final TableId LOGICAL = TableId.fromUUID(new UUID(10, 10));
    private static final TableId PHYSICAL = TableId.fromUUID(new UUID(11, 11));
    private static final UUID LAYOUT_ID = new UUID(0x1a2b3c4d00004000L, 0x8000000000000017L);
    private static final String PHYSICAL_NAME = PhysicalTables.physicalName(TABLE, LAYOUT_ID);
    private static final ByteBuffer KEY = Int32Type.instance.decompose(42);
    private static final ByteBuffer OTHER_KEY = Int32Type.instance.decompose(43);
    private static final String KEY_HEX = ByteBufferUtil.bytesToHex(KEY);
    private static final int BUCKETS = 4;
    private static final int INCARNATION = 3;
    private static final long WRITE_TIMEOUT = 2000;
    private static final int GC_GRACE = 864000;
    private static final long G = GC_GRACE * 1000L / 2;
    private static final UUID FORWARD = new UUID(0x1000, 1);
    private static final int MAX_STEPS = 3000;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    private FakeCluster cluster;
    private final AutonomicConfig config = new AutonomicConfig();
    private FakeEnv env;
    private FakeCluster.Node node1;
    private FakeCluster.Node node2;
    private ClusterCoordinator coordinator1;
    private ClusterCoordinator coordinator2;

    // ---------------------------------------------------------------------------------------------------------
    // fakes
    // ---------------------------------------------------------------------------------------------------------

    final class FakeEnv implements PartitionUnsplitActuator.Env
    {
        volatile boolean allAnswer = true;
        boolean allAlive = true;
        NodeId lagging;
        Epoch laggingAt;
        final List<PartitionSplitActuatorTest.FakeCheck> checks = new ArrayList<>();
        int checksStarted;
        final List<String> writes = new ArrayList<>();
        long bytes;
        final Map<NodeId, BasePurge.Published> published = new TreeMap<>();
        /** The epoch of the last RemoveKey and when it was committed; the bucket deletion is checked against both. */
        Epoch removedAt;
        boolean failBase;

        @Override
        public ClusterMetadata metadata()
        {
            return cluster.log;
        }

        Epoch reported(NodeId node)
        {
            return node.equals(lagging) ? laggingAt : cluster.log.epoch;
        }

        boolean allAt(Epoch epoch)
        {
            for (NodeId node : cluster.log.directory.peerIds())
                if (epoch.isAfter(reported(node)))
                    return false;
            return true;
        }

        @Override
        public boolean acknowledged(Epoch epoch)
        {
            return allAnswer && allAt(epoch);
        }

        @Override
        public boolean allAlive(ClusterMetadata metadata)
        {
            return allAlive;
        }

        @Override
        public void dropTable(String keyspace, String table)
        {
            assertNotNull("the overflow table is dropped only after RemoveKey", removedAt);
            assertTrue("the overflow table is dropped only after every node acknowledged the removal", allAt(removedAt));
            KeyspaceMetadata ks = keyspace();
            setSchema(schema().withAddedOrUpdated(ks.withSwapped(ks.tables.without(table))));
            writes.add("drop " + table);
        }

        @Override
        public void deleteBuckets(TableMetadata logical, TableMetadata physical, int buckets, ByteBuffer key, int incarnation, long timestampMicros)
        {
            writes.add("deleteBuckets " + incarnation + " of " + buckets + " removed=" + (key(cluster.log) == null)
                       + " acknowledged=" + (removedAt != null && allAt(removedAt)));
        }

        @Override
        public void deleteBase(TableMetadata logical, ByteBuffer key, long timestampMicros)
        {
            if (failBase)
                throw new IllegalStateException("a base replica is down");
            KeyState state = key(cluster.log);
            writes.add("deleteBase " + timestampMicros + " in " + (state == null ? null : state.phase) + " " + (state == null ? null : state.baseState));
        }

        @Override
        public Set<NodeId> baseReplicas(ClusterMetadata metadata, TableMetadata logical, ByteBuffer key)
        {
            return NaturalReplicas.of(metadata, logical, key);
        }

        @Override
        public Epoch baseReplicasModified(ClusterMetadata metadata, TableMetadata logical, ByteBuffer key)
        {
            return NaturalReplicas.lastModified(metadata, logical, key);
        }

        @Override
        public Set<NodeId> bucketReplicas(ClusterMetadata metadata, TableMetadata logical, TableMetadata physical, ByteBuffer key, int incarnation, int buckets)
        {
            return NaturalReplicas.ofBuckets(metadata, logical, physical, key, incarnation, buckets);
        }

        @Override
        public Epoch bucketReplicasModified(ClusterMetadata metadata, TableMetadata logical, TableMetadata physical, ByteBuffer key, int incarnation, int buckets)
        {
            return NaturalReplicas.bucketsLastModified(metadata, logical, physical, key, incarnation, buckets);
        }

        @Override
        public LayoutOwnerSteps.Check check(ClusterMetadata metadata, TableId logical, ByteBuffer key, int pageRows, long recheckDelayMillis, LongSupplier clock)
        {
            checksStarted++;
            return checks.isEmpty() ? new PartitionSplitActuatorTest.FakeCheck() : checks.remove(0);
        }

        @Override
        public long estimatedBytes(TableMetadata logical, TableMetadata physical, ByteBuffer key, int incarnation, int buckets)
        {
            return bytes;
        }

        @Override
        public Map<NodeId, BasePurge.Published> purgeReasons(ClusterMetadata metadata, TableId logical, ByteBuffer key, Set<NodeId> nodes)
        {
            Map<NodeId, BasePurge.Published> out = new TreeMap<>(published);
            out.keySet().retainAll(nodes);
            return out;
        }

        @Override
        public long writeTimeoutMillis()
        {
            return WRITE_TIMEOUT;
        }

        @Override
        public long nowMillis()
        {
            return cluster.now.get();
        }
    }

    private Keyspaces schema()
    {
        return cluster.log.schema.getKeyspaces();
    }

    private KeyspaceMetadata keyspace()
    {
        return schema().getNullable(KS);
    }

    private void setSchema(Keyspaces keyspaces)
    {
        cluster.log = cluster.log.transformer().with(new DistributedSchema(keyspaces)).build().metadata;
        catchUp();
    }

    private void catchUp()
    {
        node1.catchUp();
        node2.catchUp();
    }

    private void setReplicas(int... nodes)
    {
        Epoch at = cluster.log.epoch.nextEpoch();
        Range<Token> ring = new Range<>(cluster.log.partitioner.getMinimumToken(), new org.apache.cassandra.dht.Murmur3Partitioner.LongToken(Long.MAX_VALUE));
        DataPlacement.Builder placement = DataPlacement.builder();
        for (int i : nodes)
        {
            Replica replica = Replica.fullReplica(MembershipUtils.endpoint(i), ring);
            placement.withReadReplica(at, replica).withWriteReplica(at, replica);
        }
        cluster.log = cluster.log.transformer().with(DataPlacements.builder(1).with(keyspace().params.replication, placement.build()).build()).build().metadata;
        catchUp();
    }

    private static TableMetadata logicalTable()
    {
        return TableMetadata.builder(KS, TABLE).id(LOGICAL)
                            .addPartitionKeyColumn("p", Int32Type.instance)
                            .addClusteringColumn("c", Int32Type.instance)
                            .addRegularColumn("v", UTF8Type.instance)
                            .params(TableParams.builder().gcGraceSeconds(GC_GRACE).build())
                            .build();
    }

    private static TableMetadata physicalTable()
    {
        return TableMetadata.builder(KS, PHYSICAL_NAME).id(PHYSICAL)
                            .addPartitionKeyColumn("p", Int32Type.instance)
                            .addPartitionKeyColumn(TableLayout.BUCKET_COLUMN, Int32Type.instance)
                            .addClusteringColumn("c", Int32Type.instance)
                            .addRegularColumn("v", UTF8Type.instance)
                            .gcGraceSeconds(GC_GRACE)
                            .build();
    }

    private static KeyState key(ClusterMetadata md)
    {
        TableLayout layout = LayoutRegistry.of(md).layout(LOGICAL);
        return layout == null ? null : layout.keyState(KEY);
    }

    @Before
    public void setUp()
    {
        cluster = new FakeCluster(3);
        node1 = cluster.node(1);
        node2 = cluster.node(2);
        setSchema(Keyspaces.of(KeyspaceMetadata.create(KS, KeyspaceParams.simple(2), Tables.of(logicalTable(), physicalTable()))));
        coordinator1 = new ClusterCoordinator(node1);
        coordinator2 = new ClusterCoordinator(node2);
        assertTrue(coordinator1.setEnabled(true));
        node2.catchUp();
        env = new FakeEnv();
        setReplicas(1, 2);
        AutonomicConfig.LayoutConfig lc = config.layout;
        lc.ack_poll_interval = new DurationSpec.IntMillisecondsBound("100ms");
        lc.cutover_soak = new DurationSpec.IntMillisecondsBound("1s");
        lc.straggler_window = new DurationSpec.LongMillisecondsBound("2s");
        lc.reconcile_budget = new DurationSpec.LongMillisecondsBound("1m");
        lc.verify_recheck_delay = new DurationSpec.IntMillisecondsBound("100ms");
        lc.purge_timeout = new DurationSpec.LongMillisecondsBound("1m");
        lc.commit_approval = "auto";
        LayoutEvents.clear();
        reportPurge = true;
        reportBackfill = true;
    }

    /** The key split in {@code phase}, owned by {@code owner}, with the base state and times given; FORMAT 2. */
    private void splitKey(Phase phase, UUID owner, KeyState.BaseState base, long frozen, long tombstoneMicros, long dualWriteSince, boolean otherKey)
    {
        Epoch e = cluster.log.epoch;
        KeyState state = new KeyState(phase, e, cluster.now.get() - 1000, INCARNATION, 2, e, ImmutableSet.of(), owner, dualWriteSince, ImmutableSet.of(),
                                      base, frozen, tombstoneMicros, null, 0);
        Map<ByteBuffer, KeyState> keys = new LinkedHashMap<>();
        keys.put(KEY, state);
        if (otherKey)
            keys.put(OTHER_KEY, new KeyState(Phase.STEADY, e, 1, 1, 1, e, ImmutableSet.of(), null, 400, ImmutableSet.of(), RESIDUE, 500, 0, null, 0));
        LayoutHeader header = new LayoutHeader(LAYOUT_ID, LOGICAL, PHYSICAL, KS, TABLE, PHYSICAL_NAME, TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO,
                                               BUCKETS, 1);
        LayoutRegistry registry = new LayoutRegistry(LayoutRegistry.FORMAT_VERSION_2, ImmutableMap.of(LOGICAL, new TableLayout(header, INCARNATION + 1, keys, 5)));
        cluster.log = cluster.log.transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(registry)).build().metadata;
        catchUp();
    }

    /** A completed split, released with its residue, frozen {@code agoMillis} ago. */
    private void steadyResidue(long agoMillis)
    {
        long now = cluster.now.get();
        splitKey(Phase.STEADY, null, RESIDUE, now - agoMillis, 0, now - agoMillis - 60_000, false);
    }

    /** A split whose base was reclaimed {@code agoMillis} ago. */
    private long steadyTombstoned(long agoMillis)
    {
        long now = cluster.now.get();
        long tt = (now - agoMillis) * 1000;
        splitKey(Phase.STEADY, null, TOMBSTONED, now - agoMillis - 3_600_000, tt, now - agoMillis - 3_700_000, false);
        return tt;
    }

    // ---------------------------------------------------------------------------------------------------------
    // driver
    // ---------------------------------------------------------------------------------------------------------

    enum Entry { STEP, RECOVER, ABORT }

    boolean reportPurge;
    boolean reportBackfill;

    final class Owner
    {
        final PartitionUnsplitActuator actuator = new PartitionUnsplitActuator(env, () -> config);
        final ClusterCoordinator coordinator;
        final FakeCluster.Node node;
        final Action action;
        UUID id;
        Entry entry = Entry.STEP;
        StepResult last;
        final List<String> phases = new ArrayList<>();
        boolean autoApprove;

        Owner(ClusterCoordinator coordinator, FakeCluster.Node node, String mode)
        {
            this.coordinator = coordinator;
            this.node = node;
            this.action = new Action(cluster.newActionId(), PartitionUnsplitActuator.NAME, ActionScope.CLUSTER,
                                     PartitionSplitActuator.target(KS, TABLE, KEY_HEX), PartitionUnsplitActuator.params(mode), null);
            this.id = action.id;
        }

        Owner claimLane()
        {
            ClusterCoordinator.Outcome outcome = coordinator.claim(Lane.SCHEMA_LAYOUT, id, PartitionUnsplitActuator.NAME, action.target.asString(),
                                                                   ClusterActionExecution.INITIAL_PHASE, Collections.emptyMap());
            assertTrue(outcome.toString(), outcome.ok());
            return this;
        }

        Owner takeOverExpired(Owner previous)
        {
            catchUp();
            Claim expired = coordinator.ledger().claim(previous.id);
            assertNotNull(expired);
            cluster.now.set(expired.leaseExpiresAtMillis + 1);
            node.down.add(previous.node.self);
            ActuatorRegistry registry = new ActuatorRegistry();
            registry.register(actuator);
            registry.setEnabled(PartitionUnsplitActuator.NAME, true);
            ActionExecutor.Host host = new ActionExecutor.Host()
            {
                public String gate() { return null; }
                public boolean killed() { return false; }
                public void actionFailed(ActionRecord record, String detail) { }
            };
            ActionExecutor.Reporter reporter = (next, from, detail, outcome) -> true;
            ClusterActionExecution execution = new ClusterActionExecution(cluster.now::get, () -> config, coordinator, registry, new ManualScheduler(),
                                                                          null, host, () -> reporter);
            execution.tick();
            node.down.clear();
            Claim taken = coordinator.ledger().claims.get(Lane.SCHEMA_LAYOUT);
            assertNotNull("the tick took the expired claim over", taken);
            assertEquals(Claim.RECOVERING, taken.phase);
            assertEquals(expired.state, taken.state);
            id = taken.actionId;
            entry = Entry.RECOVER;
            return this;
        }

        Claim claim()
        {
            return coordinator.ownClaim(id);
        }

        ActionContext ctx()
        {
            long now = cluster.now.get();
            return new ActionContext()
            {
                public long stepDeadlineMillis()
                {
                    return now + 30_000;
                }

                public long nowMillis()
                {
                    return cluster.now.get();
                }

                public boolean killed()
                {
                    return false;
                }

                public void recordUndo(Map<String, String> undo)
                {
                }
            };
        }

        StepResult step() throws Exception
        {
            catchUp();
            Claim claim = claim();
            assertNotNull("action " + id + " holds no claim", claim);
            StepResult r;
            switch (entry)
            {
                case STEP: r = actuator.step(action, claim, ctx()); break;
                case RECOVER: r = actuator.recover(claim, ctx()); break;
                default: r = actuator.abort(action, claim, ctx());
            }
            last = r;
            switch (r.kind)
            {
                case STAY:
                    cluster.now.addAndGet(Math.max(1, Math.min(r.delayMillis, 60_000)));
                    break;
                case ADVANCE:
                    assertTrue(r.toString(), coordinator.advance(claim, r.phase, r.state).ok());
                    break;
                case COMMIT:
                    checkO1(r.transformation);
                    ClusterCoordinator.Outcome outcome = coordinator.commitInstead(claim, r.transformation, r.phase, r.state);
                    assertTrue(r + " " + outcome, outcome.ok());
                    if (child(r.transformation) instanceof RemoveKey)
                        env.removedAt = cluster.log.epoch;
                    break;
                default:
                    assertTrue(coordinator.release(claim, finalState(r)).ok());
            }
            KeyState state = key(cluster.log);
            String phase = state == null ? "absent" : state.phase + "/" + state.generation;
            if (phases.isEmpty() || !phases.get(phases.size() - 1).equals(phase))
                phases.add(phase);
            report();
            if (autoApprove && claim() != null && Phase.CUTBACK.name().equals(claim().phase))
                assertTrue(PartitionUnsplitActuator.approveCommit(coordinator, id));
            return r;
        }

        StepResult until(Predicate<Owner> done) throws Exception
        {
            for (int i = 0; i < MAX_STEPS; i++)
            {
                if (done.test(this))
                    return last;
                if (step().isTerminal())
                    return last;
            }
            throw new AssertionError("no progress after " + MAX_STEPS + " steps; phases " + phases + " claim " + claim());
        }

        StepResult toEnd() throws Exception
        {
            StepResult r = until(o -> false);
            assertTrue(r.toString(), r.isTerminal());
            return r;
        }

        Owner reach(Phase phase) throws Exception
        {
            until(o -> {
                KeyState k = key(cluster.log);
                return k != null && k.phase == phase && claim() != null && phase.name().equals(claim().phase);
            });
            assertEquals(phases.toString(), phase, key(cluster.log).phase);
            return this;
        }

        /** Records an operator's abort request in the claim, as {@code layout abort} does. */
        void requestAbort(String why)
        {
            Claim claim = claim();
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(PartitionUnsplitActuator.S_ABORT, why);
            assertTrue(coordinator.advance(claim, claim.phase, state).ok());
        }
    }

    private Owner owner(String mode)
    {
        return new Owner(coordinator1, node1, mode).claimLane();
    }

    private static String finalState(StepResult r)
    {
        switch (r.kind)
        {
            case COMMITTED: return ClusterActionExecution.FINAL_COMMITTED;
            case COMMITTED_CLEANUP_PENDING: return ClusterActionExecution.FINAL_COMMITTED_CLEANUP_PENDING;
            case FAILED: return ClusterActionExecution.FINAL_FAILED;
            default: return ClusterActionExecution.FINAL_ROLLED_BACK;
        }
    }

    private static Transformation child(Transformation t)
    {
        return t instanceof CustomTransformation ? ((CustomTransformation) t).child() : t;
    }

    private void checkO1(Transformation t)
    {
        Transformation child = child(t);
        if (!(child instanceof AdvanceLayout || child instanceof RemoveKey))
            return;
        KeyState state = key(cluster.log);
        assertNotNull(state);
        assertTrue(child + " committed before every node had epoch " + state.phaseEpoch.getEpoch() + " of " + state.phase, env.allAt(state.phaseEpoch));
    }

    /**
     * What the replicas commit: in a slow REVERSE_ANNOUNCED the base replicas' purge reports; in a generation for which
     * a reverse pass is due, the bucket replicas' backfill reports (as BackfillTask runs only then).
     */
    private void report()
    {
        KeyState state = key(cluster.log);
        if (state == null)
            return;
        TableMetadata logical = keyspace().tables.getNullable(TABLE);
        Set<NodeId> replicas;
        if (state.phase == Phase.REVERSE_ANNOUNCED && state.reverseMode == KeyState.ReverseMode.SLOW && reportPurge)
            replicas = NaturalReplicas.of(cluster.log, logical, KEY);
        else if (state.reverseBackfillDue() && reportBackfill)
            replicas = NaturalReplicas.ofBuckets(cluster.log, logical, keyspace().tables.getNullable(PHYSICAL_NAME), KEY, state.incarnation, BUCKETS);
        else
            return;
        for (NodeId node : replicas)
            if (!state.backfillDone.contains(node))
            {
                Transformation.Result result = new ReportBackfill(LOGICAL, KEY, state.generation, state.generationEpoch, node).wrap().execute(cluster.log);
                assertTrue(String.valueOf(result), result.isSuccess());
                cluster.log = result.success().metadata;
                state = key(cluster.log);
            }
        catchUp();
    }

    private static final List<Phase> PRE_COMMIT = List.of(Phase.REVERSE_ANNOUNCED, Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL, Phase.REVERSE_VERIFY,
                                                          Phase.CUTBACK);

    private void assertReleased(KeyState.BaseState base)
    {
        KeyState k = key(cluster.log);
        assertNotNull(k);
        assertEquals(Phase.STEADY, k.phase);
        assertNull("released", k.actionId);
        assertEquals(base, k.baseState);
        assertNull(k.reverseMode);
    }

    // ---------------------------------------------------------------------------------------------------------
    // every transition
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void everyTransitionOfAFastUnsplitInOrder() throws Exception
    {
        steadyResidue(60_000);
        Owner owner = owner(PartitionUnsplitActuator.MODE_AUTO);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.COMMITTED, end.kind);
        // design gap of T6.17c: REVERSE_BACKFILL starts its own generation; REVERSE_CLEANUP its reconcile generation
        assertEquals(List.of("STEADY/2", "REVERSE_ANNOUNCED/3", "REVERSE_DUAL_WRITE/3", "REVERSE_BACKFILL/3", "REVERSE_BACKFILL/4", "REVERSE_VERIFY/4",
                             "CUTBACK/4", "BASE_ONLY/4", "REVERSE_CLEANUP/4", "REVERSE_CLEANUP/5", "absent"), owner.phases);
        assertTrue(end.detail, end.detail.contains("FAST mode") && end.detail.contains("missing_from_overflow=0"));
        // the last key: the layout went with it, and the overflow table is dropped after the acknowledged removal
        assertTrue(LayoutRegistry.of(cluster.log).byLogical.isEmpty());
        assertEquals(List.of("drop " + PHYSICAL_NAME), env.writes);
        assertEquals(1, env.checksStarted);
    }

    @Test
    public void aSlowUnsplitWaitsForEveryBaseReplicasPurgeAndNeverCountsItAsTheCopy() throws Exception
    {
        long tt = steadyTombstoned(11L * 24 * 3_600_000);
        reportPurge = false;
        Owner owner = owner(PartitionUnsplitActuator.MODE_AUTO).reach(Phase.REVERSE_ANNOUNCED);
        long until = cluster.now.get() + 30_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals("no purge report, no dual write", Phase.REVERSE_ANNOUNCED, key(cluster.log).phase);
        reportPurge = true;
        owner.reach(Phase.REVERSE_DUAL_WRITE);
        KeyState dual = key(cluster.log);
        assertEquals(KeyState.ReverseMode.SLOW, dual.reverseMode);
        assertEquals("the purge reports are in the generation of BeginUnsplit", 2, dual.backfillDone.size());
        // that generation never completes the copy: a new one starts in REVERSE_BACKFILL
        owner.reach(Phase.REVERSE_VERIFY);
        KeyState verify = key(cluster.log);
        assertEquals(dual.generation + 1, verify.generation);
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
        assertTrue(owner.phases.toString(), owner.phases.contains("REVERSE_BACKFILL/" + (dual.generation + 1)));
        assertTrue(tt > 0);
    }

    @Test
    public void aSecondKeyKeepsTheLayoutAndItsBucketsAreDeletedAfterTheAcknowledgedRemoval() throws Exception
    {
        long now = cluster.now.get();
        splitKey(Phase.STEADY, null, RESIDUE, now - 60_000, 0, now - 120_000, true);
        Owner owner = owner(PartitionUnsplitActuator.MODE_FAST);
        owner.until(o -> o.claim() != null && PartitionUnsplitActuator.KEY_REMOVED.equals(o.claim().phase));
        // node 3 has not seen the removal: nothing is deleted
        env.lagging = cluster.id(3);
        env.laggingAt = Epoch.create(env.removedAt.getEpoch() - 1);
        long until = cluster.now.get() + 60_000;
        owner.until(o -> cluster.now.get() > until);
        assertTrue("no deletion before every node acknowledged RemoveKey: " + env.writes, env.writes.isEmpty());
        env.lagging = null;
        long acknowledged = cluster.now.get();
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.COMMITTED, end.kind);
        assertEquals(List.of("deleteBuckets " + INCARNATION + " of " + BUCKETS + " removed=true acknowledged=true"), env.writes);
        assertTrue("and not before the write timeout", cluster.now.get() >= acknowledged + WRITE_TIMEOUT);
        assertNull(key(cluster.log));
        assertNotNull("the other key keeps the layout", LayoutRegistry.of(cluster.log).layout(LOGICAL));
        assertNotNull(keyspace().tables.getNullable(PHYSICAL_NAME));
    }

    @Test
    public void nothingAdvancesWithoutEveryNodesAcknowledgement() throws Exception
    {
        steadyResidue(60_000);
        Owner owner = owner(PartitionUnsplitActuator.MODE_FAST);
        for (Phase phase : List.of(Phase.REVERSE_ANNOUNCED, Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL, Phase.REVERSE_VERIFY, Phase.CUTBACK,
                                   Phase.BASE_ONLY, Phase.REVERSE_CLEANUP))
        {
            owner.reach(phase);
            env.allAnswer = false;
            String before = key(cluster.log).phase + "/" + key(cluster.log).generation;
            long until = cluster.now.get() + 30_000;
            owner.until(o -> cluster.now.get() > until);
            KeyState now = key(cluster.log);
            assertEquals("held in " + phase, before, now.phase + "/" + now.generation);
            env.allAnswer = true;
        }
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    @Test
    public void theReverseCommitPointWaitsForTheSecondApproval() throws Exception
    {
        config.layout.commit_approval = "manual";
        steadyResidue(60_000);
        Owner owner = owner(PartitionUnsplitActuator.MODE_FAST).reach(Phase.CUTBACK);
        long until = cluster.now.get() + 120_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals("no BASE_ONLY without the second approval", Phase.CUTBACK, key(cluster.log).phase);
        assertFalse("the split's approval does not apply", PartitionSplitActuator.approveCommit(coordinator1, owner.id));
        assertTrue(PartitionUnsplitActuator.approveCommit(coordinator1, owner.id));
        owner.reach(Phase.BASE_ONLY);
        assertFalse("not approvable outside CUTBACK", PartitionUnsplitActuator.approveCommit(coordinator1, owner.id));
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    @Test
    public void aVerificationFailureBackfillsOnceThenAborts() throws Exception
    {
        steadyResidue(60_000);
        PartitionSplitActuatorTest.FakeCheck differs = new PartitionSplitActuatorTest.FakeCheck();
        differs.contentDiffers = 1;
        PartitionSplitActuatorTest.FakeCheck missing = new PartitionSplitActuatorTest.FakeCheck();
        missing.missingFromBase = 2;
        env.checks.add(differs);
        env.checks.add(missing);
        StepResult end = owner(PartitionUnsplitActuator.MODE_FAST).toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains("reverse verification failed 2 times"));
        assertEquals(2, env.checksStarted);
        assertReleased(RESIDUE);
    }

    @Test
    public void rowsOnlyInBaseAreToleratedAndCounted() throws Exception
    {
        steadyResidue(60_000);
        PartitionSplitActuatorTest.FakeCheck baseOnly = new PartitionSplitActuatorTest.FakeCheck();
        baseOnly.missingFromOverflow = 3;
        env.checks.add(baseOnly);
        StepResult end = owner(PartitionUnsplitActuator.MODE_FAST).toEnd();
        assertEquals(end.toString(), StepResult.Kind.COMMITTED, end.kind);
        assertTrue(end.detail, end.detail.contains("missing_from_overflow=3"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // abort before the commit point, none after it
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void anAbortFromEachPhaseBeforeTheCommitPointReturnsToSteadyAndReleases() throws Exception
    {
        for (Phase phase : PRE_COMMIT)
        {
            setUp();
            steadyResidue(60_000);
            long frozen = key(cluster.log).frozenSinceMillis;
            Owner owner = owner(PartitionUnsplitActuator.MODE_FAST).reach(phase);
            owner.requestAbort("operator request in " + phase);
            StepResult end = owner.toEnd();
            assertEquals(phase + ": " + end, StepResult.Kind.ROLLED_BACK, end.kind);
            assertTrue(end.detail, end.detail.contains("operator request in " + phase));
            assertReleased(RESIDUE);
            assertEquals(owner.phases.toString(), phase == Phase.CUTBACK, owner.phases.stream().anyMatch(p -> p.startsWith("REVERSE_ROLLBACK")));
            // 11.4: from ANNOUNCED the residue is unchanged; from a complete generation (VERIFY, CUTBACK) it is frozen anew;
            // from a partial copy in fast mode the old freeze stays
            long after = key(cluster.log).frozenSinceMillis;
            if (phase == Phase.REVERSE_ANNOUNCED || phase == Phase.REVERSE_DUAL_WRITE || phase == Phase.REVERSE_BACKFILL)
                assertEquals(phase.toString(), frozen, after);
            else
                assertTrue(phase.toString(), after > frozen);
            assertTrue("nothing is deleted on an abort: " + env.writes, env.writes.isEmpty());
        }
    }

    @Test
    public void aSlowAbortFromAnnouncedKeepsTheTombstone() throws Exception
    {
        long tt = steadyTombstoned(11L * 24 * 3_600_000);
        Owner owner = owner(PartitionUnsplitActuator.MODE_SLOW).reach(Phase.REVERSE_ANNOUNCED);
        owner.entry = Entry.ABORT;
        assertEquals(StepResult.Kind.ROLLED_BACK, owner.toEnd().kind);
        assertReleased(TOMBSTONED);
        assertEquals(tt, key(cluster.log).baseTombstoneMicros);
    }

    @Test
    public void noAbortAfterTheReverseCommitPoint() throws Exception
    {
        for (Phase phase : List.of(Phase.BASE_ONLY, Phase.REVERSE_CLEANUP))
        {
            setUp();
            steadyResidue(60_000);
            Owner owner = owner(PartitionUnsplitActuator.MODE_FAST).reach(phase);
            assertTrue(owner.actuator.pastCommitPoint(owner.claim()));
            owner.entry = Entry.ABORT;
            StepResult stop = owner.step();
            assertEquals(stop.toString(), StepResult.Kind.COMMITTED_CLEANUP_PENDING, stop.kind);
            assertEquals(phase, key(cluster.log).phase);
            assertEquals("the key stays owned, for a resume", owner.id, key(cluster.log).actionId);

            // a resume adopts and finishes it, as a continuation
            Owner resume = new Owner(coordinator1, node1, PartitionUnsplitActuator.MODE_RESUME);
            assertEquals(Optional.empty(), resume.actuator.validate(resume.action, null));
            assertTrue(resume.actuator.continuation(resume.action));
            resume.claimLane();
            assertEquals(StepResult.Kind.COMMITTED, resume.toEnd().kind);
            assertNull(key(cluster.log));
        }
        // and an abort request recorded in the claim after the commit point is not acted on
        setUp();
        steadyResidue(60_000);
        Owner owner = owner(PartitionUnsplitActuator.MODE_FAST).reach(Phase.BASE_ONLY);
        owner.requestAbort("too late");
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    @Test
    public void theReverseDeadlineAbortsBeforeTheCommitPoint() throws Exception
    {
        // fast: the replicas' limit is frozenSince + G - wt; the owner's deadline straggler_window before it
        long now = cluster.now.get();
        long frozen = now - G + WRITE_TIMEOUT + 70_000;
        splitKey(Phase.STEADY, null, RESIDUE, frozen, 0, frozen - 60_000, false);
        reportBackfill = false;
        Owner owner = owner(PartitionUnsplitActuator.MODE_FAST);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains("the reverse deadline passed in REVERSE_BACKFILL"));
        long limit = frozen + G - WRITE_TIMEOUT;
        assertTrue(cluster.now.get() >= limit - 2000);
        assertTrue("strictly before the replicas' limit", cluster.now.get() < limit);
        assertReleased(RESIDUE);
    }

    // ---------------------------------------------------------------------------------------------------------
    // takeover
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void aTakeoverInEachPhaseAbortsBeforeTheCommitPointAndFinishesAfterIt() throws Exception
    {
        List<Phase> all = new ArrayList<>(PRE_COMMIT);
        all.add(Phase.BASE_ONLY);
        all.add(Phase.REVERSE_CLEANUP);
        for (Phase phase : all)
        {
            setUp();
            steadyResidue(60_000);
            Owner first = new Owner(coordinator1, node1, PartitionUnsplitActuator.MODE_FAST).claimLane().reach(phase);
            Owner second = new Owner(coordinator2, node2, PartitionUnsplitActuator.MODE_FAST).takeOverExpired(first);
            StepResult end = second.toEnd();
            if (phase.pastCommit())
            {
                assertEquals(phase + ": " + end, StepResult.Kind.COMMITTED, end.kind);
                assertNull(key(cluster.log));
            }
            else
            {
                assertEquals(phase + ": " + end, StepResult.Kind.ROLLED_BACK, end.kind);
                assertReleased(RESIDUE);
            }
        }
        // after RemoveKey, before the buckets are gone
        setUp();
        long now = cluster.now.get();
        splitKey(Phase.STEADY, null, RESIDUE, now - 60_000, 0, now - 120_000, true);
        Owner first = new Owner(coordinator1, node1, PartitionUnsplitActuator.MODE_FAST).claimLane();
        first.until(o -> o.claim() != null && PartitionUnsplitActuator.KEY_REMOVED.equals(o.claim().phase));
        Owner second = new Owner(coordinator2, node2, PartitionUnsplitActuator.MODE_FAST).takeOverExpired(first);
        assertEquals(StepResult.Kind.COMMITTED, second.toEnd().kind);
        assertEquals(List.of("deleteBuckets " + INCARNATION + " of " + BUCKETS + " removed=true acknowledged=true"), env.writes);
    }

    // ---------------------------------------------------------------------------------------------------------
    // mode selection (design 11.5)
    // ---------------------------------------------------------------------------------------------------------

    private Optional<String> validate(String mode)
    {
        Owner o = new Owner(coordinator1, node1, mode);
        return o.actuator.validate(o.action, null);
    }

    @Test
    public void fastModeIsRefusedPastItsWindowWithTheEarliestSlowTime() throws Exception
    {
        long now = cluster.now.get();
        // frozen so long ago that the reverse commit window has closed
        steadyResidue(G);
        Optional<String> fast = validate(PartitionUnsplitActuator.MODE_FAST);
        assertTrue(fast.toString(), fast.isPresent() && fast.get().contains("fast mode closed"));
        assertTrue(fast.get(), fast.get().contains("slow mode from about"));
        Optional<String> auto = validate(PartitionUnsplitActuator.MODE_AUTO);
        assertTrue(auto.toString(), auto.isPresent() && auto.get().contains("slow mode needs a base tombstone"));

        // an estimate that does not fit: one hour of copy at the configured throughput
        setUp();
        steadyResidue(G - 2 * 3_600_000L);
        assertEquals(Optional.empty(), validate(PartitionUnsplitActuator.MODE_FAST));
        env.bytes = (long) (config.layout.backfill_throughput.toBytesPerSecond() * 3 * 3600);
        Optional<String> big = validate(PartitionUnsplitActuator.MODE_FAST);
        assertTrue(big.toString(), big.isPresent() && big.get().contains("estimate"));

        // slow mode: from the tombstone's time plus gc_grace
        setUp();
        long tt = steadyTombstoned(3_600_000);
        Optional<String> slow = validate(PartitionUnsplitActuator.MODE_AUTO);
        long from = tt / 1000 + GC_GRACE * 1000L;
        assertTrue(slow.toString(), slow.isPresent() && slow.get().contains("slow mode from " + from));
        Optional<String> fastOnTombstone = validate(PartitionUnsplitActuator.MODE_FAST);
        assertTrue(fastOnTombstone.toString(), fastOnTombstone.get().contains("fast mode needs the base residue"));
        cluster.now.set(from);
        assertEquals(Optional.empty(), validate(PartitionUnsplitActuator.MODE_AUTO));
        assertTrue(now > 0);
    }

    @Test
    public void aStartIsRefusedWhileTheSplitRunsOrANodeIsDown() throws Exception
    {
        long now = cluster.now.get();
        // NEW_ONLY owned by a forward action that still holds its claim
        UUID forward = cluster.newActionId();
        assertTrue(coordinator1.claim(Lane.SCHEMA_LAYOUT, forward, PartitionSplitActuator.NAME, PartitionSplitActuator.target(KS, TABLE, KEY_HEX).asString(),
                                      ClusterActionExecution.INITIAL_PHASE, Collections.emptyMap()).ok());
        catchUp();
        splitKey(Phase.NEW_ONLY, forward, RESIDUE, now - 60_000, 0, now - 120_000, false);
        Optional<String> running = validate(PartitionUnsplitActuator.MODE_AUTO);
        assertTrue(running.toString(), running.isPresent() && running.get().contains("hands the key over once it holds no claim"));

        setUp();
        steadyResidue(60_000);
        env.allAlive = false;
        Optional<String> down = validate(PartitionUnsplitActuator.MODE_AUTO);
        assertTrue(down.toString(), down.isPresent() && down.get().contains("not every node"));
    }

    @Test
    public void aStoppedForwardActionHandsANewOnlyKeyOver() throws Exception
    {
        long now = cluster.now.get();
        splitKey(Phase.NEW_ONLY, FORWARD, RESIDUE, now - 60_000, 0, now - 120_000, false);
        Owner owner = owner(PartitionUnsplitActuator.MODE_AUTO);
        owner.reach(Phase.REVERSE_ANNOUNCED);
        assertEquals(FORWARD.toString(), owner.claim().state.get(PartitionUnsplitActuator.S_PREVIOUS));
        assertEquals(owner.id, key(cluster.log).actionId);
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    // ---------------------------------------------------------------------------------------------------------
    // a key stuck at RECONCILE_PAST_DEADLINE (owner answer 6)
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void aStuckCleanupKeyIsTombstonedByTheUnsplitThenUnsplitSlowly() throws Exception
    {
        long now = cluster.now.get();
        // the forward replicas' limit dualWriteSince + G - wt passed a moment ago; the fast window closed long ago
        long dual = now - G + WRITE_TIMEOUT - 1;
        splitKey(Phase.CLEANUP, FORWARD, RESIDUE, dual + 1000, 0, dual, false);
        long limit = dual + G - WRITE_TIMEOUT;
        long due = limit + config.layout.straggler_window.toMilliseconds();
        Optional<String> early = validate(PartitionUnsplitActuator.MODE_AUTO);
        assertTrue(early.toString(), early.isPresent() && early.get().contains("slow mode from " + due) && early.get().contains("owner answer 6"));
        cluster.now.set(due);
        assertEquals(Optional.empty(), validate(PartitionUnsplitActuator.MODE_AUTO));
        // no replica can purge before Tt + gc_grace: none reports at first
        reportPurge = false;
        Owner owner = owner(PartitionUnsplitActuator.MODE_AUTO).reach(Phase.REVERSE_ANNOUNCED);
        KeyState k = key(cluster.log);
        assertEquals(KeyState.ReverseMode.SLOW, k.reverseMode);
        assertEquals(TOMBSTONED, k.baseState);
        assertTrue(k.baseTombstoneMicros >= due * 1000);
        assertEquals(List.of("deleteBase " + k.baseTombstoneMicros + " in CLEANUP RESIDUE"), env.writes);
        // the purge cannot happen before Tt + gc_grace; the purge timeout counts from then
        long until = cluster.now.get() + 120_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals(Phase.REVERSE_ANNOUNCED, key(cluster.log).phase);
        reportPurge = true;
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.COMMITTED, end.kind);
        assertTrue(end.detail, end.detail.contains("SLOW mode"));
    }

    @Test
    public void aStuckKeysTombstoneWrittenAheadIsFinishedByANewOwner() throws Exception
    {
        long now = cluster.now.get();
        long dual = now - G - 60_000;
        splitKey(Phase.CLEANUP, FORWARD, RESIDUE, dual + 1000, 0, dual, false);
        env.failBase = true;
        Owner first = new Owner(coordinator1, node1, PartitionUnsplitActuator.MODE_AUTO).claimLane();
        first.until(o -> o.claim() != null && o.claim().state.containsKey(PartitionUnsplitActuator.S_TOMBSTONE) && cluster.now.get() > now + 10_000);
        assertEquals("the registry does not have it yet", RESIDUE, key(cluster.log).baseState);
        env.failBase = false;
        Owner second = new Owner(coordinator2, node2, PartitionUnsplitActuator.MODE_AUTO).takeOverExpired(first);
        StepResult end = second.toEnd();
        // the new owner writes and records the tombstone, then aborts the reverse before the commit point
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertReleased(TOMBSTONED);
        assertTrue(key(cluster.log).baseTombstoneMicros > 0);
        assertTrue(env.writes.toString(), env.writes.get(env.writes.size() - 1).startsWith("deleteBase " + key(cluster.log).baseTombstoneMicros + " in CLEANUP"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // purge_timeout (owner answer 3)
    // ---------------------------------------------------------------------------------------------------------

    private LayoutEvents.UnsplitResidue purgeTimesOut(Map<Integer, BasePurge.Published> published) throws Exception
    {
        setUp();
        steadyTombstoned(11L * 24 * 3_600_000);
        reportPurge = false;
        published.forEach((node, p) -> env.published.put(cluster.id(node), p));
        Owner owner = owner(PartitionUnsplitActuator.MODE_SLOW).reach(Phase.REVERSE_ANNOUNCED);
        owner.until(o -> o.claim() != null && o.claim().state.containsKey(PartitionUnsplitActuator.S_ABORT));
        long acked = Long.parseLong(owner.claim().state.get(PartitionUnsplitActuator.S_ACKED).replaceAll(".*/", ""));
        assertTrue("not before purge_timeout", cluster.now.get() >= acked + config.layout.purge_timeout.toMilliseconds());
        assertTrue(owner.claim().state.get(PartitionUnsplitActuator.S_ABORT).startsWith(PartitionUnsplitActuator.UNSPLIT_RESIDUE + ": purge_timeout 1m"));
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains(PartitionUnsplitActuator.UNSPLIT_RESIDUE));
        assertReleased(TOMBSTONED);
        List<LayoutEvents.UnsplitResidue> residues = LayoutEvents.unsplitResidues();
        assertEquals(1, residues.size());
        LayoutEvents.UnsplitResidue r = residues.get(0);
        assertEquals(KEY_HEX, r.keyHex);
        // the diagnosis carries every node's reason and the remedy
        List<Diagnosis> raised = new SplitOutcomeDetectors.UnsplitResidue().analyze(new InMemoryKnowledgeView(new ObservationStore(8, 8), cluster.now::get));
        assertEquals(1, raised.size());
        assertEquals(SplitOutcomeDetectors.UNSPLIT_RESIDUE, raised.get(0).type);
        assertEquals(r.remedy, raised.get(0).evidence.get("remedy"));
        for (String node : r.nodes.keySet())
            assertEquals(r.nodes.get(node), raised.get(0).evidence.get("node." + node));
        return r;
    }

    private static BasePurge.Published reason(BasePurge.Reason reason, long maxTimestamp, String detail)
    {
        return new BasePurge.Published(LOGICAL, KEY_HEX, reason, maxTimestamp, detail);
    }

    @Test
    public void aTombstoneOfUnknownTimeIsLearnedFromTheReplicasForThePurgeTimeout() throws Exception
    {
        // T6.17k: a key of the old forward cleanup is TOMBSTONED with Tt unknown; slow mode is allowed at once and each
        // replica waits for its own deletion's gc_grace, which it publishes; purge_timeout counts from then
        long now = cluster.now.get();
        splitKey(Phase.STEADY, null, TOMBSTONED, now - 3_600_000, 0, now - 3_700_000, false);
        assertEquals(Optional.empty(), validate(PartitionUnsplitActuator.MODE_AUTO));
        long due = now + 10 * 60_000;
        for (int node : new int[]{ 1, 2 })
            env.published.put(cluster.id(node), reason(BasePurge.Reason.WAITING_GC_GRACE, due, "the partition deletion may be purged from " + due));
        reportPurge = false;
        Owner owner = owner(PartitionUnsplitActuator.MODE_AUTO).reach(Phase.REVERSE_ANNOUNCED);
        long until = cluster.now.get() + 2 * config.layout.purge_timeout.toMilliseconds();
        owner.until(o -> cluster.now.get() > until);
        assertEquals("not aborted while the replicas wait for gc_grace", Phase.REVERSE_ANNOUNCED, key(cluster.log).phase);
        assertFalse(owner.claim().state.containsKey(PartitionUnsplitActuator.S_ABORT));
        // the replicas purge and report (before the owner's next step, which would otherwise find the time up)
        reportPurge = true;
        report();
        env.published.clear();
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    @Test
    public void purgeTimeoutAbortsWithEachNodesReasonAndRemedy() throws Exception
    {
        LayoutEvents.UnsplitResidue shadowed = purgeTimesOut(Map.of(1, reason(BasePurge.Reason.STILL_SHADOWED, 5, "the partition deletion was not purged (0 repaired, 1 unrepaired, 0 pending repair sstables)"),
                                                                    2, reason(BasePurge.Reason.STILL_SHADOWED, 5, "the partition deletion was not purged")));
        assertEquals(2, shadowed.nodes.size());
        assertTrue(shadowed.nodes.get("1"), shadowed.nodes.get("1").startsWith("STILL_SHADOWED: the partition deletion was not purged"));
        assertTrue(shadowed.remedy, shadowed.remedy.contains("Run a repair of the key's token range") && shadowed.remedy.contains("force_purge_compaction: false"));

        LayoutEvents.UnsplitResidue repaired = purgeTimesOut(Map.of(1, reason(BasePurge.Reason.ONLY_PURGE_REPAIRED_TOMBSTONES, 5, "only_purge_repaired_tombstones is set"),
                                                                    2, reason(BasePurge.Reason.ONLY_PURGE_REPAIRED_TOMBSTONES, 5, "only_purge_repaired_tombstones is set")));
        assertTrue(repaired.remedy, repaired.remedy.contains("Run a repair"));

        LayoutEvents.UnsplitResidue pending = purgeTimesOut(Map.of(1, reason(BasePurge.Reason.WAITING_GC_GRACE, 5, "the key is in 1 pending repair sstables"),
                                                                   2, reason(BasePurge.Reason.WAITING_GC_GRACE, 5, "waiting")));
        assertTrue(pending.remedy, pending.remedy.contains("Run a repair") && pending.remedy.contains("clock"));

        long future = 4_102_444_800_000_000L;
        LayoutEvents.UnsplitResidue above = purgeTimesOut(Map.of(1, reason(BasePurge.Reason.RESIDUE_ABOVE_TT, future, "residue above the base tombstone"),
                                                                 2, reason(BasePurge.Reason.RESIDUE_ABOVE_TT, future - 1, "residue above the base tombstone")));
        assertEquals(future, above.largestResidueTimestamp);
        assertTrue(above.remedy, above.remedy.contains(Long.toString(future)) && above.remedy.contains("operator decision"));
        assertFalse(above.remedy, above.remedy.contains("Run a repair"));

        LayoutEvents.UnsplitResidue silent = purgeTimesOut(Map.of(1, reason(BasePurge.Reason.STILL_SHADOWED, 5, "not purged")));
        assertTrue(silent.nodes.get("2"), silent.nodes.get("2").startsWith("no purge reason published"));
        assertTrue(silent.remedy, silent.remedy.contains("published no reason"));
    }

    @Test
    public void beginUnsplitWithATombstoneIsAcceptedOnlyForACleanupKeyInResidue()
    {
        long now = cluster.now.get();
        steadyResidue(60_000);
        Claim claim = new Owner(coordinator1, node1, PartitionUnsplitActuator.MODE_SLOW).claimLane().claim();
        BeginUnsplit begin = new BeginUnsplit(claim.actionId, claim.owner, claim.step, LOGICAL, KEY, KeyState.ReverseMode.SLOW, null, now * 1000, now, 600_000);
        ClusterCoordinator.Outcome outcome = coordinator1.commitInstead(claim, begin.wrap(), Phase.REVERSE_ANNOUNCED.name(), claim.state);
        assertFalse(outcome.toString(), outcome.ok());
        assertEquals(Phase.STEADY, key(cluster.log).phase);
    }
}
