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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.detectors.SplitOutcomeDetectors;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.FakeCluster;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.Recent;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.ActionExecutor;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActuatorRegistry;
import org.apache.cassandra.autonomic.execute.ClusterActionExecution;
import org.apache.cassandra.autonomic.execute.ManualScheduler;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.FinishKey;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutEvents;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.LayoutRegistryValue;
import org.apache.cassandra.autonomic.layout.LayoutVerifier;
import org.apache.cassandra.autonomic.layout.NaturalReplicas;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.ReleaseKey;
import org.apache.cassandra.autonomic.layout.RemoveKey;
import org.apache.cassandra.autonomic.layout.ReportBackfill;
import org.apache.cassandra.autonomic.layout.ReportHints;
import org.apache.cassandra.autonomic.layout.SplitEvaluation;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
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
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.MembershipUtils;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.9: {@link PartitionSplitActuator} against hand-built metadata in a {@link FakeCluster} log, with the real layout
 * and ledger transformations and coordinator, a fake epoch watermark, fake backfill reports (committed as the replicas
 * would), a fake equivalence check and fake data writes. Time is the cluster's fake clock.
 *
 * <p>The driver checks on every commit it carries out that rule O1 held: a transformation that changes a key's phase
 * is committed only after the fake watermark answered true for the key's current phase epoch.
 *
 * <p>There is no shared contract class for cluster actuators (see {@code ScaleOutActuatorTest}); this is the phase
 * table of design 4.1, 4.2 and 4.9.
 */
public class PartitionSplitActuatorTest
{
    private static final String KS = "ks";
    private static final String TABLE = "t";
    private static final TableId LOGICAL = TableId.fromUUID(new UUID(10, 10));
    private static final ByteBuffer KEY = Int32Type.instance.decompose(42);
    private static final String KEY_HEX = ByteBufferUtil.bytesToHex(KEY);
    private static final long WRITE_TIMEOUT = 2000;
    private static final int MAX_STEPS = 2000;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    // ---------------------------------------------------------------------------------------------------------
    // fakes
    // ---------------------------------------------------------------------------------------------------------

    private FakeCluster cluster;
    private final AutonomicConfig config = new AutonomicConfig();
    private FakeEnv env;

    /** An equivalence check that ends after {@link #pages} steps with the given counts. */
    static final class FakeCheck implements PartitionSplitActuator.Check
    {
        int pages = 2;
        long contentDiffers;
        long missingFromOverflow;
        long missingFromBase;

        @Override
        public LayoutVerifier.Status step()
        {
            if (--pages > 0)
                return LayoutVerifier.Status.RUNNING;
            return contentDiffers + missingFromOverflow + missingFromBase == 0 ? LayoutVerifier.Status.PASSED : LayoutVerifier.Status.FAILED;
        }

        @Override
        public long nextDueMillis()
        {
            return 0;
        }

        @Override
        public long count(LayoutVerifier.Kind kind)
        {
            switch (kind)
            {
                case CONTENT_DIFFERS: return contentDiffers;
                case MISSING_FROM_OVERFLOW: return missingFromOverflow;
                default: return missingFromBase;
            }
        }

        @Override
        public boolean tooManyDifferences()
        {
            return false;
        }

        @Override
        public String summary()
        {
            return "fake: " + contentDiffers + '/' + missingFromOverflow + '/' + missingFromBase;
        }
    }

    final class FakeEnv implements PartitionSplitActuator.Env
    {
        /** False: some node does not answer the watermark request. */
        volatile boolean allAnswer = true;
        /** A node whose metadata stays at {@link #laggingAt}, the epoch it reports; null: every node has the log's. */
        NodeId lagging;
        Epoch laggingAt;
        /** Results of the next checks, in order; a passing check when empty. */
        final List<FakeCheck> checks = new ArrayList<>();
        int checksStarted;
        final List<String> writes = new ArrayList<>();
        boolean failDeletes;
        boolean failBase;

        @Override
        public ClusterMetadata metadata()
        {
            return cluster.log;
        }

        /** The epoch each directory member would report: the log's, or its own if it lags. */
        Epoch reported(NodeId node)
        {
            return node.equals(lagging) ? laggingAt : cluster.log.epoch;
        }

        /** True if every directory member has seen {@code epoch}, whatever the actuator asked. */
        boolean allAt(Epoch epoch)
        {
            for (NodeId node : cluster.log.directory.peerIds())
                if (epoch.isAfter(reported(node)))
                    return false;
            return true;
        }

        /** Rule O1 as EpochWatermarks answers it: every member reports an epoch at or after {@code epoch}. */
        @Override
        public boolean acknowledged(Epoch epoch)
        {
            return allAnswer && allAt(epoch);
        }

        @Override
        public void createTable(TableMetadata logical, UUID layoutId, TableId physicalId)
        {
            TableMetadata physical = TableMetadata.builder(KS, PhysicalTables.physicalName(logical.name, layoutId)).id(physicalId)
                                                  .addPartitionKeyColumn("p", Int32Type.instance)
                                                  .addPartitionKeyColumn(TableLayout.BUCKET_COLUMN, Int32Type.instance)
                                                  .addClusteringColumn("c", Int32Type.instance)
                                                  .addRegularColumn("v", UTF8Type.instance)
                                                  .gcGraceSeconds(logical.params.gcGraceSeconds)
                                                  .build();
            setSchema(schema().withAddedOrUpdated(keyspace().withSwapped(keyspace().tables.with(physical))));
            writes.add("create " + physical.name);
        }

        @Override
        public void dropTable(String keyspace, String table)
        {
            KeyspaceMetadata ks = keyspace();
            setSchema(schema().withAddedOrUpdated(ks.withSwapped(ks.tables.without(table))));
            writes.add("drop " + table);
        }

        @Override
        public void deleteBuckets(TableMetadata logical, TableMetadata physical, TableLayout layout, ByteBuffer key, int incarnation, long timestampMicros)
        {
            if (failDeletes)
                throw new IllegalStateException("an overflow replica is down");
            KeyState state = layout.keyState(key);
            writes.add("deleteBuckets " + incarnation + " in " + state.phase + " acknowledged=" + allAt(state.phaseEpoch));
        }

        @Override
        public void deleteBase(TableMetadata logical, ByteBuffer key, long timestampMicros)
        {
            if (failBase)
                throw new IllegalStateException("a base replica is down");
            KeyState state = key(cluster.log);
            writes.add("deleteBase in " + (state == null ? null : state.phase) + " acknowledged=" + (state != null && allAt(state.phaseEpoch)));
        }

        /** The placement of the metadata, as in production: what FinishKey checks too. */
        @Override
        public Set<NodeId> replicas(ClusterMetadata metadata, TableMetadata logical, ByteBuffer key)
        {
            return NaturalReplicas.of(metadata, logical, key);
        }

        @Override
        public Epoch replicasModified(ClusterMetadata metadata, TableMetadata logical, ByteBuffer key)
        {
            return NaturalReplicas.lastModified(metadata, logical, key);
        }

        @Override
        public PartitionSplitActuator.Check check(ClusterMetadata metadata, TableId logical, ByteBuffer key, int pageRows, long recheckDelayMillis, LongSupplier clock)
        {
            checksStarted++;
            return checks.isEmpty() ? new FakeCheck() : checks.remove(0);
        }

        @Override
        public long requestTimeoutsMillis()
        {
            return 3000;
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

        /** The signals of this node; null: stages A and B are not judged. */
        KnowledgeView knowledge;

        @Override
        public KnowledgeView knowledge()
        {
            return knowledge;
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

    /**
     * The whole ring of the keyspace on these nodes, modified at the epoch of this change: a change of the key's
     * replica group as the completion test and FinishKey see it.
     */
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

    private Set<NodeId> replicas()
    {
        return NaturalReplicas.of(cluster.log, keyspace().tables.getNullable(TABLE), KEY);
    }

    private static TableMetadata logicalTable(int gcGrace)
    {
        return TableMetadata.builder(KS, TABLE).id(LOGICAL)
                            .addPartitionKeyColumn("p", Int32Type.instance)
                            .addClusteringColumn("c", Int32Type.instance)
                            .addRegularColumn("v", UTF8Type.instance)
                            .params(TableParams.builder().gcGraceSeconds(gcGrace).build())
                            .build();
    }

    private static KeyState key(ClusterMetadata md)
    {
        TableLayout layout = LayoutRegistry.of(md).layout(LOGICAL);
        return layout == null ? null : layout.keyState(KEY);
    }

    private FakeCluster.Node node1;
    private FakeCluster.Node node2;
    private ClusterCoordinator coordinator1;
    private ClusterCoordinator coordinator2;

    @Before
    public void setUp()
    {
        cluster = new FakeCluster(3);
        node1 = cluster.node(1);
        node2 = cluster.node(2);
        setSchema(Keyspaces.of(KeyspaceMetadata.create(KS, KeyspaceParams.simple(2), Tables.of(logicalTable(864000)))));
        coordinator1 = new ClusterCoordinator(node1);
        coordinator2 = new ClusterCoordinator(node2);
        assertTrue(coordinator1.setEnabled(true));
        node2.catchUp();
        env = new FakeEnv();
        setReplicas(1, 2);
        AutonomicConfig.LayoutConfig lc = config.layout;
        lc.ack_poll_interval = new DurationSpec.IntMillisecondsBound("100ms");
        lc.cas_drain = new DurationSpec.IntMillisecondsBound("500ms");
        lc.cutover_soak = new DurationSpec.IntMillisecondsBound("1s");
        lc.straggler_window = new DurationSpec.LongMillisecondsBound("2s");
        lc.verify_recheck_delay = new DurationSpec.IntMillisecondsBound("100ms");
        lc.commit_approval = "auto";
        lc.baseline_window = new DurationSpec.IntMillisecondsBound("1s");
    }

    // ---------------------------------------------------------------------------------------------------------
    // driver: what ClusterActionExecution does with a result, against the fake log
    // ---------------------------------------------------------------------------------------------------------

    enum Entry { STEP, RECOVER, ABORT }

    final class Owner
    {
        final PartitionSplitActuator actuator = new PartitionSplitActuator(env, () -> config);
        final ClusterCoordinator coordinator;
        final FakeCluster.Node node;
        final Action action;
        UUID id;
        Entry entry = Entry.STEP;
        StepResult last;
        final List<String> phases = new ArrayList<>();
        boolean autoReport = true;
        /** False: replicas do not report the reconcile generation of CLEANUP. */
        boolean reportReconcile = true;
        boolean autoApprove;

        Owner(ClusterCoordinator coordinator, FakeCluster.Node node, Map<String, String> params)
        {
            this.coordinator = coordinator;
            this.node = node;
            this.action = new Action(cluster.newActionId(), PartitionSplitActuator.NAME, ActionScope.CLUSTER,
                                     PartitionSplitActuator.target(KS, TABLE, KEY_HEX), params, null);
            this.id = action.id;
        }

        Owner(ClusterCoordinator coordinator, FakeCluster.Node node)
        {
            this(coordinator, node, PartitionSplitActuator.params(4, PartitionSplitActuator.MODE_SPLIT));
        }

        Owner claimLane()
        {
            ClusterCoordinator.Outcome outcome = coordinator.claim(Lane.SCHEMA_LAYOUT, id, PartitionSplitActuator.NAME, action.target.asString(),
                                                                   ClusterActionExecution.INITIAL_PHASE, Collections.emptyMap());
            assertTrue(outcome.toString(), outcome.ok());
            return this;
        }

        /**
         * The takeover of the expired claim of {@code previous} by the periodic tick of a {@link ClusterActionExecution}
         * on this owner's node (overview 6.4), with the node of {@code previous} down so that this node is the
         * preferred proposer. The recovery is then driven here, step by step, with the O1 check of every commit.
         */
        Owner takeOverExpired(Owner previous)
        {
            catchUp();
            Claim expired = coordinator.ledger().claim(previous.id);
            assertNotNull(expired);
            cluster.now.set(expired.leaseExpiresAtMillis + 1);
            node.down.add(previous.node.self);
            ActuatorRegistry registry = new ActuatorRegistry();
            registry.register(actuator);
            registry.setEnabled(PartitionSplitActuator.NAME, true);
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
            assertEquals(node.self, taken.owner);
            assertFalse(taken.actionId.equals(previous.id));
            assertEquals(expired.state, taken.state);
            assertEquals(Collections.singleton(taken.actionId), execution.running());
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

        /** One step and its ledger commit. */
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
                    break;
                default:
                    assertTrue(coordinator.release(claim, finalState(r)).ok());
            }
            KeyState state = key(cluster.log);
            String phase = state == null ? "absent" : state.phase + "/" + state.generation;
            if (phases.isEmpty() || !phases.get(phases.size() - 1).equals(phase))
                phases.add(phase);
            reportHints();
            KeyState now = key(cluster.log);
            if (autoReport && (reportReconcile || now == null || now.phase != Phase.CLEANUP))
                report();
            if (autoApprove && claim() != null && Phase.CUTOVER.name().equals(claim().phase))
                assertTrue(PartitionSplitActuator.approveCommit(coordinator, id));
            return r;
        }

        /** Steps until the condition holds, or the action ends. */
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

        Owner reach(Phase phase, boolean reconcileStarted) throws Exception
        {
            until(o -> {
                KeyState k = key(cluster.log);
                return k != null && k.phase == phase && (phase != Phase.CLEANUP || k.backfillDue() == reconcileStarted)
                       && claim() != null && phase.name().equals(claim().phase);
            });
            assertEquals(phase, key(cluster.log).phase);
            return this;
        }

        Owner reach(Phase phase) throws Exception
        {
            return reach(phase, false);
        }
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

    /** Rule O1, checked on every commit that changes a key's phase or generation, or removes or finishes it. */
    private void checkO1(Transformation t)
    {
        Transformation child = t instanceof CustomTransformation ? ((CustomTransformation) t).child() : t;
        if (!(child instanceof AdvanceLayout || child instanceof RemoveKey || child instanceof FinishKey))
            return;
        KeyState state = key(cluster.log);
        assertNotNull(state);
        assertTrue(child + " committed before every node had epoch " + state.phaseEpoch.getEpoch() + " of " + state.phase,
                   env.allAt(state.phaseEpoch));
    }

    /** Directory members that hold hints from before the split: they make no hint report (ADR-035). */
    private final Set<NodeId> holdHints = new HashSet<>();

    /** In ANNOUNCED every directory member not in {@link #holdHints} reports its hints, as HintReport does. */
    private void reportHints()
    {
        KeyState state = key(cluster.log);
        if (state != null && state.phase == Phase.ANNOUNCED)
        {
            for (NodeId node : cluster.log.directory.peerIds())
                if (!holdHints.contains(node) && !state.hintsClear.contains(node))
                {
                    cluster.log = new ReportHints(LOGICAL, KEY, state.phaseEpoch, node).wrap().execute(cluster.log).success().metadata;
                    state = key(cluster.log);
                }
            catchUp();
        }
    }

    /** The replicas commit their reports, as BackfillTask does, for the generation that is due. */
    private void report()
    {
        KeyState state = key(cluster.log);
        if (state == null || !state.backfillDue())
            return;
        for (NodeId node : replicas())
            if (!state.backfillDone.contains(node))
            {
                Transformation.Result result = new ReportBackfill(LOGICAL, KEY, state.generation, state.generationEpoch, node).wrap().execute(cluster.log);
                assertTrue(String.valueOf(result), result.isSuccess());
                cluster.log = result.success().metadata;
                state = key(cluster.log);
            }
        catchUp();
    }

    private Owner owner()
    {
        return new Owner(coordinator1, node1).claimLane();
    }

    private Recent released(UUID id)
    {
        return ClusterLedger.of(cluster.log).recent(id);
    }

    private void assertRemovedAndDropped()
    {
        assertNull("the key is removed", key(cluster.log));
        assertTrue("no layout is left", LayoutRegistry.of(cluster.log).byLogical.isEmpty());
        assertEquals("only the logical table is left", 1, keyspace().tables.size());
        assertNotNull(keyspace().tables.getNullable(TABLE));
    }

    private static final Set<Phase> PRE_COMMIT = EnumSet.of(Phase.ANNOUNCED, Phase.DUAL_WRITE, Phase.BACKFILL, Phase.VERIFY, Phase.CUTOVER);

    // ---------------------------------------------------------------------------------------------------------
    // the forward path
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void everyForwardTransitionInOrder() throws Exception
    {
        Owner owner = owner();
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.COMMITTED, end.kind);
        assertEquals(List.of("absent", "ANNOUNCED/1", "DUAL_WRITE/1", "BACKFILL/1", "VERIFY/1", "CUTOVER/1", "NEW_ONLY/1", "CLEANUP/1", "CLEANUP/2", "STEADY/2"),
                     owner.phases);
        KeyState state = key(cluster.log);
        assertEquals(Phase.STEADY, state.phase);
        assertNull(state.actionId);
        assertTrue(state.dualWriteSinceMillis > 0);
        // the overflow table was created, never dropped; no bucket was deleted; and (ADR-036) the base partition was not
        // deleted: STEADY releases the key with its residue kept
        assertEquals(env.writes.toString(), 1, env.writes.size());
        assertTrue(env.writes.get(0), env.writes.get(0).startsWith("create "));
        assertEquals(KeyState.BaseState.RESIDUE, state.baseState);
        assertEquals(LayoutRegistry.FORMAT_VERSION_2, LayoutRegistry.of(cluster.log).formatVersion);
        assertEquals(1, env.checksStarted);
        assertEquals(ClusterActionExecution.FINAL_COMMITTED, released(owner.id).finalState);
        assertTrue(end.detail, end.detail.contains("missing_from_base=0"));
    }

    @Test
    public void announcedIsHeldForTheDrainOfAdr028AfterEveryNodeAcknowledged() throws Exception
    {
        Owner owner = owner().reach(Phase.ANNOUNCED);
        env.allAnswer = false;
        long until = cluster.now.get() + 60_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals("nothing moves while a node does not answer", Phase.ANNOUNCED, key(cluster.log).phase);
        env.allAnswer = true;
        long acknowledgedAt = cluster.now.get();
        owner.reach(Phase.DUAL_WRITE);
        // the drain is the largest request timeout (3s here, above cas_drain) plus the margin, counted from the acknowledgement
        long drain = 3000 + PartitionSplitActuator.DRAIN_MARGIN_MILLIS;
        assertTrue("DUAL_WRITE at " + key(cluster.log).phaseSinceMillis + ", acknowledged at " + acknowledgedAt,
                   key(cluster.log).phaseSinceMillis >= acknowledgedAt + drain);
    }

    @Test
    public void announcedWaitsForEveryNodesHintReportAlongsideTheDrain() throws Exception
    {
        // ADR-035
        holdHints.add(cluster.id(3));
        Owner owner = owner().reach(Phase.ANNOUNCED);
        long until = cluster.now.get() + 60_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals("node 3 has not reported: the drain is long over, the key stays", Phase.ANNOUNCED, key(cluster.log).phase);
        assertEquals(ImmutableSet.of(cluster.id(1), cluster.id(2)), key(cluster.log).hintsClear);
        String status = PartitionSplitActuator.status(cluster.log, cluster.now.get(), config.layout, WRITE_TIMEOUT).get(0).get("status");
        assertTrue(status, status.contains("waits for hint reports (ADR-035), 2 of 3 nodes reported"));
        holdHints.clear();
        owner.reach(Phase.DUAL_WRITE);
        assertTrue("the next phase clears the reports", key(cluster.log).hintsClear.isEmpty());
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    @Test
    public void announcedAbortsWhenHintsAreNotDeliveredInHintDrainTimeout() throws Exception
    {
        config.layout.hint_drain_timeout = new DurationSpec.LongMillisecondsBound("2m");
        holdHints.add(cluster.id(3));
        Owner owner = owner().reach(Phase.ANNOUNCED);
        owner.until(o -> o.claim() != null && o.claim().state.containsKey(PartitionSplitActuator.S_ABORT));
        long acked = Long.parseLong(owner.claim().state.get(PartitionSplitActuator.S_ACKED).replaceAll(".*/", ""));
        assertTrue("not before the timeout", cluster.now.get() >= acked + 120_000);
        String why = owner.claim().state.get(PartitionSplitActuator.S_ABORT);
        assertTrue(why, why.startsWith(PartitionSplitActuator.HINTS_NOT_DRAINED + ": hint_drain_timeout 2m passed in ANNOUNCED; node(s) "
                                       + cluster.id(3).id() + " ("));
        assertTrue(why, why.contains("still hold hints created before the split"));
        assertFalse("nodes that reported are not named", why.contains("node(s) " + cluster.id(1).id() + ' '));
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(owner.phases.toString(), owner.phases.contains("ABORTING/1"));
        assertFalse(owner.phases.toString(), owner.phases.contains("DUAL_WRITE/1"));
        assertRemovedAndDropped();
    }

    /** Node 3 has every epoch before the current phase epoch and not that one: it is one phase behind. */
    private void lagOnePhaseBehind()
    {
        env.lagging = cluster.id(3);
        env.laggingAt = Epoch.create(key(cluster.log).phaseEpoch.getEpoch() - 1);
    }

    /** Steps for 30 s of fake time: the key keeps its phase, epoch and generation, and nothing but claim sub-steps is committed. */
    private void assertHeld(Owner owner, String what) throws Exception
    {
        int before = cluster.applied.size();
        KeyState held = key(cluster.log);
        int writes = env.writes.size();
        long until = cluster.now.get() + 30_000;
        owner.until(o -> cluster.now.get() > until);
        KeyState now = key(cluster.log);
        assertNotNull(what, now);
        assertEquals(what, held.phase, now.phase);
        assertEquals(what, held.phaseEpoch, now.phaseEpoch);
        assertEquals(what, held.generation, now.generation);
        assertEquals(what, held.actionId, now.actionId);
        for (Transformation t : cluster.applied.subList(before, cluster.applied.size()))
        {
            Transformation child = t instanceof CustomTransformation ? ((CustomTransformation) t).child() : t;
            assertFalse(what + ": " + child, child instanceof AdvanceLayout || child instanceof RemoveKey || child instanceof FinishKey
                                             || child instanceof ReleaseKey);
        }
        assertEquals(what + ": no data was written or deleted " + env.writes, writes, env.writes.size());
    }

    @Test
    public void noTransitionWithoutFullAcknowledgement() throws Exception
    {
        // in every phase that waits for O1, a node one phase behind holds the key where it is: waiting on any earlier
        // epoch (the generation's, the previous phase's) would let it through
        String[] points = { "ANNOUNCED", "DUAL_WRITE", "BACKFILL", "VERIFY", "CUTOVER", "NEW_ONLY", "CLEANUP", "CLEANUP_RECONCILE", "STEADY" };
        for (String point : points)
        {
            setUp();
            Owner owner = owner();
            switch (point)
            {
                case "CLEANUP_RECONCILE":
                    owner.reach(Phase.CLEANUP, true);
                    break;
                case "STEADY":
                    owner.until(o -> o.claim() != null && Phase.STEADY.name().equals(o.claim().phase));
                    break;
                default:
                    owner.reach(Phase.valueOf(point));
            }
            lagOnePhaseBehind();
            assertHeld(owner, point);
            env.lagging = null;
            assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
        }
    }

    @Test
    public void rollbackDualAndAbortingHoldWithoutAcknowledgement() throws Exception
    {
        for (Phase phase : new Phase[]{ Phase.ROLLBACK_DUAL, Phase.ABORTING })
        {
            setUp();
            Owner owner = owner().reach(Phase.CUTOVER);
            PartitionSplitActuator.requestAbort(coordinator1, () -> cluster.log, PartitionSplitActuator.target(KS, TABLE, KEY_HEX), "test");
            owner.until(o -> key(cluster.log).phase == phase && o.claim() != null && phase.name().equals(o.claim().phase));
            lagOnePhaseBehind();
            // in ABORTING the bucket partitions are not deleted either: a coordinator one phase behind still reads them
            assertHeld(owner, phase.name());
            env.lagging = null;
            assertEquals(StepResult.Kind.ROLLED_BACK, owner.toEnd().kind);
            assertRemovedAndDropped();
        }
    }

    @Test
    public void commitPointWaitsForTheSecondApproval() throws Exception
    {
        config.layout.commit_approval = "manual";
        Owner owner = owner().reach(Phase.CUTOVER);
        long until = cluster.now.get() + 20_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals("the soak is over, the approval is missing", Phase.CUTOVER, key(cluster.log).phase);
        assertFalse(PartitionSplitActuator.approveCommit(coordinator1, UUID.randomUUID()));
        assertTrue(PartitionSplitActuator.approveCommit(coordinator2, owner.id));
        owner.reach(Phase.NEW_ONLY);
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    @Test
    public void cutoverSoaksAfterAcknowledgement() throws Exception
    {
        Owner owner = owner().reach(Phase.CUTOVER);
        long entered = key(cluster.log).phaseSinceMillis;
        owner.reach(Phase.NEW_ONLY);
        assertTrue(key(cluster.log).phaseSinceMillis >= entered + config.layout.cutover_soak.toMilliseconds());
    }

    // ---------------------------------------------------------------------------------------------------------
    // backfill completion, topology
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void backfillWaitsForEveryNaturalReplicaOfTheCurrentGeneration() throws Exception
    {
        Owner owner = owner();
        owner.autoReport = false;
        owner.reach(Phase.BACKFILL);
        KeyState state = key(cluster.log);
        cluster.log = new ReportBackfill(LOGICAL, KEY, 1, state.generationEpoch, cluster.id(1)).wrap().execute(cluster.log).success().metadata;
        catchUp();
        long until = cluster.now.get() + 30_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals("one of two replicas reported", Phase.BACKFILL, key(cluster.log).phase);
        // a third replica appears in the placement: the generation restarts and every replica must report it again
        setReplicas(1, 2, 3);
        owner.until(o -> key(cluster.log).generation == 2);
        assertTrue(key(cluster.log).backfillDone.isEmpty());
        owner.autoReport = true;
        owner.reach(Phase.VERIFY);
        assertEquals(ImmutableSet.of(cluster.id(1), cluster.id(2), cluster.id(3)), key(cluster.log).backfillDone);
        assertEquals(2, key(cluster.log).generation);
    }

    @Test
    public void topologyChangeInProgressHoldsEveryForwardPhase() throws Exception
    {
        Owner owner = owner().reach(Phase.DUAL_WRITE);
        ClusterMetadata md = cluster.log;
        cluster.log = md.transformer().with(md.inProgressSequences.with(new NodeId(77), Mockito.mock(MultiStepOperation.class))).build().metadata;
        catchUp();
        long until = cluster.now.get() + 60_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals(Phase.DUAL_WRITE, key(cluster.log).phase);
        md = cluster.log;
        cluster.log = md.transformer().with(md.inProgressSequences.without(new NodeId(77))).build().metadata;
        catchUp();
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    @Test
    public void placementChangeInVerifyAndCleanupRestartsTheGeneration() throws Exception
    {
        Owner owner = owner().reach(Phase.VERIFY);
        setReplicas(1, 2);
        owner.until(o -> key(cluster.log).phase == Phase.BACKFILL);
        assertEquals(2, key(cluster.log).generation);
        owner.reportReconcile = false;
        owner.reach(Phase.CLEANUP, true);
        int reconcile = key(cluster.log).generation;
        // a replica group change committed after the reconcile generation started
        setReplicas(1, 2);
        owner.until(o -> key(cluster.log).generation == reconcile + 1);
        assertEquals(Phase.CLEANUP, key(cluster.log).phase);
        owner.reportReconcile = true;
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    // ---------------------------------------------------------------------------------------------------------
    // verification (ADR-030)
    // ---------------------------------------------------------------------------------------------------------

    // ---------------------------------------------------------------------------------------------------------
    // stage A and stage B (T6.11, design 7.3)
    // ---------------------------------------------------------------------------------------------------------

    private ObservationStore signals;

    private void withKnowledge()
    {
        signals = new ObservationStore(64, 64);
        env.knowledge = new InMemoryKnowledgeView(signals, cluster.now::get);
    }

    private void observe(SignalId signal, Subject subject, long time, double value)
    {
        signals.accept(new Observation(signal, subject, time, value));
    }

    private static Subject keySubject()
    {
        return Subject.partition(KS, TABLE, KEY_HEX);
    }

    /** What the key looked like when the policy proposed the split: 1000us reads, 500us writes, busiest partition 60% of the table. */
    private static Map<String, String> proposedWithProfile()
    {
        Map<String, String> params = PartitionSplitActuator.params(4, PartitionSplitActuator.MODE_SPLIT);
        params.putAll(new SplitEvaluation.Baseline(1000, 500, 0.0, 1000, 1000, 0.6, false).toParams());
        return params;
    }

    private Owner ownerWithProfile()
    {
        return new Owner(coordinator1, node1, proposedWithProfile()).claimLane();
    }

    /** A proposed split whose key had the baseline's numbers in ANNOUNCED: the baseline of design 7.3 is taken there. */
    private Owner profiled() throws Exception
    {
        Owner owner = ownerWithProfile().reach(Phase.ANNOUNCED);
        keySignals(1000, 500, 0.0);
        return owner;
    }

    /** Key signals as {@code LayoutSensor} emits them, 300 and 400 ms from now: inside CUTOVER's soak, or the last 10 minutes of STEADY. */
    private void keySignals(double readMicros, double writeMicros, double errorRate)
    {
        long t = cluster.now.get();
        for (long at : new long[]{ t + 300, t + 400 })
        {
            observe(Signals.KEY_READ_LATENCY_MICROS, keySubject(), at, readMicros);
            observe(Signals.KEY_READ_OPS_DELTA, keySubject(), at, 300);
            observe(Signals.KEY_WRITE_LATENCY_MICROS, keySubject(), at, writeMicros);
            observe(Signals.KEY_WRITE_OPS_DELTA, keySubject(), at, 300);
            observe(Signals.KEY_ERROR_RATE, keySubject(), at, errorRate);
        }
    }

    private void overflowShare(double share)
    {
        TableLayout layout = LayoutRegistry.of(cluster.log).layout(LOGICAL);
        observe(Signals.PARTITION_READ_SHARE, Subject.partition(KS, layout.physicalName, "0001"), cluster.now.get(), share);
    }

    @Test
    public void stageAFailureRollsBackThroughRollbackDual() throws Exception
    {
        withKnowledge();
        Owner owner = profiled().reach(Phase.CUTOVER);
        keySignals(2100, 500, 0.0);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(owner.phases.toString(), owner.phases.containsAll(List.of("CUTOVER/1", "ROLLBACK_DUAL/1", "ABORTING/1")));
        assertFalse(owner.phases.toString(), owner.phases.contains("NEW_ONLY/1"));
        assertTrue(end.detail, end.detail.contains("stage A failed") && end.detail.contains("read latency"));
        assertRemovedAndDropped();
    }

    @Test
    public void stageAFailureOnTheErrorRateRollsBack() throws Exception
    {
        withKnowledge();
        Owner owner = profiled().reach(Phase.CUTOVER);
        keySignals(1000, 500, 0.05);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains("stage A failed") && end.detail.contains("error rate"));
    }

    @Test
    public void stageAWithinToleranceCommitsAndStageBJudgesTheGoal() throws Exception
    {
        withKnowledge();
        Owner owner = profiled().reach(Phase.CUTOVER);
        keySignals(1900, 500, 0.0);
        owner.reach(Phase.CLEANUP);
        overflowShare(0.2);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.COMMITTED, end.kind);
        assertEquals(Verdict.IMPROVED, end.verdict);
        assertTrue(end.detail, end.detail.contains("stage B IMPROVED") && end.detail.contains("0.600 -> 0.200"));
        assertEquals(Phase.STEADY, key(cluster.log).phase);
    }

    @Test
    public void stageBVerdictsAreRecordedAndTheKeyStaysSplit() throws Exception
    {
        // goal missed: the busiest bucket still has 40% of the table: NEUTRAL
        withKnowledge();
        Owner neutral = profiled().reach(Phase.CLEANUP);
        keySignals(1000, 500, 0.0);
        overflowShare(0.4);
        StepResult end = neutral.toEnd();
        assertEquals(end.toString(), Verdict.NEUTRAL, end.verdict);
        assertEquals(Phase.STEADY, key(cluster.log).phase);
    }

    @Test
    public void aBreachedStageBGuardIsRegressed() throws Exception
    {
        withKnowledge();
        Owner owner = profiled().reach(Phase.CUTOVER);
        // fine during the soak, worse than 25% over the baseline's write latency afterwards
        keySignals(1000, 500, 0.0);
        owner.reach(Phase.CLEANUP);
        cluster.now.addAndGet(1000);
        keySignals(1000, 900, 0.0);
        overflowShare(0.1);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), Verdict.REGRESSED, end.verdict);
        assertTrue(end.detail, end.detail.contains("write latency") && end.detail.contains("a guard was breached"));
        assertEquals("the key stays split", Phase.STEADY, key(cluster.log).phase);
    }

    @Test
    public void withoutAProfileTheStagesAreNotJudgedAndNothingIsRolledBack() throws Exception
    {
        // an operator's split carries no baseline; the signals may be anything
        withKnowledge();
        Owner owner = owner().reach(Phase.CUTOVER);
        keySignals(99999, 99999, 1.0);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.COMMITTED, end.kind);
        assertEquals(Verdict.INCONCLUSIVE, end.verdict);
        assertTrue(end.detail, end.detail.contains("no baseline"));
    }

    @Test
    public void aProposedSplitWithTooFewOperationsInAnnouncedAbortsWithInsufficientBaseline() throws Exception
    {
        withKnowledge();
        Owner owner = ownerWithProfile();
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains(SplitEvaluation.INSUFFICIENT_BASELINE));
        assertEquals(List.of("absent", "ANNOUNCED/1", "ABORTING/1", "absent"), owner.phases);
        assertRemovedAndDropped();
    }

    @Test
    public void theAnnouncedBaselineIsTakenOverTheBaselineWindow() throws Exception
    {
        withKnowledge();
        Owner owner = ownerWithProfile().reach(Phase.ANNOUNCED);
        long registered = key(cluster.log).phaseSinceMillis;
        keySignals(800, 400, 0.0);
        observe(Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS, Subject.table(KS, TABLE), cluster.now.get() + 200, 3000);
        owner.reach(Phase.DUAL_WRITE);
        assertTrue("held for the baseline window", key(cluster.log).phaseSinceMillis >= registered + config.layout.baseline_window.toMilliseconds());
        Map<String, String> state = owner.claim().state;
        SplitEvaluation.Baseline baseline = SplitEvaluation.Baseline.fromMap(state);
        assertEquals("the key's latency in ANNOUNCED, not the profile's", 800, baseline.readMicros, 0.0);
        assertEquals(400, baseline.writeMicros, 0.0);
        assertEquals(3000, baseline.tableWriteMicros, 0.0);
        assertEquals("the goal stays the profile's", 0.6, baseline.opsShare, 0.0);
        assertFalse(PartitionSplitActuator.NO_BASELINE.equals(state.get(PartitionSplitActuator.S_BASELINE_AT)));
        // an operator's split is not judged and is not held for it
        setUp();
        withKnowledge();
        Owner operator = owner().reach(Phase.DUAL_WRITE);
        assertEquals(PartitionSplitActuator.NO_BASELINE, operator.claim().state.get(PartitionSplitActuator.S_BASELINE_AT));
        assertNull(SplitEvaluation.Baseline.fromMap(operator.claim().state));
    }

    @Test
    public void theC1GuardAbortsOnTheKeysErrorRate() throws Exception
    {
        withKnowledge();
        Owner owner = profiled();
        owner.autoReport = false;
        owner.reach(Phase.BACKFILL);
        keySignals(1000, 500, 0.05);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains("C1 guard failed in BACKFILL") && end.detail.contains("error rate"));
        assertFalse(owner.phases.toString(), owner.phases.contains("CUTOVER/1"));
        assertRemovedAndDropped();
    }

    @Test
    public void theC1GuardAbortsOnTheTablesWriteLatencyAndOnNewSaturation() throws Exception
    {
        withKnowledge();
        Owner owner = ownerWithProfile().reach(Phase.ANNOUNCED);
        keySignals(1000, 500, 0.0);
        observe(Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS, Subject.table(KS, TABLE), cluster.now.get() + 200, 2000);
        owner.reach(Phase.DUAL_WRITE);
        observe(Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS, Subject.table(KS, TABLE), cluster.now.get(), 6500);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains("table write latency"));

        setUp();
        withKnowledge();
        owner = profiled();
        owner.autoReport = false;
        owner.reach(Phase.BACKFILL);
        long dualWrite = key(cluster.log).dualWriteSinceMillis;
        ((InMemoryKnowledgeView) env.knowledge).setActiveDiagnoses(List.of(
            new Diagnosis(UUID.randomUUID(), "pool_saturation", Subject.node(), Severity.CRITICAL, 1, dualWrite - 1, dualWrite, "old", Map.of(), null)));
        long until = cluster.now.get() + 5_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals("a saturation that was open before dual write is not new", Phase.BACKFILL, key(cluster.log).phase);
        ((InMemoryKnowledgeView) env.knowledge).setActiveDiagnoses(List.of(
            new Diagnosis(UUID.randomUUID(), "pool_saturation", Subject.node(), Severity.CRITICAL, 1, dualWrite + 1, dualWrite + 1, "new", Map.of(), null)));
        end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains("new CRITICAL saturation diagnosis [pool_saturation]"));
    }

    @Test
    public void stageBLooksAtSamplesAfterTheCommitPointOnly() throws Exception
    {
        withKnowledge();
        Owner owner = profiled().reach(Phase.CUTOVER);
        // slow writes during the soak (stage A judges reads and errors only); stage B must not see them
        keySignals(1000, 5000, 0.0);
        owner.reach(Phase.CLEANUP);
        assertNotNull(owner.claim().state.get(PartitionSplitActuator.S_COMMITTED_AT));
        cluster.now.addAndGet(1000);
        keySignals(1000, 500, 0.0);
        overflowShare(0.1);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), Verdict.IMPROVED, end.verdict);
        assertTrue(end.detail, end.detail.contains("write latency ok"));
    }

    @Test
    public void aNodeWithoutKnowledgeJudgesNothing() throws Exception
    {
        env.knowledge = null;
        StepResult end = ownerWithProfile().toEnd();
        assertEquals(end.toString(), StepResult.Kind.COMMITTED, end.kind);
        assertEquals(Verdict.INCONCLUSIVE, end.verdict);
    }

    @Test
    public void aBaselineIsKeptInTheClaimAcrossAnOwnerChange() throws Exception
    {
        Owner owner = ownerWithProfile().reach(Phase.BACKFILL);
        Map<String, String> state = owner.claim().state;
        SplitEvaluation.Baseline baseline = SplitEvaluation.Baseline.fromMap(state);
        assertNotNull(baseline);
        assertEquals(1000, baseline.readMicros, 0.0);
        assertEquals(0.6, baseline.opsShare, 0.0);
        assertEquals("the baseline is one entry of the claim state", 1, state.keySet().stream().filter(k -> k.startsWith(SplitEvaluation.P_PREFIX) || k.equals(SplitEvaluation.S_BASELINE)).count());
    }

    /** A claim holds 16 entries: a proposed split that fails one verification and is size-only still fits to the end. */
    @Test
    public void theClaimStateStaysWithinItsBoundOnTheLongestPath() throws Exception
    {
        Map<String, String> params = PartitionSplitActuator.params(4, PartitionSplitActuator.MODE_SPLIT);
        params.putAll(new SplitEvaluation.Baseline(1000, 500, 0.0, 1000, 1000, 0.6, true).toParams());
        FakeCheck failed = new FakeCheck();
        failed.missingFromOverflow = 1;
        env.checks.add(failed);
        config.layout.commit_approval = "manual";
        Owner owner = new Owner(coordinator1, node1, params).claimLane();
        owner.autoApprove = true;
        int most = 0;
        for (int i = 0; i < MAX_STEPS && (owner.last == null || !owner.last.isTerminal()); i++)
        {
            owner.step();
            if (owner.claim() != null)
                most = Math.max(most, owner.claim().state.size());
        }
        assertEquals(StepResult.Kind.COMMITTED, owner.last.kind);
        assertTrue("at most " + most + " entries", most <= Claim.MAX_STATE_ENTRIES);
    }

    @Test
    public void anAbortedVerificationIsReportedForTheDiagnosis() throws Exception
    {
        LayoutEvents.clear();
        FakeCheck first = new FakeCheck();
        first.contentDiffers = 1;
        FakeCheck second = new FakeCheck();
        second.contentDiffers = 1;
        env.checks.add(first);
        env.checks.add(second);
        Owner owner = owner();
        StepResult end = owner.toEnd();
        assertEquals(StepResult.Kind.ROLLED_BACK, end.kind);
        List<LayoutEvents.VerifyFailure> failures = LayoutEvents.verifyFailures();
        assertEquals(1, failures.size());
        assertEquals(owner.id, failures.get(0).actionId);
        assertEquals(KEY_HEX, failures.get(0).keyHex);
        assertTrue(failures.get(0).detail, failures.get(0).detail.contains("fake"));
        LayoutEvents.clear();
    }

    @Test
    public void failedVerificationRestartsTheBackfillThenAborts() throws Exception
    {
        FakeCheck first = new FakeCheck();
        first.missingFromOverflow = 1;
        FakeCheck second = new FakeCheck();
        second.contentDiffers = 2;
        env.checks.add(first);
        env.checks.add(second);
        Owner owner = owner();
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(owner.phases.toString(), owner.phases.containsAll(List.of("VERIFY/1", "BACKFILL/2", "VERIFY/2", "ABORTING/2")));
        assertFalse(owner.phases.toString(), owner.phases.contains("CUTOVER/2"));
        assertTrue(end.detail, end.detail.contains("verification failed 2 times"));
        assertRemovedAndDropped();
    }

    @Test
    public void rowsMissingFromTheBaseAreToleratedAndCounted() throws Exception
    {
        FakeCheck check = new FakeCheck();
        check.missingFromBase = 3;
        env.checks.add(check);
        Owner owner = owner();
        StepResult end = owner.toEnd();
        assertEquals(StepResult.Kind.COMMITTED, end.kind);
        assertEquals(1, env.checksStarted);
        assertTrue(end.detail, end.detail.contains("missing_from_base=3"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // abort (design 4.9)
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void operatorAbortFromEachPreCommitPhase() throws Exception
    {
        for (Phase phase : PRE_COMMIT)
        {
            setUp();
            Owner owner = owner().reach(phase);
            int incarnation = key(cluster.log).incarnation;
            String done = PartitionSplitActuator.requestAbort(coordinator2, () -> cluster.log, PartitionSplitActuator.target(KS, TABLE, KEY_HEX), "test");
            assertTrue(done, done.contains("requested"));
            StepResult end = owner.toEnd();
            assertEquals(phase + ": " + end, StepResult.Kind.ROLLED_BACK, end.kind);
            List<String> expected = new ArrayList<>(List.of(phase + "/1"));
            if (phase == Phase.CUTOVER)
                expected.add("ROLLBACK_DUAL/1");
            expected.add("ABORTING/1");
            expected.add("absent");
            assertEquals(phase.name(), expected, owner.phases.subList(owner.phases.indexOf(phase + "/1"), owner.phases.size()));
            // the buckets were deleted in ABORTING once every node acknowledged it, before RemoveKey; then the drop
            List<String> writes = env.writes.subList(1, env.writes.size());
            assertEquals(phase.name(), 2, writes.size());
            assertEquals("deleteBuckets " + incarnation + " in ABORTING acknowledged=true", writes.get(0));
            assertTrue(writes.get(1), writes.get(1).startsWith("drop "));
            assertRemovedAndDropped();
            assertFalse("the base copy is never deleted by an abort", env.writes.toString().contains("deleteBase"));
        }
    }

    @Test
    public void killBeforeTheCommitPointAbortsAndRestartAborts() throws Exception
    {
        for (Phase phase : PRE_COMMIT)
        {
            for (Entry entry : new Entry[]{ Entry.ABORT, Entry.RECOVER })
            {
                setUp();
                Owner owner = owner().reach(phase);
                assertFalse(owner.actuator.pastCommitPoint(owner.claim()));
                owner.entry = entry;
                StepResult end = owner.toEnd();
                assertEquals(phase + " " + entry + ": " + end, StepResult.Kind.ROLLED_BACK, end.kind);
                assertRemovedAndDropped();
            }
        }
    }

    @Test
    public void bucketDeletionIsBestEffort() throws Exception
    {
        Owner owner = owner().reach(Phase.BACKFILL);
        env.failDeletes = true;
        PartitionSplitActuator.requestAbort(coordinator1, () -> cluster.log, PartitionSplitActuator.target(KS, TABLE, KEY_HEX), "test");
        assertEquals(StepResult.Kind.ROLLED_BACK, owner.toEnd().kind);
        assertRemovedAndDropped();
    }

    @Test
    public void abortBeforeRegistrationDropsTheTableItCreated() throws Exception
    {
        Owner owner = owner();
        owner.until(o -> o.claim() != null && PartitionSplitActuator.CREATING.equals(o.claim().phase) && keyspace().tables.size() == 2);
        owner.entry = Entry.ABORT;
        assertEquals(StepResult.Kind.ROLLED_BACK, owner.toEnd().kind);
        assertRemovedAndDropped();
    }

    @Test
    public void noAbortAfterTheCommitPoint() throws Exception
    {
        for (Phase phase : new Phase[]{ Phase.NEW_ONLY, Phase.CLEANUP })
        {
            setUp();
            Owner owner = owner().reach(phase);
            try
            {
                PartitionSplitActuator.requestAbort(coordinator2, () -> cluster.log, PartitionSplitActuator.target(KS, TABLE, KEY_HEX), "test");
                fail("abort past the commit point must be refused");
            }
            catch (IllegalStateException e)
            {
                assertTrue(e.getMessage(), e.getMessage().contains("commit point") && e.getMessage().contains("unsplit"));
            }
            // a kill, shutdown or restart there releases the claim without undoing anything (FC5)
            assertTrue(owner.actuator.pastCommitPoint(owner.claim()));
            owner.entry = Entry.ABORT;
            StepResult end = owner.step();
            assertEquals(StepResult.Kind.COMMITTED_CLEANUP_PENDING, end.kind);
            assertEquals(phase, key(cluster.log).phase);
            assertEquals(ClusterActionExecution.FINAL_COMMITTED_CLEANUP_PENDING, released(owner.id).finalState);

            // resume finishes it under a new action, which adopts the key
            Owner resumed = new Owner(coordinator2, node2, PartitionSplitActuator.params(0, PartitionSplitActuator.MODE_RESUME));
            assertEquals(Optional.empty(), resumed.actuator.validate(resumed.action, null));
            resumed.claimLane();
            assertEquals(StepResult.Kind.COMMITTED, resumed.toEnd().kind);
            assertEquals(Phase.STEADY, key(cluster.log).phase);
            assertResidueKept(env.writes.toString());
        }
    }

    @Test
    public void resumeOfAnAbandonedPreCommitMigrationAborts() throws Exception
    {
        Owner owner = owner().reach(Phase.BACKFILL);
        coordinator1.forceRelease(Lane.SCHEMA_LAYOUT);
        catchUp();
        Owner resumed = new Owner(coordinator2, node2, PartitionSplitActuator.params(0, PartitionSplitActuator.MODE_RESUME));
        assertEquals(Optional.empty(), resumed.actuator.validate(resumed.action, null));
        resumed.claimLane();
        StepResult end = resumed.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertRemovedAndDropped();
    }

    // ---------------------------------------------------------------------------------------------------------
    // takeover: the new owner has nothing but the registry and the ledger
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void takeoverInEachPhase() throws Exception
    {
        String[] points = { "CREATING", "ANNOUNCED", "DUAL_WRITE", "BACKFILL", "VERIFY", "CUTOVER", "ROLLBACK_DUAL", "ABORTING",
                            "KEY_REMOVED", "NEW_ONLY", "CLEANUP", "CLEANUP_RECONCILE", "STEADY" };
        for (String point : points)
        {
            setUp();
            Owner first = owner();
            boolean preCommit;
            switch (point)
            {
                case "CREATING":
                    first.until(o -> o.claim() != null && PartitionSplitActuator.CREATING.equals(o.claim().phase) && keyspace().tables.size() == 2);
                    preCommit = true;
                    break;
                case "ROLLBACK_DUAL":
                case "ABORTING":
                case "KEY_REMOVED":
                    first.reach(Phase.CUTOVER);
                    PartitionSplitActuator.requestAbort(coordinator1, () -> cluster.log, PartitionSplitActuator.target(KS, TABLE, KEY_HEX), "test");
                    first.until(o -> o.claim() != null && o.claim().phase.equals(point));
                    preCommit = true;
                    break;
                case "CLEANUP":
                    first.reach(Phase.CLEANUP, false);
                    preCommit = false;
                    break;
                case "CLEANUP_RECONCILE":
                    first.reportReconcile = false;
                    first.reach(Phase.CLEANUP, true);
                    preCommit = false;
                    break;
                case "STEADY":
                    first.until(o -> o.claim() != null && Phase.STEADY.name().equals(o.claim().phase));
                    preCommit = false;
                    break;
                default:
                    first.reach(Phase.valueOf(point));
                    preCommit = PRE_COMMIT.contains(Phase.valueOf(point));
            }
            // the first owner is lost; the tick of node 2 takes the expired claim over for a fresh actuator
            Owner second = new Owner(coordinator2, node2).takeOverExpired(first);
            StepResult end = second.toEnd();
            if (preCommit)
            {
                assertEquals(point + ": " + end, StepResult.Kind.ROLLED_BACK, end.kind);
                assertRemovedAndDropped();
            }
            else
            {
                assertEquals(point + ": " + end, StepResult.Kind.COMMITTED, end.kind);
                assertEquals(Phase.STEADY, key(cluster.log).phase);
                assertResidueKept(point + ": " + env.writes);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // deadlines (design 4.5, ADR-031)
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void ownerDeadlineFiresStrictlyBeforeTheReplicaLimit()
    {
        PartitionSplitActuator actuator = new PartitionSplitActuator(env, () -> config);
        Epoch e = Epoch.create(5);
        for (int gcGrace : new int[]{ 28800, 864000 })
        {
            TableMetadata logical = logicalTable(gcGrace);
            for (long straggler : new long[]{ 0, 1000, 3 * 3600_000L })
            {
                config.layout.straggler_window = new DurationSpec.LongMillisecondsBound(straggler + "ms");
                KeyState state = new KeyState(Phase.BACKFILL, e, 2000, 1, 1, e, ImmutableSet.of(), new UUID(1, 1), 1000);
                long deadline = actuator.preCommitDeadline(state, logical);
                long replicaLimit = 1000 + gcGrace * 1000L / 2 - WRITE_TIMEOUT;
                assertEquals(replicaLimit, actuator.replicaLimit(state, logical));
                assertTrue(gcGrace + "/" + straggler + ": " + deadline + " < " + replicaLimit, deadline < replicaLimit);
                assertTrue(deadline <= 1000 + config.layout.max_migration_duration.toMilliseconds());
            }
        }
    }

    @Test
    public void deadlineAbortsBeforeTheCommitPoint() throws Exception
    {
        // replicas never report: BACKFILL holds until the owner's deadline turns it to the abort path
        Owner owner = owner();
        owner.autoReport = false;
        owner.reach(Phase.BACKFILL);
        KeyState state = key(cluster.log);
        long deadline = owner.actuator.preCommitDeadline(state, keyspace().tables.getNullable(TABLE));
        cluster.now.set(deadline - 5_000);
        long until = deadline - 4_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals("holds until the deadline", Phase.BACKFILL, key(cluster.log).phase);
        owner.until(o -> key(cluster.log) == null || key(cluster.log).phase == Phase.ABORTING);
        assertTrue(cluster.now.get() >= deadline);
        assertTrue("the abort starts before the replicas stop copying",
                   key(cluster.log).phaseSinceMillis < owner.actuator.replicaLimit(state, keyspace().tables.getNullable(TABLE)));
        StepResult end = owner.toEnd();
        assertEquals(StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains("backfill_deadline"));
        assertRemovedAndDropped();
    }

    @Test
    public void announcedDeadlineAborts() throws Exception
    {
        Owner owner = owner().reach(Phase.ANNOUNCED);
        long registered = key(cluster.log).phaseSinceMillis;
        cluster.now.set(registered + config.layout.max_migration_duration.toMilliseconds());
        StepResult end = owner.toEnd();
        assertEquals(StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains("max_migration_duration"));
    }

    @Test
    public void reconcilePastTheReplicaLimitEndsFailedAndKeepsTheBaseCopy() throws Exception
    {
        Owner owner = owner();
        owner.reportReconcile = false;
        owner.reach(Phase.CLEANUP, true);
        KeyState state = key(cluster.log);
        long limit = owner.actuator.replicaLimit(state, keyspace().tables.getNullable(TABLE));
        cluster.now.set(limit);
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.FAILED, end.kind);
        assertTrue(end.detail, end.detail.startsWith(PartitionSplitActuator.RECONCILE_PAST_DEADLINE));
        assertEquals("the key stays in CLEANUP, routed to the overflow copy", Phase.CLEANUP, key(cluster.log).phase);
        assertFalse("no base tombstone", env.writes.toString().contains("deleteBase"));
        // the status names it; resume refuses it
        List<Map<String, String>> status = PartitionSplitActuator.status(cluster.log, cluster.now.get(), config.layout, WRITE_TIMEOUT);
        assertTrue(status.toString(), status.get(0).get("status").startsWith(PartitionSplitActuator.ORPHANED + ": " + PartitionSplitActuator.RECONCILE_PAST_DEADLINE));
        assertTrue(PartitionSplitActuator.orphans(cluster.log, cluster.now.get(), WRITE_TIMEOUT).get(0).pastLimit);
        Owner resumed = new Owner(coordinator2, node2, PartitionSplitActuator.params(0, PartitionSplitActuator.MODE_RESUME));
        Optional<String> refused = resumed.actuator.validate(resumed.action, null);
        assertTrue(refused.isPresent());
        assertTrue(refused.get(), refused.get().contains("tombstone deadline"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // the commit window (T6.9 review, fix A)
    // ---------------------------------------------------------------------------------------------------------

    /** gc_grace_seconds 28800 with the default windows: half of it is 4 h after DUAL_WRITE. */
    private void smallGraceWithDefaultWindows()
    {
        setSchema(Keyspaces.of(KeyspaceMetadata.create(KS, KeyspaceParams.simple(2), Tables.of(logicalTable(28800)))));
        setReplicas(1, 2);
        AutonomicConfig.LayoutConfig defaults = new AutonomicConfig.LayoutConfig();
        config.layout.straggler_window = defaults.straggler_window;
        config.layout.max_migration_duration = defaults.max_migration_duration;
        config.layout.reconcile_budget = defaults.reconcile_budget;
    }

    @Test
    public void commitPointRefusedWhenTheReconcilePassCouldNotEndBeforeTheReplicaLimit() throws Exception
    {
        // the review's example: approval at DUAL_WRITE + 59 min; 59 min + 3 h straggler window + 30 min reconcile budget > 4 h
        smallGraceWithDefaultWindows();
        config.layout.commit_approval = "manual";
        Owner owner = owner().reach(Phase.CUTOVER);
        KeyState state = key(cluster.log);
        TableMetadata logical = keyspace().tables.getNullable(TABLE);
        assertNull("one migration fits", owner.actuator.migrationDoesNotFit(logical));
        long window = owner.actuator.commitWindowEnd(state, logical);
        assertEquals(state.dualWriteSinceMillis + 4 * 3600_000L - WRITE_TIMEOUT - 3 * 3600_000L - 1800_000L - 2 * 100, window);
        assertTrue("the owner's deadline would not have stopped it", owner.actuator.preCommitDeadline(state, logical) > state.dualWriteSinceMillis + 59 * 60_000L);
        cluster.now.set(state.dualWriteSinceMillis + 59 * 60_000L);
        assertTrue(PartitionSplitActuator.approveCommit(coordinator1, owner.id));
        StepResult end = owner.toEnd();
        assertEquals(end.toString(), StepResult.Kind.ROLLED_BACK, end.kind);
        assertTrue(end.detail, end.detail.contains("the commit window closed"));
        assertTrue(owner.phases.toString(), owner.phases.containsAll(List.of("CUTOVER/1", "ROLLBACK_DUAL/1", "ABORTING/1", "absent")));
        assertFalse(owner.phases.toString(), owner.phases.contains("NEW_ONLY/1"));
        assertFalse("the base copy is never deleted by an abort", env.writes.toString().contains("deleteBase"));
        assertRemovedAndDropped();
    }

    @Test
    public void commitPointTakenInsideTheWindow() throws Exception
    {
        smallGraceWithDefaultWindows();
        Owner owner = owner().reach(Phase.CUTOVER);
        KeyState state = key(cluster.log);
        long window = owner.actuator.commitWindowEnd(state, keyspace().tables.getNullable(TABLE));
        cluster.now.set(window - 10_000);
        owner.reach(Phase.NEW_ONLY);
        assertTrue(key(cluster.log).phaseSinceMillis < window);
    }

    @Test
    public void aTableWhoseGcGraceCannotFitOneMigrationIsRefusedUpFront()
    {
        smallGraceWithDefaultWindows();
        config.layout.reconcile_budget = new DurationSpec.LongMillisecondsBound("1h");
        PartitionSplitActuator actuator = new PartitionSplitActuator(env, () -> config);
        Optional<String> refused = actuator.validate(action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(4, "split")), null);
        assertTrue(refused.isPresent());
        assertEquals("gc_grace_seconds 28800 of ks.t leaves no time for a migration: half of it less the write timeout is 14398000 ms, and cutover_soak 1s"
                     + " + straggler_window 3h + reconcile_budget 1h + 2 x ack_poll_interval 100ms need 14401200 ms", refused.get());
        config.layout.reconcile_budget = new DurationSpec.LongMillisecondsBound("30m");
        assertEquals(Optional.empty(), actuator.validate(action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(4, "split")), null));
    }

    // ---------------------------------------------------------------------------------------------------------
    // FinishKey after a complete reconcile only (ADR-033, fix B)
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void placementChangeBetweenTheReportsAndFinishKeyStartsANewReconcileGeneration() throws Exception
    {
        Owner owner = owner();
        // every replica reported the reconcile generation and the owner has decided to finish
        owner.until(o -> o.claim() != null && o.claim().state.containsKey(PartitionSplitActuator.S_FINISHING) && Phase.CLEANUP.name().equals(o.claim().phase));
        int reconcile = key(cluster.log).generation;
        // a third node becomes a replica of the key before FinishKey: it has not reported
        owner.autoReport = false;
        setReplicas(1, 2, 3);
        owner.until(o -> key(cluster.log).generation == reconcile + 1);
        assertEquals(Phase.CLEANUP, key(cluster.log).phase);
        assertTrue(key(cluster.log).backfillDone.isEmpty());
        long until = cluster.now.get() + 10_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals("no FinishKey while the new replica has not reported", Phase.CLEANUP, key(cluster.log).phase);
        owner.autoReport = true;
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
        assertEquals(ImmutableSet.of(cluster.id(1), cluster.id(2), cluster.id(3)), key(cluster.log).backfillDone);
        assertEquals(reconcile + 1, key(cluster.log).generation);
    }

    @Test
    public void finishKeyIsRejectedWhileANaturalReplicaHasNotReported() throws Exception
    {
        Owner owner = owner();
        owner.reportReconcile = false;
        owner.reach(Phase.CLEANUP, true);
        KeyState state = key(cluster.log);
        cluster.log = new ReportBackfill(LOGICAL, KEY, state.generation, state.generationEpoch, cluster.id(1)).wrap().execute(cluster.log).success().metadata;
        catchUp();
        Claim claim = owner.claim();
        Transformation.Result result = new FinishKey(owner.id, claim.owner, claim.step, LOGICAL, KEY, cluster.now.get(), 600_000).wrap().execute(cluster.log);
        assertTrue(String.valueOf(result), result.isRejected());
        assertTrue(result.rejected().reason, result.rejected().reason.contains("natural replicas [" + cluster.id(2).id() + "] have not reported"));
        // the owner waits for it
        long until = cluster.now.get() + 10_000;
        owner.until(o -> cluster.now.get() > until);
        assertEquals(Phase.CLEANUP, key(cluster.log).phase);
        owner.reportReconcile = true;
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    // ---------------------------------------------------------------------------------------------------------
    // orphaned keys (fix C) and the continuation of a stopped split (fix I)
    // ---------------------------------------------------------------------------------------------------------

    /** A split stopped past its commit point in {@code phase}: the claim is released, the key keeps its action. */
    private Owner stoppedIn(Phase phase, boolean reconcileStarted) throws Exception
    {
        Owner owner = owner().reach(phase, reconcileStarted);
        owner.entry = Entry.ABORT;
        assertEquals(StepResult.Kind.COMMITTED_CLEANUP_PENDING, owner.step().kind);
        assertEquals(owner.id, key(cluster.log).actionId);
        return owner;
    }

    private Owner resumeOn(ClusterCoordinator coordinator, FakeCluster.Node node)
    {
        return new Owner(coordinator, node, PartitionSplitActuator.params(0, PartitionSplitActuator.MODE_RESUME));
    }

    @Test
    public void anOrphanedKeyIsDiagnosedShownAndAdopted() throws Exception
    {
        for (Phase phase : new Phase[]{ Phase.NEW_ONLY, Phase.CLEANUP })
        {
            setUp();
            Owner owner = stoppedIn(phase, false);
            List<PartitionSplitActuator.Orphan> orphans = PartitionSplitActuator.orphans(cluster.log, cluster.now.get(), WRITE_TIMEOUT);
            assertEquals(1, orphans.size());
            assertEquals(KEY_HEX, orphans.get(0).keyHex());
            assertFalse(orphans.get(0).pastLimit);
            Map<String, String> status = PartitionSplitActuator.status(cluster.log, cluster.now.get(), config.layout, WRITE_TIMEOUT).get(0);
            assertTrue(status.toString(), status.get("status").startsWith(PartitionSplitActuator.ORPHANED + ": no owner, past the commit point"));
            assertEquals("true", status.get("orphaned"));
            // the diagnosis, on any node
            InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(8, 8), cluster.now::get);
            List<Diagnosis> diagnoses = new SplitOutcomeDetectors.Orphaned(() -> cluster.log, () -> WRITE_TIMEOUT).analyze(view);
            assertEquals(1, diagnoses.size());
            assertEquals(PartitionSplitActuator.ORPHANED, diagnoses.get(0).type);
            assertEquals(Severity.CRITICAL, diagnoses.get(0).severity);
            assertEquals(KEY_HEX, diagnoses.get(0).evidence.get("key_hex"));
            assertEquals(phase.name(), diagnoses.get(0).evidence.get("phase"));

            // its adoption (what the policy proposes on the preferred proposer): a continuation, which the ledger
            // takes without the cooldown the stopped action left
            node2.cooldownAfterCommit = 3_600_000;
            catchUp();
            Owner adopter = resumeOn(coordinator2, node2);
            assertEquals(Optional.empty(), adopter.actuator.validate(adopter.action, null));
            assertTrue(adopter.actuator.continuation(adopter.action));
            assertFalse("a new split of the key is not a continuation",
                        adopter.actuator.continuation(action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(4, "split"))));
            ClusterCoordinator.Outcome cooled = coordinator2.claim(Lane.SCHEMA_LAYOUT, adopter.id, PartitionSplitActuator.NAME, adopter.action.target.asString(),
                                                                   ClusterActionExecution.INITIAL_PHASE, Collections.emptyMap());
            assertFalse("without the exemption the ledger cooldown refuses it", cooled.ok());
            catchUp();
            adopter = resumeOn(coordinator2, node2);
            ClusterCoordinator.Outcome claimed = coordinator2.claim(Lane.SCHEMA_LAYOUT, adopter.id, PartitionSplitActuator.NAME, adopter.action.target.asString(),
                                                                    ClusterActionExecution.INITIAL_PHASE, Collections.emptyMap(), true);
            assertTrue(claimed.toString(), claimed.ok());
            catchUp();
            assertTrue("a claim is adopting it", PartitionSplitActuator.orphans(cluster.log, cluster.now.get(), WRITE_TIMEOUT).isEmpty());
            assertTrue(PartitionSplitActuator.status(cluster.log, cluster.now.get(), config.layout, WRITE_TIMEOUT).get(0).get("status").startsWith("being adopted"));
            assertEquals(StepResult.Kind.COMMITTED, adopter.toEnd().kind);
            assertEquals(Phase.STEADY, key(cluster.log).phase);
            assertNull(key(cluster.log).actionId);
            assertResidueKept(env.writes.toString());
            assertTrue(PartitionSplitActuator.orphans(cluster.log, cluster.now.get(), WRITE_TIMEOUT).isEmpty());
            assertTrue(PartitionSplitActuator.status(cluster.log, cluster.now.get(), config.layout, WRITE_TIMEOUT).get(0).get("status").startsWith("split; base residue kept"));
            assertFalse("a split key has nothing to resume", adopter.actuator.continuation(adopter.action));
        }
    }

    @Test
    public void aSteadyKeyThatWasNotReleasedIsOrphanedUntilAdopted() throws Exception
    {
        Owner owner = owner();
        owner.until(o -> o.claim() != null && Phase.STEADY.name().equals(o.claim().phase));
        owner.entry = Entry.ABORT;
        assertEquals(StepResult.Kind.COMMITTED_CLEANUP_PENDING, owner.step().kind);
        assertEquals(Phase.STEADY, key(cluster.log).phase);
        assertEquals("owned until released", owner.id, key(cluster.log).actionId);
        Map<String, String> status = PartitionSplitActuator.status(cluster.log, cluster.now.get(), config.layout, WRITE_TIMEOUT).get(0);
        assertTrue(status.toString(), status.get("status").contains("not released"));
        assertTrue(status.get("status").startsWith(PartitionSplitActuator.ORPHANED));
        List<Diagnosis> diagnoses = new SplitOutcomeDetectors.Orphaned(() -> cluster.log, () -> WRITE_TIMEOUT)
                                    .analyze(new InMemoryKnowledgeView(new ObservationStore(8, 8), cluster.now::get));
        assertEquals(Severity.WARN, diagnoses.get(0).severity);

        Owner adopter = resumeOn(coordinator2, node2);
        assertTrue(adopter.actuator.continuation(adopter.action));
        adopter.claimLane();
        assertEquals(StepResult.Kind.COMMITTED, adopter.toEnd().kind);
        assertNull(key(cluster.log).actionId);
        assertResidueKept("adoption of an unreleased STEADY key");
        assertTrue(PartitionSplitActuator.status(cluster.log, cluster.now.get(), config.layout, WRITE_TIMEOUT).get(0).get("status").startsWith("split; base residue kept"));
        Optional<String> nothing = adopter.actuator.validate(resumeOn(coordinator2, node2).action, null);
        assertTrue(nothing.toString(), nothing.isPresent() && nothing.get().contains("nothing to resume"));
    }

    /** ADR-036: the key is released with its base residue, and nothing deleted the base partition. */
    private void assertResidueKept(String what)
    {
        assertFalse(what + ": " + env.writes, env.writes.toString().contains("deleteBase"));
        assertEquals(what, Phase.STEADY, key(cluster.log).phase);
        assertNull(what, key(cluster.log).actionId);
        assertEquals(what, KeyState.BaseState.RESIDUE, key(cluster.log).baseState);
    }

    // ---------------------------------------------------------------------------------------------------------
    // reclaim and the reverse phases (ADR-036, T6.17b)
    // ---------------------------------------------------------------------------------------------------------

    private Owner reclaimOn(ClusterCoordinator coordinator, FakeCluster.Node node)
    {
        return new Owner(coordinator, node, PartitionSplitActuator.params(0, PartitionSplitActuator.MODE_RECLAIM));
    }

    /** A completed split: released, base residue kept; the reclaim window is one hour from the freeze. */
    private long splitAndRelease() throws Exception
    {
        config.layout.reclaim_window = new DurationSpec.LongMillisecondsBound("1h");
        Owner owner = owner();
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
        assertResidueKept("a split");
        return key(cluster.log).frozenSinceMillis;
    }

    @Test
    public void aReclaimWritesTheTombstoneWhenDueAndReleasesTheKeyTombstoned() throws Exception
    {
        long frozen = splitAndRelease();
        assertTrue(frozen > 0);
        long due = PartitionSplitActuator.reclaimDueMillis(key(cluster.log), logicalTable(864000), config.layout);
        assertEquals(frozen + 3_600_000, due);
        // not before it is due
        Owner early = reclaimOn(coordinator1, node1);
        cluster.now.set(due - 1);
        Optional<String> notYet = early.actuator.validate(early.action, null);
        assertTrue(notYet.toString(), notYet.isPresent() && notYet.get().contains("not yet"));
        assertTrue(PartitionSplitActuator.reclaimsDue(cluster.log, due - 1, config.layout).isEmpty());
        String before = PartitionSplitActuator.status(cluster.log, due - 1, config.layout, WRITE_TIMEOUT).get(0).get("status");
        assertTrue(before, before.startsWith("split; base residue kept, reclaim due at " + due));

        SplitOutcomeDetectors.ReclaimDue detector = new SplitOutcomeDetectors.ReclaimDue(() -> cluster.log, () -> config.layout);
        assertTrue(detector.analyze(new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> due - 1)).isEmpty());
        cluster.now.set(due);
        catchUp();
        List<Diagnosis> raised = detector.analyze(new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> due));
        assertEquals(1, raised.size());
        assertEquals("layout.reclaim_due", raised.get(0).type);
        assertEquals(KEY_HEX, raised.get(0).evidence.get("key_hex"));
        assertEquals(1, PartitionSplitActuator.reclaimsDue(cluster.log, due, config.layout).size());
        Owner reclaim = reclaimOn(coordinator1, node1);
        assertEquals(Optional.empty(), reclaim.actuator.validate(reclaim.action, null));
        assertTrue("no new approval and no cooldown: a continuation", reclaim.actuator.continuation(reclaim.action));
        reclaim.claimLane();
        StepResult end = reclaim.toEnd();
        assertEquals(end.toString(), StepResult.Kind.COMMITTED, end.kind);
        assertEquals("deleteBase in STEADY acknowledged=true", env.writes.get(env.writes.size() - 1));
        KeyState k = key(cluster.log);
        assertEquals(Phase.STEADY, k.phase);
        assertNull(k.actionId);
        assertEquals(KeyState.BaseState.TOMBSTONED, k.baseState);
        assertTrue(k.baseTombstoneMicros > 0);
        assertEquals(frozen, k.frozenSinceMillis);
        assertTrue(PartitionSplitActuator.status(cluster.log, cluster.now.get(), config.layout, WRITE_TIMEOUT).get(0).get("status")
                                         .startsWith("split; base partition tombstoned at " + k.baseTombstoneMicros));
        // once is enough
        catchUp();
        Owner again = reclaimOn(coordinator1, node1);
        Optional<String> done = again.actuator.validate(again.action, null);
        assertTrue(done.toString(), done.isPresent() && done.get().contains("already TOMBSTONED"));
        assertFalse(again.actuator.continuation(again.action));
        assertTrue(PartitionSplitActuator.reclaimsDue(cluster.log, cluster.now.get(), config.layout).isEmpty());
    }

    @Test
    public void aReclaimIsRefusedWhileTheKeyIsOwnedAndInEveryOtherPhase() throws Exception
    {
        config.layout.reclaim_window = new DurationSpec.LongMillisecondsBound("1s");
        // owned: a STEADY key its action has not released
        Owner owner = owner();
        owner.until(o -> o.claim() != null && Phase.STEADY.name().equals(o.claim().phase));
        cluster.now.addAndGet(3_600_000);
        catchUp();
        Owner reclaim = reclaimOn(coordinator2, node2);
        Optional<String> owned = reclaim.actuator.validate(reclaim.action, null);
        assertTrue(owned.toString(), owned.isPresent() && owned.get().contains("not reclaimed while owned"));
        assertFalse(reclaim.actuator.continuation(reclaim.action));
        assertTrue(PartitionSplitActuator.reclaimsDue(cluster.log, cluster.now.get(), config.layout).isEmpty());

        // every other phase, forward and reverse, owned or not
        for (Phase phase : Phase.values())
        {
            if (phase == Phase.STEADY)
                continue;
            setKeyState(phase, new UUID(7, 7));
            Optional<String> refused = reclaim.actuator.validate(reclaim.action, null);
            assertTrue(phase + ": " + refused, refused.isPresent() && refused.get().contains("only a STEADY key is reclaimed"));
            assertFalse(phase.name(), reclaim.actuator.continuation(reclaim.action));
            assertTrue(phase.name(), PartitionSplitActuator.reclaimsDue(cluster.log, cluster.now.get() + 10_000_000_000L, config.layout).isEmpty());
        }
    }

    /** Replaces the state of the key with one in {@code phase} owned by {@code owner}, which holds no claim. */
    private void setKeyState(Phase phase, UUID owner)
    {
        TableLayout layout = LayoutRegistry.of(cluster.log).layout(LOGICAL);
        KeyState old = layout.keyState(KEY);
        Epoch e = cluster.log.epoch;
        KeyState state = new KeyState(phase, e, 1000, old.incarnation, 1, e, ImmutableSet.of(), owner, 500, ImmutableSet.of(), KeyState.BaseState.RESIDUE,
                                      1000, 0, phase.isReverse() ? KeyState.ReverseMode.FAST : null, 0);
        TableLayout changed = new TableLayout(layout.header(), layout.nextIncarnation, ImmutableMap.of(KEY, state), layout.version + 1);
        cluster.log = cluster.log.transformer().with(LayoutRegistryValue.KEY,
                                                    new LayoutRegistryValue(new LayoutRegistry(LayoutRegistry.FORMAT_VERSION_2, ImmutableMap.of(LOGICAL, changed))))
                                 .build().metadata;
        catchUp();
    }

    /** Orphan adoption (class 3 and no claim) must not match the reverse phases, REVERSE_ANNOUNCED included. */
    @Test
    public void orphanAdoptionNeverMatchesAReversePhase() throws Exception
    {
        splitAndRelease();
        for (Phase phase : Phase.values())
        {
            if (!phase.isReverse())
                continue;
            setKeyState(phase, new UUID(7, 7));
            String what = phase.name();
            assertTrue(what, PartitionSplitActuator.orphans(cluster.log, cluster.now.get(), WRITE_TIMEOUT).isEmpty());
            assertTrue(what, new SplitOutcomeDetectors.Orphaned(() -> cluster.log, () -> WRITE_TIMEOUT)
                             .analyze(new InMemoryKnowledgeView(new ObservationStore(8, 8), cluster.now::get)).isEmpty());
            Owner resume = resumeOn(coordinator2, node2);
            assertFalse(what, resume.actuator.continuation(resume.action));
            Optional<String> refused = resume.actuator.validate(resume.action, null);
            assertTrue(what + ": " + refused, refused.isPresent() && refused.get().contains("reverse phase"));
            String status = PartitionSplitActuator.status(cluster.log, cluster.now.get(), config.layout, WRITE_TIMEOUT).get(0).get("status");
            assertFalse(what + ": " + status, status.startsWith(PartitionSplitActuator.ORPHANED));
        }
        // while a forward C3 phase of the same shape is an orphan
        setKeyState(Phase.CLEANUP, new UUID(7, 7));
        assertEquals(1, PartitionSplitActuator.orphans(cluster.log, cluster.now.get(), WRITE_TIMEOUT).size());
    }

    @Test
    public void aReclaimInterruptedByAStopIsResumedAndWritesTheTombstoneWhenStillDue() throws Exception
    {
        long frozen = splitAndRelease();
        cluster.now.set(frozen + 3_600_000);
        catchUp();
        env.failBase = true;
        Owner reclaim = reclaimOn(coordinator1, node1).claimLane();
        reclaim.until(o -> o.claim() != null && Phase.STEADY.name().equals(o.claim().phase) && o.claim().state.containsKey(PartitionSplitActuator.S_TOMBSTONE)
                           && o.claim().state.containsKey(PartitionSplitActuator.S_ACKED));
        long until = cluster.now.get() + 5_000;
        reclaim.until(o -> cluster.now.get() > until);
        reclaim.entry = Entry.ABORT;
        assertEquals(StepResult.Kind.COMMITTED_CLEANUP_PENDING, reclaim.step().kind);
        assertEquals("the key stays owned by the stopped reclaim", reclaim.id, key(cluster.log).actionId);
        assertEquals(KeyState.BaseState.RESIDUE, key(cluster.log).baseState);
        assertTrue(PartitionSplitActuator.status(cluster.log, cluster.now.get(), config.layout, WRITE_TIMEOUT).get(0).get("status")
                                         .startsWith(PartitionSplitActuator.ORPHANED));
        assertEquals(1, PartitionSplitActuator.orphans(cluster.log, cluster.now.get(), WRITE_TIMEOUT).size());

        env.failBase = false;
        Owner resumed = resumeOn(coordinator2, node2);
        assertEquals(Optional.empty(), resumed.actuator.validate(resumed.action, null));
        assertTrue(resumed.actuator.continuation(resumed.action));
        resumed.claimLane();
        assertEquals(StepResult.Kind.COMMITTED, resumed.toEnd().kind);
        assertEquals("deleteBase in STEADY acknowledged=true", env.writes.get(env.writes.size() - 1));
        assertEquals(KeyState.BaseState.TOMBSTONED, key(cluster.log).baseState);
        assertNull(key(cluster.log).actionId);
    }

    @Test
    public void anExpiredClaimThatOwnsAKeyPastItsCommitPointIsNotReplacedByANewAction() throws Exception
    {
        Owner owner = owner().reach(Phase.CLEANUP);
        Claim expired = owner.claim();
        cluster.now.set(expired.leaseExpiresAtMillis + 1);
        catchUp();
        String other = ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(7));
        UUID newAction = cluster.newActionId();
        ClusterCoordinator.Outcome replaced = coordinator2.claim(Lane.SCHEMA_LAYOUT, newAction, PartitionSplitActuator.NAME,
                                                                 PartitionSplitActuator.target(KS, TABLE, other).asString(),
                                                                 ClusterActionExecution.INITIAL_PHASE, Collections.emptyMap());
        assertFalse(replaced.ok());
        assertEquals("the expired claim is still there", owner.id, cluster.ledger().claims.get(Lane.SCHEMA_LAYOUT).actionId);
        assertTrue(String.valueOf(replaced.detail), replaced.detail.contains("past its commit point; the claim can only be taken over"));
        // a takeover of it is accepted, and finishes the key
        Owner second = new Owner(coordinator2, node2).takeOverExpired(owner);
        assertEquals(StepResult.Kind.COMMITTED, second.toEnd().kind);
        assertEquals(Phase.STEADY, key(cluster.log).phase);
    }

    // ---------------------------------------------------------------------------------------------------------
    // abort requests read one snapshot (fix H)
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void abortRequestReadsTheKeyAndTheClaimFromOneSnapshot() throws Exception
    {
        Owner owner = owner().reach(Phase.CUTOVER);
        ClusterMetadata stale = cluster.log;
        owner.reach(Phase.NEW_ONLY);
        catchUp();
        // the first snapshot is from before the commit point, the second is current
        java.util.Iterator<ClusterMetadata> snapshots = List.of(stale, cluster.log, cluster.log, cluster.log).iterator();
        try
        {
            PartitionSplitActuator.requestAbort(coordinator2, snapshots::next, PartitionSplitActuator.target(KS, TABLE, KEY_HEX), "test");
            fail("an abort past the commit point must be refused");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("past its commit point (phase NEW_ONLY)"));
        }
        assertFalse("no abort request was written onto the claim past the commit point", owner.claim().state.containsKey(PartitionSplitActuator.S_ABORT));
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    // ---------------------------------------------------------------------------------------------------------
    // validate
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void validateRejections()
    {
        PartitionSplitActuator actuator = new PartitionSplitActuator(env, () -> config);
        assertEquals(Optional.empty(), actuator.validate(action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(8, "split")), null));
        assertRejected(actuator, "does not exist", action(KS, "nope", KEY_HEX, PartitionSplitActuator.params(8, "split")));
        assertRejected(actuator, "not hex", action(KS, TABLE, "zz", PartitionSplitActuator.params(8, "split")));
        assertRejected(actuator, "outside 2..64", action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(65, "split")));
        assertRejected(actuator, "not a valid partition key", action(KS, TABLE, "0102", PartitionSplitActuator.params(8, "split")));
        assertRejected(actuator, "nothing to resume", action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(0, "resume")));
        assertRejected(actuator, "unknown mode", action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(0, "sideways")));
        Action notPartition = new Action(cluster.newActionId(), PartitionSplitActuator.NAME, ActionScope.CLUSTER, Subject.table(KS, TABLE),
                                         PartitionSplitActuator.params(8, "split"), null);
        assertRejected(actuator, "target must be a partition", notPartition);
        config.layout.excluded_tables.add(KS + '.' + TABLE);
        assertRejected(actuator, "excluded_tables", action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(8, "split")));
        config.layout.excluded_tables.clear();
        // a gc_grace_seconds the straggler window does not fit in
        config.layout.straggler_window = new DurationSpec.LongMillisecondsBound("10d");
        assertRejected(actuator, "too small for straggler_window", action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(8, "split")));
    }

    @Test
    public void anOperatorsSplitChecksStragglerWindowAgainstGcGraceAsThePolicyDoes()
    {
        // ADR-029: the policy's window, min(max_migration_duration, gc_grace / 2 - straggler_window), shared
        AutonomicConfig.LayoutConfig lc = config.layout;
        lc.straggler_window = new DurationSpec.LongMillisecondsBound("3h");
        assertEquals(4 * 3600_000L - 3 * 3600_000L, PartitionSplitActuator.backfillWindowMillis(lc, 28800));
        assertEquals(lc.max_migration_duration.toMilliseconds(), PartitionSplitActuator.backfillWindowMillis(lc, 864000));
        assertNull(PartitionSplitActuator.stragglerWindowDoesNotFit(lc, logicalTable(28800)));
        lc.straggler_window = new DurationSpec.LongMillisecondsBound("4h");
        assertEquals(0, PartitionSplitActuator.backfillWindowMillis(lc, 28800));
        assertEquals("gc_grace_seconds 28800 of ks.t is too small for straggler_window 4h: half of gc_grace_seconds less straggler_window leaves 0 ms "
                     + "for the migration (ADR-029); raise gc_grace_seconds or lower layout.straggler_window",
                     PartitionSplitActuator.stragglerWindowDoesNotFit(lc, logicalTable(28800)));

        // the manual path (nodetool autonomic layout split goes through validate) refuses it with that error
        setSchema(Keyspaces.of(KeyspaceMetadata.create(KS, KeyspaceParams.simple(2), Tables.of(logicalTable(28800)))));
        PartitionSplitActuator actuator = new PartitionSplitActuator(env, () -> config);
        Optional<String> refused = actuator.validate(action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(4, "split")), null);
        assertEquals(Optional.of(PartitionSplitActuator.stragglerWindowDoesNotFit(lc, logicalTable(28800))), refused);
        lc.straggler_window = new DurationSpec.LongMillisecondsBound("1h");
        assertEquals(Optional.empty(), actuator.validate(action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(4, "split")), null));
    }

    @Test
    public void validateWhileAMigrationRuns() throws Exception
    {
        Owner owner = owner().reach(Phase.BACKFILL);
        assertRejected(owner.actuator, "already in the layout", action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(4, "split")));
        assertRejected(owner.actuator, "running under action", action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(0, "resume")));
        // another key of the same table must use the layout's bucket count
        String other = ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(7));
        assertRejected(owner.actuator, "has 4 buckets", action(KS, TABLE, other, PartitionSplitActuator.params(16, "split")));
        assertEquals(Optional.empty(), owner.actuator.validate(action(KS, TABLE, other, PartitionSplitActuator.params(0, "split")), null));
    }

    private Action action(String ks, String table, String keyHex, Map<String, String> params)
    {
        return new Action(cluster.newActionId(), PartitionSplitActuator.NAME, ActionScope.CLUSTER, Subject.partition(ks, table, keyHex), params, null);
    }

    private static void assertRejected(PartitionSplitActuator actuator, String part, Action action)
    {
        Optional<String> rejection = actuator.validate(action, null);
        assertTrue("expected a rejection containing '" + part + "'", rejection.isPresent());
        assertTrue(rejection.get(), rejection.get().contains(part));
    }
}
