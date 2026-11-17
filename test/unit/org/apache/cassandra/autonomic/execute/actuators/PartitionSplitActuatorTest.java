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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.FakeCluster;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.Recent;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.ClusterActionExecution;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.FinishKey;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.LayoutVerifier;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.RemoveKey;
import org.apache.cassandra.autonomic.layout.ReportBackfill;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
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
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
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
        /** The highest epoch the watermark answered true for. */
        Epoch acknowledgedUpTo = Epoch.EMPTY;
        Set<NodeId> replicas = new LinkedHashSet<>();
        Epoch replicasModified = Epoch.EMPTY;
        /** Results of the next checks, in order; a passing check when empty. */
        final List<FakeCheck> checks = new ArrayList<>();
        int checksStarted;
        final List<String> writes = new ArrayList<>();
        boolean failDeletes;

        @Override
        public ClusterMetadata metadata()
        {
            return cluster.log;
        }

        @Override
        public boolean acknowledged(Epoch epoch)
        {
            if (!allAnswer || epoch.isAfter(cluster.log.epoch))
                return false;
            if (epoch.isAfter(acknowledgedUpTo))
                acknowledgedUpTo = epoch;
            return true;
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
            writes.add("deleteBuckets " + incarnation + " in " + state.phase + " acknowledged=" + acknowledgedUpTo.isEqualOrAfter(state.phaseEpoch));
        }

        @Override
        public void deleteBase(TableMetadata logical, ByteBuffer key, long timestampMicros)
        {
            KeyState state = key(cluster.log);
            writes.add("deleteBase in " + (state == null ? null : state.phase) + " acknowledged=" + (state != null && acknowledgedUpTo.isEqualOrAfter(state.phaseEpoch)));
        }

        @Override
        public Set<NodeId> replicas(ClusterMetadata metadata, TableMetadata logical, ByteBuffer key)
        {
            return replicas;
        }

        @Override
        public Epoch replicasModified(ClusterMetadata metadata, TableMetadata logical, ByteBuffer key)
        {
            return replicasModified;
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
        env.replicas.add(cluster.id(1));
        env.replicas.add(cluster.id(2));
        AutonomicConfig.LayoutConfig lc = config.layout;
        lc.ack_poll_interval = new DurationSpec.IntMillisecondsBound("100ms");
        lc.cas_drain = new DurationSpec.IntMillisecondsBound("500ms");
        lc.cutover_soak = new DurationSpec.IntMillisecondsBound("1s");
        lc.straggler_window = new DurationSpec.LongMillisecondsBound("2s");
        lc.verify_recheck_delay = new DurationSpec.IntMillisecondsBound("100ms");
        lc.commit_approval = "auto";
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

        /** A takeover of the expired claim of {@code previous} by this owner's node, as the periodic tick does it. */
        Owner takeOver(Owner previous)
        {
            catchUp();
            Claim expired = coordinator.ledger().claim(previous.id);
            cluster.now.set(expired.leaseExpiresAtMillis + 1);
            id = coordinator.newActionId();
            ClusterCoordinator.Outcome outcome = coordinator.takeOver(Lane.SCHEMA_LAYOUT, expired, id);
            assertTrue(outcome.toString(), outcome.ok());
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
        assertTrue(child + " committed before every node acknowledged epoch " + state.phaseEpoch.getEpoch() + " of " + state.phase,
                   env.acknowledgedUpTo.isEqualOrAfter(state.phaseEpoch));
    }

    /** The replicas commit their reports, as BackfillTask does, for the generation that is due. */
    private void report()
    {
        KeyState state = key(cluster.log);
        if (state == null || !state.backfillDue())
            return;
        for (NodeId node : env.replicas)
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
        // the overflow table was created, never dropped; no bucket was deleted; the base tombstone came last, in STEADY
        assertEquals(2, env.writes.size());
        assertTrue(env.writes.get(0), env.writes.get(0).startsWith("create "));
        assertEquals("deleteBase in STEADY acknowledged=true", env.writes.get(1));
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
    public void noTransitionWithoutFullAcknowledgement() throws Exception
    {
        // in every phase that waits for O1, a node that does not answer holds the key where it is
        for (Phase phase : new Phase[]{ Phase.ANNOUNCED, Phase.DUAL_WRITE, Phase.BACKFILL, Phase.VERIFY, Phase.CUTOVER, Phase.NEW_ONLY, Phase.CLEANUP })
        {
            setUp();
            Owner owner = owner().reach(phase);
            env.allAnswer = false;
            int before = cluster.applied.size();
            KeyState held = key(cluster.log);
            long until = cluster.now.get() + 30_000;
            owner.until(o -> cluster.now.get() > until);
            KeyState now = key(cluster.log);
            assertEquals(phase.name(), held.phase, now.phase);
            assertEquals(phase.name(), held.phaseEpoch, now.phaseEpoch);
            assertEquals(phase.name(), held.generation, now.generation);
            // nothing but renewals and recorded sub-steps of the claim were committed
            for (Transformation t : cluster.applied.subList(before, cluster.applied.size()))
            {
                Transformation child = t instanceof CustomTransformation ? ((CustomTransformation) t).child() : t;
                assertFalse(phase + ": " + child, child instanceof AdvanceLayout || child instanceof RemoveKey || child instanceof FinishKey);
            }
            env.allAnswer = true;
            assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
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
        env.replicas.add(cluster.id(3));
        env.replicasModified = cluster.log.epoch;
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
        env.replicasModified = cluster.log.epoch;
        owner.until(o -> key(cluster.log).phase == Phase.BACKFILL);
        assertEquals(2, key(cluster.log).generation);
        owner.reportReconcile = false;
        owner.reach(Phase.CLEANUP, true);
        int reconcile = key(cluster.log).generation;
        // a replica group change committed after the reconcile generation started
        env.replicasModified = cluster.log.epoch.nextEpoch();
        owner.until(o -> key(cluster.log).generation == reconcile + 1);
        assertEquals(Phase.CLEANUP, key(cluster.log).phase);
        owner.reportReconcile = true;
        assertEquals(StepResult.Kind.COMMITTED, owner.toEnd().kind);
    }

    // ---------------------------------------------------------------------------------------------------------
    // verification (ADR-030)
    // ---------------------------------------------------------------------------------------------------------

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
            assertTrue(env.writes.toString(), env.writes.get(env.writes.size() - 1).startsWith("deleteBase in STEADY"));
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
                            "KEY_REMOVED", "NEW_ONLY", "CLEANUP", "CLEANUP_RECONCILE", "CLEANUP_TOMBSTONE", "STEADY" };
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
                case "CLEANUP_TOMBSTONE":
                    first.until(o -> o.claim() != null && o.claim().state.containsKey(PartitionSplitActuator.S_TOMBSTONE)
                                     && Phase.CLEANUP.name().equals(o.claim().phase));
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
            // the first owner is lost; a fresh actuator on node 2 takes the expired claim over
            Owner second = new Owner(coordinator2, node2).takeOver(first);
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
                assertTrue(point + ": " + env.writes, env.writes.get(env.writes.size() - 1).startsWith("deleteBase in STEADY acknowledged=true"));
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
        assertTrue(status.toString(), status.get(0).get("status").startsWith(PartitionSplitActuator.RECONCILE_PAST_DEADLINE));
        Owner resumed = new Owner(coordinator2, node2, PartitionSplitActuator.params(0, PartitionSplitActuator.MODE_RESUME));
        Optional<String> refused = resumed.actuator.validate(resumed.action, null);
        assertTrue(refused.isPresent());
        assertTrue(refused.get(), refused.get().contains("tombstone deadline"));
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
        assertRejected(actuator, "leaves no time", action(KS, TABLE, KEY_HEX, PartitionSplitActuator.params(8, "split")));
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
