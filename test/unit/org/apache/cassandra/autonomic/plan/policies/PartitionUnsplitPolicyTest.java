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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.detectors.SplitOutcomeDetectors;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.FakeCluster;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.actuators.PartitionUnsplitActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutHeader;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.LayoutRegistryValue;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.autonomic.monitor.Subject;
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
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.17g: a proposal on each diagnosis, never approved without an operator, the earliest time a rejection names, and
 * the reverse orphan proposed for {@code mode resume}.
 */
public class PartitionUnsplitPolicyTest
{
    private static final String KS = "ks";
    private static final String TABLE = "t";
    private static final TableId LOGICAL = TableId.fromUUID(new UUID(1, 1));
    private static final ByteBuffer KEY = Int32Type.instance.decompose(7);
    private static final String KEY_HEX = ByteBufferUtil.bytesToHex(KEY);

    private final AutonomicConfig config = new AutonomicConfig();
    private final long[] now = { 10_000_000 };
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(16, 64), () -> now[0]);
    private final List<ActionRecord> records = new ArrayList<>();
    private ClusterLedger ledger = ClusterLedger.EMPTY;
    private final Env env = new Env();
    private PartitionUnsplitPolicy policy;
    private FakeCluster cluster;

    private static final class Env implements PartitionUnsplitPolicy.Env
    {
        ClusterMetadata metadata;
        boolean preferred = true;
        final List<NodeId> notAlive = new ArrayList<>();
        Optional<String> rejection = Optional.empty();

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
        public Optional<String> validate(Action action, KnowledgeView knowledge)
        {
            return rejection;
        }

        @Override
        public String note(Action action)
        {
            return "fast mode, estimate 1 ms";
        }
    }

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @org.junit.Before
    public void setUp()
    {
        view.setActionSource(ms -> records);
        view.setLedgerSource(() -> ledger);
        view.setActiveDiagnoses(new ArrayList<>());
        cluster = new FakeCluster(3);
        TableMetadata t = TableMetadata.builder(KS, TABLE).id(LOGICAL)
                                       .addPartitionKeyColumn("p", Int32Type.instance)
                                       .addClusteringColumn("c", Int32Type.instance)
                                       .addRegularColumn("v", UTF8Type.instance).build();
        env.metadata = cluster.log.transformer().with(new DistributedSchema(Keyspaces.of(KeyspaceMetadata.create(KS, KeyspaceParams.simple(2), Tables.of(t))))).build().metadata;
        policy = new PartitionUnsplitPolicy(config, env);
    }

    private static Diagnosis diagnosis(String type, Map<String, String> evidence)
    {
        return new Diagnosis(UUID.randomUUID(), type, Subject.partition(KS, TABLE, KEY_HEX), Severity.WARN, 1, 0, 0, type, evidence, null);
    }

    private static Diagnosis notEffective()
    {
        return diagnosis(PartitionUnsplitActuator.SPLIT_NOT_EFFECTIVE, Map.of("key_hex", KEY_HEX));
    }

    private static Diagnosis pending(long due)
    {
        return diagnosis(PartitionUnsplitActuator.UNSPLIT_PENDING, Map.of("key_hex", KEY_HEX, "due_millis", Long.toString(due)));
    }

    private static Diagnosis orphaned()
    {
        return diagnosis(PartitionUnsplitActuator.UNSPLIT_ORPHANED, Map.of("key_hex", KEY_HEX, "phase", "REVERSE_VERIFY"));
    }

    @Test
    public void handlesTheThreeDiagnoses()
    {
        assertEquals(ImmutableSet.of("layout.split_not_effective", "layout.unsplit_pending", "layout.unsplit_orphaned"), policy.handles());
    }

    @Test
    public void proposesOnSplitNotEffective()
    {
        Plan plan = policy.plan(notEffective(), view).get();
        Action a = plan.actions.get(0);
        assertEquals(PartitionUnsplitActuator.NAME, a.actuator);
        assertEquals(PartitionUnsplitActuator.MODE_AUTO, a.params.get(PartitionUnsplitActuator.PARAM_MODE));
        assertTrue(plan.rationale, plan.rationale.contains("held for approval"));
        assertTrue(plan.rationale, plan.rationale.contains("fast mode"));
        // only the preferred proposer proposes it
        env.preferred = false;
        assertFalse(policy.plan(notEffective(), view).isPresent());
    }

    @Test
    public void proposesOnDuePendingOnly()
    {
        assertFalse("not due", policy.plan(pending(now[0] + 1), view).isPresent());
        assertTrue("due", policy.plan(pending(now[0]), view).isPresent());
        // the node that holds the request proposes, whichever node is the preferred proposer
        env.preferred = false;
        assertTrue(policy.plan(pending(now[0]), view).isPresent());
    }

    @Test
    public void proposesResumeForTheReverseOrphan()
    {
        Plan plan = policy.plan(orphaned(), view).get();
        assertEquals(PartitionUnsplitActuator.MODE_RESUME, plan.actions.get(0).params.get(PartitionUnsplitActuator.PARAM_MODE));
    }

    @Test
    public void neverApprovedAutomatically()
    {
        PartitionUnsplitActuator actuator = PartitionUnsplitActuator.create(() -> config);
        for (Diagnosis d : List.of(notEffective(), pending(now[0]), orphaned()))
            assertTrue(actuator.requiresManualApproval(policy.plan(d, view).get().actions.get(0)));
    }

    @Test
    public void nothingWhileBusyOrRejected()
    {
        env.rejection = Optional.of("a key stays split for 5 more ms: slow mode from " + (now[0] + 5));
        assertFalse(policy.plan(notEffective(), view).isPresent());
        env.rejection = Optional.empty();
        env.notAlive.add(new NodeId(3));
        assertFalse(policy.plan(notEffective(), view).isPresent());
        env.notAlive.clear();
        ledger = new ClusterLedger(ClusterLedger.FORMAT_VERSION, true,
                                   Map.of(Lane.SCHEMA_LAYOUT, new Claim(UUID.randomUUID(), "partition_split", "x", new NodeId(1), "CLAIMED", 0, 0, 1, Map.of())),
                                   List.of());
        assertFalse("the lane is claimed", policy.plan(pending(now[0]), view).isPresent());
        ledger = ClusterLedger.EMPTY;
        Action a = new Action(UUID.randomUUID(), "partition_split", ActionScope.CLUSTER, Subject.partition(KS, "u", "00"), Map.of(), null);
        records.add(new ActionRecord(a, ActionState.PROPOSED, now[0], null, "p", false, null, Map.of(), Map.of(), Map.of(), null, now[0]));
        assertFalse("a split is proposed: pending unsplits take the lane first", policy.plan(notEffective(), view).isPresent());
    }

    @Test
    public void rejectionCarriesTheEarliestTime()
    {
        assertEquals(1795052794028L, PartitionUnsplitActuator.earliestMillis("slow mode from 1795052794028: base still holds data").getAsLong());
        assertFalse(PartitionUnsplitActuator.earliestMillis("the key is not split").isPresent());
    }

    @Test
    public void reverseOrphanDetected()
    {
        ByteBuffer other = Int32Type.instance.decompose(8);
        KeyState reverse = new KeyState(Phase.REVERSE_VERIFY, Epoch.create(10), 1000, 1, 1, Epoch.create(10), ImmutableSet.of(), new UUID(7, 7), 500, ImmutableSet.of(),
                                        KeyState.BaseState.RESIDUE, 0, 0, KeyState.ReverseMode.FAST, 0);
        KeyState forward = new KeyState(Phase.NEW_ONLY, Epoch.create(10), 1000, 1, 1, Epoch.create(10), ImmutableSet.of(), new UUID(8, 8));
        LayoutHeader header = new LayoutHeader(new UUID(8, 1), LOGICAL, TableId.fromUUID(new UUID(99, 1)), KS, TABLE, "overflow", TableLayout.Transform.HASH_SPLIT,
                                               LayoutRegistry.HASH_ALGO, 8, 1);
        TableLayout layout = new TableLayout(header, 2, ImmutableMap.of(KEY, reverse, other, forward), 3);
        env.metadata = env.metadata.transformer().with(LayoutRegistryValue.KEY,
                                                      new LayoutRegistryValue(new LayoutRegistry(LayoutRegistry.FORMAT_VERSION, ImmutableMap.of(LOGICAL, layout)))).build().metadata;
        SplitOutcomeDetectors.UnsplitOrphaned detector = new SplitOutcomeDetectors.UnsplitOrphaned(() -> env.metadata);
        List<Diagnosis> found = detector.analyze(view);
        assertEquals("only the reverse key; the forward orphan is the split's", 1, found.size());
        assertEquals(KEY_HEX, found.get(0).evidence.get("key_hex"));
        Plan plan = policy.plan(found.get(0), view).get();
        assertEquals(PartitionUnsplitActuator.MODE_RESUME, plan.actions.get(0).params.get(PartitionUnsplitActuator.PARAM_MODE));
    }
}
