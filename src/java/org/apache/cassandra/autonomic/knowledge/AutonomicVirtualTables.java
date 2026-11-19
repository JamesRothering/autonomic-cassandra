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
package org.apache.cassandra.autonomic.knowledge;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.knowledge.KnowledgeStore.StoredDiagnosis;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Read-only virtual tables over the autonomic layer (overview 5.3). Each table is local to the queried node and
 * is selectable whether or not the layer is enabled: with the layer disabled it returns no rows or status only.
 */
public final class AutonomicVirtualTables
{
    private AutonomicVirtualTables()
    {
    }

    static final long DAY_MILLIS = 24L * 60 * 60 * 1000;
    static final long DIAGNOSES_LOOKBACK_MILLIS = DAY_MILLIS;
    static final long ACTIONS_LOOKBACK_MILLIS = 7 * DAY_MILLIS;
    static final int HISTORY_LIMIT = 1000;

    static final String NAME = "name";
    static final String VALUE = "value";

    public static Collection<VirtualTable> getAll(String keyspace)
    {
        return Arrays.asList(new StatusTable(keyspace),
                             new ComponentsTable(keyspace),
                             signals(keyspace, AutonomicService.instance::observationStore),
                             new DiagnosesTable(keyspace),
                             new ActionsTable(keyspace),
                             new ClusterLedgerTable(keyspace),
                             layouts(keyspace, ClusterMetadata::currentNullable));
    }

    /** The signals table over the given store source; package-private so tests can supply their own store. */
    @VisibleForTesting
    static VirtualTable signals(String keyspace, Supplier<ObservationStore> source)
    {
        return new SignalsTable(keyspace, source);
    }

    /** The layouts table over the given metadata source; public so that the layout tests can supply their own metadata. */
    @VisibleForTesting
    public static VirtualTable layouts(String keyspace, Supplier<ClusterMetadata> source)
    {
        return new LayoutsTable(keyspace, source);
    }

    private static int day(long millis)
    {
        return SimpleDateSerializer.timeInMillisToDay(millis);
    }

    /** Key/value view of {@link AutonomicService#getStatus()} plus the store counters. */
    static final class StatusTable extends AbstractVirtualTable
    {
        static final String TABLE_NAME = "autonomic_status";

        StatusTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, TABLE_NAME)
                               .comment("autonomic layer state and counters of this node")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn(NAME, UTF8Type.instance)
                               .addRegularColumn(VALUE, UTF8Type.instance)
                               .build());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            for (Map.Entry<String, String> e : AutonomicService.instance.getStatus().entrySet())
                result.row(e.getKey()).column(VALUE, e.getValue());
            KnowledgeStore knowledge = AutonomicService.instance.knowledgeStore();
            if (knowledge != null)
            {
                result.row("knowledge_write_failures").column(VALUE, String.valueOf(knowledge.writeFailures()));
                result.row("knowledge_read_failures").column(VALUE, String.valueOf(knowledge.readFailures()));
            }
            ObservationStore store = AutonomicService.instance.observationStore();
            if (store != null)
            {
                result.row("series_count").column(VALUE, String.valueOf(store.seriesCount()));
                result.row("series_evicted").column(VALUE, String.valueOf(store.evictions()));
            }
            return result;
        }
    }

    /** Failure accounting of every sensor, detector and policy. */
    static final class ComponentsTable extends AbstractVirtualTable
    {
        static final String TABLE_NAME = "autonomic_components";

        ComponentsTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, TABLE_NAME)
                               .comment("autonomic sensors, detectors and policies of this node")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn("kind", UTF8Type.instance)
                               .addClusteringColumn("name", UTF8Type.instance)
                               .addRegularColumn("enabled", BooleanType.instance)
                               .addRegularColumn("healthy", BooleanType.instance)
                               .addRegularColumn("consecutive_failures", Int32Type.instance)
                               .addRegularColumn("disabled_reason", UTF8Type.instance)
                               .build());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            for (AutonomicService.ComponentSnapshot c : AutonomicService.instance.componentSnapshots())
            {
                result.row(c.kind, c.name)
                      .column("enabled", !c.disabled)
                      .column("healthy", !c.disabled && c.consecutiveFailures == 0)
                      .column("consecutive_failures", c.consecutiveFailures)
                      .column("disabled_reason", c.disabled ? c.lastError : null);
            }
            return result;
        }
    }

    /** The newest point of every observation series. */
    static final class SignalsTable extends AbstractVirtualTable
    {
        static final String TABLE_NAME = "autonomic_signals";

        private final Supplier<ObservationStore> source;

        SignalsTable(String keyspace, Supplier<ObservationStore> source)
        {
            super(TableMetadata.builder(keyspace, TABLE_NAME)
                               .comment("latest autonomic observation per signal and subject")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn("signal", UTF8Type.instance)
                               .addClusteringColumn("subject", UTF8Type.instance)
                               .addRegularColumn("value", DoubleType.instance)
                               .addRegularColumn("at", TimestampType.instance)
                               .addRegularColumn("samples", Int32Type.instance)
                               .build());
            this.source = source;
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            ObservationStore store = source.get();
            if (store == null)
                return result;
            for (ObservationStore.SeriesSummary s : store.summaries())
            {
                result.row(s.signal.name, s.subject.asString())
                      .column("value", s.value)
                      .column("at", new Date(s.atMillis))
                      .column("samples", s.samples);
            }
            return result;
        }
    }

    /**
     * Open diagnoses from memory, then the stored diagnoses of the last 24 hours. A stored row that is still OPEN but
     * not active in memory is shown with state STALE and active = false.
     */
    static final class DiagnosesTable extends AbstractVirtualTable
    {
        static final String TABLE_NAME = "autonomic_diagnoses";

        DiagnosesTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, TABLE_NAME)
                               .comment("open and recent autonomic diagnoses of this node")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UUIDType.instance))
                               .addPartitionKeyColumn("id", UUIDType.instance)
                               .addRegularColumn("day", SimpleDateType.instance)
                               .addRegularColumn("type", UTF8Type.instance)
                               .addRegularColumn("subject", UTF8Type.instance)
                               .addRegularColumn("severity", UTF8Type.instance)
                               .addRegularColumn("confidence", DoubleType.instance)
                               .addRegularColumn("state", UTF8Type.instance)
                               .addRegularColumn("first_seen", TimestampType.instance)
                               .addRegularColumn("last_seen", TimestampType.instance)
                               .addRegularColumn("summary", UTF8Type.instance)
                               .addRegularColumn("evidence", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false))
                               .addRegularColumn("root_cause", UUIDType.instance)
                               .addRegularColumn("active", BooleanType.instance)
                               .build());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            Set<UUID> seen = new HashSet<>();
            for (Diagnosis d : AutonomicService.instance.activeDiagnoses())
            {
                seen.add(d.id);
                addRow(result, d, KnowledgeStore.STATE_OPEN, true);
            }
            KnowledgeStore knowledge = AutonomicService.instance.knowledgeStore();
            if (knowledge != null)
            {
                long now = Clock.Global.currentTimeMillis();
                for (StoredDiagnosis stored : knowledge.recentDiagnoses(now - DIAGNOSES_LOOKBACK_MILLIS, now, HISTORY_LIMIT))
                {
                    if (!seen.add(stored.diagnosis.id))
                        continue;
                    addRow(result, stored.diagnosis, stored.cleared ? KnowledgeStore.STATE_CLEARED : "STALE", false);
                }
            }
            return result;
        }

        private static void addRow(SimpleDataSet result, Diagnosis d, String state, boolean active)
        {
            result.row(d.id)
                  .column("day", day(d.firstSeenMillis))
                  .column("type", d.type)
                  .column("subject", d.subject.asString())
                  .column("severity", d.severity.name())
                  .column("confidence", d.confidence)
                  .column("state", state)
                  .column("first_seen", new Date(d.firstSeenMillis))
                  .column("last_seen", new Date(d.lastSeenMillis))
                  .column("summary", d.summary)
                  .column("evidence", d.evidence)
                  .column("root_cause", d.rootCause)
                  .column("active", active);
        }
    }

    /** Action records of the last 7 days, without the undo map. */
    static final class ActionsTable extends AbstractVirtualTable
    {
        static final String TABLE_NAME = "autonomic_actions";

        ActionsTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, TABLE_NAME)
                               .comment("recent autonomic actions of this node")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(TimeUUIDType.instance))
                               .addPartitionKeyColumn("id", TimeUUIDType.instance)
                               .addRegularColumn("day", SimpleDateType.instance)
                               .addRegularColumn("diagnosis_id", UUIDType.instance)
                               .addRegularColumn("policy", UTF8Type.instance)
                               .addRegularColumn("actuator", UTF8Type.instance)
                               .addRegularColumn("scope", UTF8Type.instance)
                               .addRegularColumn("target", UTF8Type.instance)
                               .addRegularColumn("params", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false))
                               .addRegularColumn("dry_run", BooleanType.instance)
                               .addRegularColumn("state", UTF8Type.instance)
                               .addRegularColumn("state_reason", UTF8Type.instance)
                               .addRegularColumn("baseline", MapType.getInstance(UTF8Type.instance, DoubleType.instance, false))
                               .addRegularColumn("result", MapType.getInstance(UTF8Type.instance, DoubleType.instance, false))
                               .addRegularColumn("verdict", UTF8Type.instance)
                               .addRegularColumn("proposed_at", TimestampType.instance)
                               .addRegularColumn("updated_at", TimestampType.instance)
                               .build());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            KnowledgeStore knowledge = AutonomicService.instance.knowledgeStore();
            if (knowledge == null)
                return result;
            long now = Clock.Global.currentTimeMillis();
            for (ActionRecord r : knowledge.recentActions(now - ACTIONS_LOOKBACK_MILLIS, now, HISTORY_LIMIT))
            {
                result.row(r.id())
                      .column("day", day(r.proposedMillis))
                      .column("diagnosis_id", r.diagnosisId)
                      .column("policy", r.policy)
                      .column("actuator", r.action.actuator)
                      .column("scope", r.action.scope.name())
                      .column("target", r.action.target.asString())
                      .column("params", r.action.params)
                      .column("dry_run", r.dryRun)
                      .column("state", r.state.name())
                      .column("state_reason", r.stateReason)
                      .column("baseline", r.baseline)
                      .column("result", r.result)
                      .column("verdict", r.verdict == null ? null : r.verdict.name())
                      .column("proposed_at", new Date(r.proposedMillis))
                      .column("updated_at", new Date(r.updatedMillis));
            }
            return result;
        }
    }

    /**
     * Claims of the cluster-wide action ledger (overview 5.3), one row per claimed lane, read from the cluster
     * metadata of this node whether or not the layer is enabled here. Empty while no lane is claimed.
     */
    static final class ClusterLedgerTable extends AbstractVirtualTable
    {
        static final String TABLE_NAME = "autonomic_cluster_ledger";

        ClusterLedgerTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, TABLE_NAME)
                               .comment("cluster-wide autonomic action ledger")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn("lane", UTF8Type.instance)
                               .addRegularColumn("action_id", TimeUUIDType.instance)
                               .addRegularColumn("actuator", UTF8Type.instance)
                               .addRegularColumn("target", UTF8Type.instance)
                               .addRegularColumn("owner", UUIDType.instance)
                               .addRegularColumn("phase", UTF8Type.instance)
                               .addRegularColumn("step", UTF8Type.instance)
                               .addRegularColumn("claimed_at", TimestampType.instance)
                               .addRegularColumn("lease_expires_at", TimestampType.instance)
                               .build());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            for (Map.Entry<Lane, Claim> e : ClusterLedger.of(ClusterMetadata.currentNullable()).claims.entrySet())
            {
                Claim claim = e.getValue();
                result.row(e.getKey().name())
                      .column("action_id", claim.actionId.version() == 1 ? TimeUUID.fromUuid(claim.actionId) : null)
                      .column("actuator", claim.actuator)
                      .column("target", claim.target)
                      .column("owner", claim.owner.toUUID())
                      .column("phase", claim.phase)
                      .column("step", String.valueOf(claim.step))
                      .column("claimed_at", new Date(claim.claimedAtMillis))
                      .column("lease_expires_at", new Date(claim.leaseExpiresAtMillis));
            }
            return result;
        }
    }

    /**
     * The adaptive-schema layout registry (design 2.3), one row per layout and split key, read from the cluster
     * metadata of this node whether or not the layer is enabled here. {@code orphan} is true when the logical table
     * of the layout no longer exists in the schema of that metadata.
     */
    static final class LayoutsTable extends AbstractVirtualTable
    {
        static final String TABLE_NAME = "autonomic_layouts";

        private final Supplier<ClusterMetadata> source;

        LayoutsTable(String keyspace, Supplier<ClusterMetadata> source)
        {
            super(TableMetadata.builder(keyspace, TABLE_NAME)
                               .comment("adaptive schema layouts: one row per logical table and split partition key")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn("keyspace_name", UTF8Type.instance)
                               .addClusteringColumn("table_name", UTF8Type.instance)
                               .addClusteringColumn("partition_key", UTF8Type.instance)
                               .addRegularColumn("layout_id", UUIDType.instance)
                               .addRegularColumn("physical_table", UTF8Type.instance)
                               .addRegularColumn("transform", UTF8Type.instance)
                               .addRegularColumn("buckets", Int32Type.instance)
                               .addRegularColumn("layout_version", LongType.instance)
                               .addRegularColumn("phase", UTF8Type.instance)
                               .addRegularColumn("epoch", LongType.instance)
                               .addRegularColumn("phase_since", TimestampType.instance)
                               .addRegularColumn("dual_write_since", TimestampType.instance)
                               .addRegularColumn("incarnation", Int32Type.instance)
                               .addRegularColumn("generation", Int32Type.instance)
                               .addRegularColumn("generation_epoch", LongType.instance)
                               .addRegularColumn("backfill_done", SetType.getInstance(UUIDType.instance, false))
                               .addRegularColumn("action_id", UUIDType.instance)
                               .addRegularColumn("orphan", BooleanType.instance)
                               .addRegularColumn("base_state", UTF8Type.instance)
                               .addRegularColumn("frozen_since", TimestampType.instance)
                               .addRegularColumn("reverse_mode", UTF8Type.instance)
                               .build());
            this.source = source;
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            ClusterMetadata metadata = source.get();
            if (metadata == null)
                return result;
            for (TableLayout layout : LayoutRegistry.of(metadata).byLogical.values())
            {
                boolean orphan = metadata.schema.getTableMetadata(layout.logical) == null;
                for (Map.Entry<ByteBuffer, KeyState> e : layout.keys.entrySet())
                {
                    KeyState key = e.getValue();
                    Set<UUID> done = new HashSet<>();
                    for (NodeId node : key.backfillDone)
                        done.add(node.toUUID());
                    result.row(layout.keyspace, layout.logicalName, ByteBufferUtil.bytesToHex(e.getKey()))
                          .column("layout_id", layout.layoutId)
                          .column("physical_table", layout.physicalName)
                          .column("transform", layout.transform.name())
                          .column("buckets", layout.buckets)
                          .column("layout_version", layout.version)
                          .column("phase", key.phase.name())
                          .column("epoch", key.phaseEpoch.getEpoch())
                          .column("phase_since", new Date(key.phaseSinceMillis))
                          .column("dual_write_since", key.dualWriteSinceMillis > 0 ? new Date(key.dualWriteSinceMillis) : null)
                          .column("incarnation", key.incarnation)
                          .column("generation", key.generation)
                          .column("generation_epoch", key.generationEpoch.getEpoch())
                          .column("backfill_done", done)
                          .column("action_id", key.actionId)
                          .column("orphan", orphan)
                          .column("base_state", key.baseState.name())
                          .column("frozen_since", key.frozenSinceMillis > 0 ? new Date(key.frozenSinceMillis) : null)
                          .column("reverse_mode", key.reverseMode == null ? null : key.reverseMode.name());
                }
            }
            return result;
        }
    }
}
