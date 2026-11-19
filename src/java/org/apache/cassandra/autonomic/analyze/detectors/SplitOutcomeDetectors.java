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
package org.apache.cassandra.autonomic.analyze.detectors;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.execute.actuators.PartitionSplitActuator;
import org.apache.cassandra.autonomic.execute.actuators.PartitionUnsplitActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutEvents;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The outcomes of a partition split that an operator should look at (adaptive-schema design 7.3), as WARN diagnoses of
 * the key's partition subject that stay open for {@link #OPEN_MILLIS} after the outcome. {@code PartitionUnsplitPolicy}
 * proposes an unsplit (manual approval) for {@code layout.split_not_effective}, {@code layout.unsplit_pending} and
 * {@code layout.unsplit_orphaned}; {@code partition_split} proposes the reclaim (ADR-036).
 */
public final class SplitOutcomeDetectors
{
    public static final String VERIFY_FAILED = "layout.verify_failed";
    public static final String SPLIT_NOT_EFFECTIVE = "layout.split_not_effective";
    public static final String ORPHANED = PartitionSplitActuator.ORPHANED;
    public static final String RECLAIM_DUE = PartitionSplitActuator.RECLAIM_DUE;
    /** A slow reverse migration aborted on {@code purge_timeout}: a base replica's partition did not purge clean (T6.17f). */
    public static final String UNSPLIT_RESIDUE = "layout.unsplit_residue";
    public static final String UNSPLIT_PENDING = PartitionUnsplitActuator.UNSPLIT_PENDING;
    public static final String UNSPLIT_ORPHANED = PartitionUnsplitActuator.UNSPLIT_ORPHANED;
    public static final long OPEN_MILLIS = 24L * 3_600_000;

    private SplitOutcomeDetectors()
    {
    }

    private static UUID id(String type, UUID actionId)
    {
        return UUID.nameUUIDFromBytes((type + ':' + actionId).getBytes(StandardCharsets.UTF_8));
    }

    /** The equivalence verification of a migration failed beyond its retries and the migration was aborted. */
    public static final class VerifyFailed implements Detector
    {
        @Override
        public String name()
        {
            return VERIFY_FAILED;
        }

        @Override
        public Set<SignalId> requires()
        {
            return Collections.emptySet();
        }

        @Override
        public List<Diagnosis> analyze(KnowledgeView knowledge)
        {
            long now = knowledge.nowMillis();
            List<Diagnosis> out = new ArrayList<>();
            for (LayoutEvents.VerifyFailure f : LayoutEvents.verifyFailures())
            {
                if (now - f.atMillis > OPEN_MILLIS)
                    continue;
                Map<String, String> evidence = new LinkedHashMap<>();
                evidence.put("key_hex", f.keyHex);
                evidence.put("table", f.keyspace + '.' + f.table);
                evidence.put("action", f.actionId.toString());
                evidence.put("verification", f.detail);
                out.add(new Diagnosis(id(VERIFY_FAILED, f.actionId), VERIFY_FAILED, Subject.partition(f.keyspace, f.table, f.keyHex), Severity.WARN, 1.0,
                                      f.atMillis, now, "The split of partition 0x" + f.keyHex + " of " + f.keyspace + '.' + f.table
                                                       + " was aborted: its overflow copy did not verify equal to the base copy", evidence, null));
            }
            return out;
        }
    }

    /** {@value #UNSPLIT_RESIDUE}: each base replica's purge reason and what the operator can do (owner answer 3). */
    public static final class UnsplitResidue implements Detector
    {
        @Override
        public String name()
        {
            return UNSPLIT_RESIDUE;
        }

        @Override
        public Set<SignalId> requires()
        {
            return Collections.emptySet();
        }

        @Override
        public List<Diagnosis> analyze(KnowledgeView knowledge)
        {
            long now = knowledge.nowMillis();
            List<Diagnosis> out = new ArrayList<>();
            for (LayoutEvents.UnsplitResidue r : LayoutEvents.unsplitResidues())
            {
                if (now - r.atMillis > OPEN_MILLIS)
                    continue;
                Map<String, String> evidence = new LinkedHashMap<>();
                evidence.put("key_hex", r.keyHex);
                evidence.put("table", r.keyspace + '.' + r.table);
                evidence.put("action", r.actionId.toString());
                r.nodes.forEach((node, reason) -> evidence.put("node." + node, reason));
                if (r.largestResidueTimestamp != Long.MIN_VALUE)
                    evidence.put("largest_residue_timestamp", Long.toString(r.largestResidueTimestamp));
                evidence.put("remedy", r.remedy);
                out.add(new Diagnosis(id(UNSPLIT_RESIDUE, r.actionId), UNSPLIT_RESIDUE, Subject.partition(r.keyspace, r.table, r.keyHex), Severity.WARN, 1.0,
                                      r.atMillis, now, "The slow unsplit of partition 0x" + r.keyHex + " of " + r.keyspace + '.' + r.table
                                                       + " was aborted: the base partition did not purge clean on every replica. " + r.remedy,
                                      evidence, null));
            }
            return out;
        }
    }

    /** Stage B of a split ended NEUTRAL or REGRESSED: the key stays split. */
    public static final class SplitNotEffective implements Detector
    {
        @Override
        public String name()
        {
            return SPLIT_NOT_EFFECTIVE;
        }

        @Override
        public Set<SignalId> requires()
        {
            return Collections.emptySet();
        }

        @Override
        public List<Diagnosis> analyze(KnowledgeView knowledge)
        {
            long now = knowledge.nowMillis();
            List<Diagnosis> out = new ArrayList<>();
            for (ActionRecord r : knowledge.recentActions(OPEN_MILLIS))
            {
                if (!r.action.actuator.equals(PartitionSplitActuator.NAME) || r.state != ActionState.COMMITTED
                    || (r.verdict != Verdict.NEUTRAL && r.verdict != Verdict.REGRESSED))
                    continue;
                Subject target = r.action.target;
                Map<String, String> evidence = new LinkedHashMap<>();
                evidence.put("key_hex", String.valueOf(target.detail));
                evidence.put("table", target.keyspace + '.' + target.table);
                evidence.put("action", r.action.id.toString());
                evidence.put("verdict", r.verdict.name());
                out.add(new Diagnosis(id(SPLIT_NOT_EFFECTIVE, r.action.id), SPLIT_NOT_EFFECTIVE, target, Severity.WARN, 1.0,
                                      r.updatedMillis, now, "The split of partition 0x" + target.detail + " of " + target.keyspace + '.' + target.table
                                                            + " ended " + r.verdict + ": it stays split until it is unsplit (`nodetool autonomic layout unsplit`, which needs approval)",
                                      evidence, null));
            }
            return out;
        }
    }

    /**
     * A key past its commit point that no action owns (T6.9 review): open while it is orphaned. CRITICAL in NEW_ONLY
     * and CLEANUP, where its reconcile pass must run before the replicas' tombstone deadline (ADR-031); WARN in STEADY,
     * where only the base partition tombstone is missing. The policy {@code partition_split} proposes its adoption on
     * the preferred proposer. Reads the metadata this node has, so every node raises it.
     */
    public static final class Orphaned implements Detector
    {
        private final Supplier<ClusterMetadata> metadata;
        private final LongSupplier writeTimeoutMillis;
        /** First time each orphan was seen here, by diagnosis id. */
        private final Map<UUID, Long> firstSeen = new HashMap<>();

        public Orphaned(Supplier<ClusterMetadata> metadata, LongSupplier writeTimeoutMillis)
        {
            this.metadata = metadata;
            this.writeTimeoutMillis = writeTimeoutMillis;
        }

        @Override
        public String name()
        {
            return ORPHANED;
        }

        @Override
        public Set<SignalId> requires()
        {
            return Collections.emptySet();
        }

        @Override
        public List<Diagnosis> analyze(KnowledgeView knowledge)
        {
            ClusterMetadata cm = metadata.get();
            if (cm == null)
                return Collections.emptyList();
            long now = knowledge.nowMillis();
            List<Diagnosis> out = new ArrayList<>();
            Map<UUID, Long> seen = new HashMap<>();
            for (PartitionSplitActuator.Orphan o : PartitionSplitActuator.orphans(cm, now, writeTimeoutMillis.getAsLong()))
            {
                UUID id = id(ORPHANED, UUID.nameUUIDFromBytes((o.layout.layoutId + ":" + o.keyHex() + ':' + o.state.incarnation + ':' + o.state.actionId)
                                                              .getBytes(StandardCharsets.UTF_8)));
                long first = firstSeen.getOrDefault(id, now);
                seen.put(id, first);
                boolean steady = o.state.phase == Phase.STEADY;
                Map<String, String> evidence = new LinkedHashMap<>();
                evidence.put("key_hex", o.keyHex());
                evidence.put("table", o.layout.keyspace + '.' + o.layout.logicalName);
                evidence.put("phase", o.state.phase.name());
                evidence.put("previous_action", String.valueOf(o.state.actionId));
                evidence.put("past_tombstone_deadline", Boolean.toString(o.pastLimit));
                String summary = "Partition 0x" + o.keyHex() + " of " + o.layout.keyspace + '.' + o.layout.logicalName + " is past its split's commit point in "
                                 + o.state.phase + " and no action owns it" + (steady ? ": its base partition tombstone was not written"
                                                                                      : o.pastLimit ? ": its reconcile pass can no longer run; it needs an operator"
                                                                                                    : ": its reconcile pass must run before the tombstone deadline");
                out.add(new Diagnosis(id, ORPHANED, Subject.partition(o.layout.keyspace, o.layout.logicalName, o.keyHex()),
                                      steady ? Severity.WARN : Severity.CRITICAL, 1.0, first, now, summary, evidence, null));
            }
            firstSeen.keySet().retainAll(seen.keySet());
            firstSeen.putAll(seen);
            return out;
        }
    }

    /**
     * A split key whose action has released it and whose base partition still holds the pre-split data, once the time
     * after which a fast reverse could no longer be completed has passed (ADR-036, design 11.5). The policy
     * {@code partition_split} proposes the {@code reclaim} on the preferred proposer. Never raised for a key that is
     * owned or in a reverse phase: only a released {@code STEADY} key in base state {@code RESIDUE} qualifies.
     */
    public static final class ReclaimDue implements Detector
    {
        private final Supplier<ClusterMetadata> metadata;
        private final Supplier<AutonomicConfig.LayoutConfig> config;

        public ReclaimDue(Supplier<ClusterMetadata> metadata, Supplier<AutonomicConfig.LayoutConfig> config)
        {
            this.metadata = metadata;
            this.config = config;
        }

        @Override
        public String name()
        {
            return RECLAIM_DUE;
        }

        @Override
        public Set<SignalId> requires()
        {
            return Collections.emptySet();
        }

        @Override
        public List<Diagnosis> analyze(KnowledgeView knowledge)
        {
            ClusterMetadata cm = metadata.get();
            if (cm == null)
                return Collections.emptyList();
            long now = knowledge.nowMillis();
            List<Diagnosis> out = new ArrayList<>();
            for (PartitionSplitActuator.Orphan o : PartitionSplitActuator.reclaimsDue(cm, now, config.get()))
            {
                UUID id = id(RECLAIM_DUE, UUID.nameUUIDFromBytes((o.layout.layoutId + ":" + o.keyHex() + ':' + o.state.incarnation)
                                                                 .getBytes(StandardCharsets.UTF_8)));
                Map<String, String> evidence = new LinkedHashMap<>();
                evidence.put("key_hex", o.keyHex());
                evidence.put("table", o.layout.keyspace + '.' + o.layout.logicalName);
                evidence.put("frozen_since", Long.toString(o.state.frozenSinceMillis));
                out.add(new Diagnosis(id, RECLAIM_DUE, Subject.partition(o.layout.keyspace, o.layout.logicalName, o.keyHex()), Severity.INFO, 1.0, now, now,
                                      "The base partition of 0x" + o.keyHex() + " of " + o.layout.keyspace + '.' + o.layout.logicalName
                                      + " still holds its pre-split data and its reclaim is due", evidence, null));
            }
            return out;
        }
    }

    /**
     * An operator's {@code layout unsplit --when-ready} that could not be proposed yet (T6.17g, design 11.8): one INFO
     * diagnosis per pending request, carrying the time from which an unsplit is possible ({@code due_millis}).
     * {@code PartitionUnsplitPolicy} proposes the action from that time. A request is dropped once its key is no
     * longer a released {@code STEADY} key that is waiting for it (the unsplit began, or the key is gone). Requests are
     * kept by the node the operator asked, in its local state.
     */
    public static final class UnsplitPending implements Detector
    {
        private final Supplier<ClusterMetadata> metadata;
        private final Supplier<Map<String, String>> requests;
        private final Consumer<String> drop;

        /**
         * @param requests the pending requests by state key ({@link PartitionUnsplitActuator#PENDING_PREFIX} and
         *                 {@code <ks>.<table>/<key hex>}), each with its due time as the value
         * @param drop removes a request by its state key
         */
        public UnsplitPending(Supplier<ClusterMetadata> metadata, Supplier<Map<String, String>> requests, Consumer<String> drop)
        {
            this.metadata = metadata;
            this.requests = requests;
            this.drop = drop;
        }

        @Override
        public String name()
        {
            return UNSPLIT_PENDING;
        }

        @Override
        public Set<SignalId> requires()
        {
            return Collections.emptySet();
        }

        @Override
        public List<Diagnosis> analyze(KnowledgeView knowledge)
        {
            ClusterMetadata cm = metadata.get();
            if (cm == null)
                return Collections.emptyList();
            long now = knowledge.nowMillis();
            List<Diagnosis> out = new ArrayList<>();
            for (Map.Entry<String, String> e : requests.get().entrySet())
            {
                String rest = e.getKey().substring(PartitionUnsplitActuator.PENDING_PREFIX.length());
                int slash = rest.lastIndexOf('/');
                int dot = rest.indexOf('.');
                long due;
                try
                {
                    due = Long.parseLong(e.getValue());
                }
                catch (NumberFormatException ex)
                {
                    drop.accept(e.getKey());
                    continue;
                }
                if (slash < 0 || dot < 0 || dot > slash)
                {
                    drop.accept(e.getKey());
                    continue;
                }
                String keyspace = rest.substring(0, dot);
                String table = rest.substring(dot + 1, slash);
                String keyHex = rest.substring(slash + 1);
                if (!waiting(cm, keyspace, table, keyHex))
                {
                    drop.accept(e.getKey());
                    continue;
                }
                Map<String, String> evidence = new LinkedHashMap<>();
                evidence.put("key_hex", keyHex);
                evidence.put("table", keyspace + '.' + table);
                evidence.put("due_millis", Long.toString(due));
                out.add(new Diagnosis(UUID.nameUUIDFromBytes((UNSPLIT_PENDING + ':' + e.getKey()).getBytes(StandardCharsets.UTF_8)), UNSPLIT_PENDING,
                                      Subject.partition(keyspace, table, keyHex), Severity.INFO, 1.0, now, now,
                                      "An unsplit of partition 0x" + keyHex + " of " + keyspace + '.' + table + " was asked for and is proposed from " + due, evidence, null));
            }
            return out;
        }

        /** True if the key is still in the layout in a phase an unsplit starts from. */
        private static boolean waiting(ClusterMetadata cm, String keyspace, String table, String keyHex)
        {
            org.apache.cassandra.schema.TableMetadata logical = cm.schema.getTableMetadata(keyspace, table);
            TableLayout layout = logical == null ? null : LayoutRegistry.of(cm).layout(logical.id);
            KeyState state;
            try
            {
                state = layout == null ? null : layout.keyState(ByteBufferUtil.hexToBytes(keyHex));
            }
            catch (RuntimeException e)
            {
                return false;
            }
            return state != null && !state.phase.isReverse();
        }
    }

    /**
     * A key in a reverse phase whose action holds no claim (T6.17g): the counterpart of {@link Orphaned}. Open while
     * it is orphaned; CRITICAL past the reverse commit point, where the migration must be finished, WARN before it,
     * where a resume aborts it. {@code PartitionUnsplitPolicy} proposes {@code mode resume} on the preferred proposer.
     */
    public static final class UnsplitOrphaned implements Detector
    {
        private final Supplier<ClusterMetadata> metadata;

        public UnsplitOrphaned(Supplier<ClusterMetadata> metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public String name()
        {
            return UNSPLIT_ORPHANED;
        }

        @Override
        public Set<SignalId> requires()
        {
            return Collections.emptySet();
        }

        @Override
        public List<Diagnosis> analyze(KnowledgeView knowledge)
        {
            ClusterMetadata cm = metadata.get();
            if (cm == null)
                return Collections.emptyList();
            long now = knowledge.nowMillis();
            List<Diagnosis> out = new ArrayList<>();
            for (PartitionSplitActuator.Orphan o : PartitionUnsplitActuator.orphans(cm))
            {
                UUID id = id(UNSPLIT_ORPHANED, UUID.nameUUIDFromBytes((o.layout.layoutId + ":" + o.keyHex() + ':' + o.state.incarnation + ':' + o.state.actionId)
                                                                      .getBytes(StandardCharsets.UTF_8)));
                Map<String, String> evidence = new LinkedHashMap<>();
                evidence.put("key_hex", o.keyHex());
                evidence.put("table", o.layout.keyspace + '.' + o.layout.logicalName);
                evidence.put("phase", o.state.phase.name());
                evidence.put("previous_action", String.valueOf(o.state.actionId));
                boolean past = o.state.phase.pastCommit();
                out.add(new Diagnosis(id, UNSPLIT_ORPHANED, Subject.partition(o.layout.keyspace, o.layout.logicalName, o.keyHex()),
                                      past ? Severity.CRITICAL : Severity.WARN, 1.0, now, now,
                                      "Partition 0x" + o.keyHex() + " of " + o.layout.keyspace + '.' + o.layout.logicalName + " is in the reverse phase " + o.state.phase
                                      + " and no action owns it" + (past ? ": the unsplit is past its commit point and must be finished" : ": resuming it aborts it"),
                                      evidence, null));
            }
            return out;
        }
    }
}
