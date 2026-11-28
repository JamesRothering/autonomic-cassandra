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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.PartitionSplitActuator;
import org.apache.cassandra.autonomic.execute.actuators.PartitionUnsplitActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Proposes {@code partition_unsplit} (adaptive-schema design 11.8, ADR-036, T6.17g) on three diagnoses:
 * <ul>
 * <li>{@code layout.split_not_effective}: the split of the key ended neutral or regressed; propose an unsplit of it;</li>
 * <li>{@code layout.unsplit_pending}: an operator asked for an unsplit {@code --when-ready}; propose it once the time
 *     the request carries has come;</li>
 * <li>{@code layout.unsplit_orphaned}: a key in a reverse phase whose action holds no claim; propose {@code mode resume},
 *     the counterpart of the adoption of an orphaned split.</li>
 * </ul>
 * The unsplit actuator is held for approval whatever {@code cluster_approval} is ({@link PartitionUnsplitActuator#requiresManualApproval}),
 * so nothing this policy proposes is ever approved automatically; the plan's rationale states the mode and the estimate.
 * One layout migration at a time: nothing while the lane is claimed or an action of the split or unsplit actuator is
 * proposed or running, a member is down or a metadata operation runs. The proposal is checked with the actuator's
 * {@code validate} first, so a key that cannot be unsplit yet (its mode window is not open) gets no plan. After each abort of the key the wait
 * doubles, as for the split policy.
 */
public class PartitionUnsplitPolicy implements Policy
{
    public static final String NAME = "partition_unsplit";
    static final long LOOKBACK_MILLIS = 24L * 3_600_000;

    /** What the policy needs from the node; replaced in unit tests. */
    public interface Env
    {
        ClusterMetadata metadata();

        /** True if this node is the first live node of the directory (the proposer of cluster actions). */
        boolean preferredProposer();

        /** Directory members that gossip does not consider alive. */
        Collection<NodeId> membersNotAlive();

        /** The actuator's {@code validate}: a rejection, or empty if the action can be proposed now. */
        Optional<String> validate(Action action, KnowledgeView knowledge);

        /** What the actuator says of the action now: the mode and the estimate. */
        String note(Action action);
    }

    private final AutonomicConfig config;
    private final Env env;

    public PartitionUnsplitPolicy(AutonomicConfig config, Env env)
    {
        this.config = config;
        this.env = env;
    }

    public static PartitionUnsplitPolicy create(AutonomicConfig config, ClusterCoordinator coordinator, Function<String, Actuator> actuators)
    {
        return new PartitionUnsplitPolicy(config, new NodeEnv(coordinator, actuators));
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<String> handles()
    {
        return Set.of(PartitionUnsplitActuator.SPLIT_NOT_EFFECTIVE, PartitionUnsplitActuator.UNSPLIT_PENDING, PartitionUnsplitActuator.UNSPLIT_ORPHANED);
    }

    @Override
    public Set<SignalId> inputs()
    {
        return Set.of(Signals.TABLE_COORDINATOR_READ_LATENCY_P99_MICROS);
    }

    @Override
    public Set<String> actuations()
    {
        return Set.of(Policy.actuation(PartitionUnsplitActuator.NAME, Policy.APPLY));
    }

    @Override
    public Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        String type = diagnosis.type;
        boolean pending = PartitionUnsplitActuator.UNSPLIT_PENDING.equals(type);
        boolean orphaned = PartitionUnsplitActuator.UNSPLIT_ORPHANED.equals(type);
        // a pending request is the proposal of the node the operator asked; the others are the preferred proposer's
        if (!pending && !env.preferredProposer())
            return Optional.empty();
        String keyHex = diagnosis.evidence.get("key_hex");
        if (keyHex == null)
            return Optional.empty();
        try
        {
            ByteBufferUtil.hexToBytes(keyHex);
        }
        catch (RuntimeException e)
        {
            return Optional.empty();
        }
        Subject subject = diagnosis.subject;
        ClusterMetadata cm = env.metadata();
        TableMetadata table = cm.schema.getTableMetadata(subject.keyspace, subject.table);
        if (table == null || busy(knowledge) || !env.membersNotAlive().isEmpty() || !cm.inProgressSequences.isEmpty())
            return Optional.empty();
        long now = knowledge.nowMillis();
        if (pending)
        {
            String due = diagnosis.evidence.get("due_millis");
            try
            {
                if (due == null || now < Long.parseLong(due))
                    return Optional.empty();
            }
            catch (NumberFormatException e)
            {
                return Optional.empty();
            }
        }
        Subject target = PartitionSplitActuator.target(table.keyspace, table.name, keyHex);
        if (!orphaned && coolingDown(knowledge, target))
            return Optional.empty();

        Map<String, String> params = PartitionUnsplitActuator.params(orphaned ? PartitionUnsplitActuator.MODE_RESUME : PartitionUnsplitActuator.MODE_AUTO);
        VerifySpec verify = new VerifySpec(Signals.TABLE_COORDINATOR_READ_LATENCY_P99_MICROS, Subject.table(table.keyspace, table.name), true, 0.0,
                                           List.of(), 0, 0);
        Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), PartitionUnsplitActuator.NAME, ActionScope.CLUSTER, target, params, verify);
        if (env.validate(action, knowledge).isPresent())
            return Optional.empty();
        String what = orphaned ? "partition " + keyHex + " of " + table.keyspace + '.' + table.name + " is in a reverse phase with no owner: "
                      : pending ? "an operator asked for the unsplit of partition " + keyHex + " of " + table.keyspace + '.' + table.name + " when it is possible: "
                                : "the split of partition " + keyHex + " of " + table.keyspace + '.' + table.name + " did not help: unsplit it; ";
        return Optional.of(new Plan(UUID.randomUUID(), diagnosis.id, NAME, List.of(action), what + env.note(action) + "; held for approval"));
    }

    /** One layout migration at a time, and none while an action of either actuator is proposed or running. */
    private static boolean busy(KnowledgeView knowledge)
    {
        if (knowledge.ledger().claims.containsKey(Lane.SCHEMA_LAYOUT))
            return true;
        for (ActionRecord r : knowledge.recentActions(LOOKBACK_MILLIS))
            if ((r.action.actuator.equals(PartitionSplitActuator.NAME) || r.action.actuator.equals(PartitionUnsplitActuator.NAME)) && !r.state.isTerminal())
                return true;
        return false;
    }

    /**
     * After each abort of an unsplit of the key the wait doubles, from {@code cooldown_after_rollback} up to
     * {@code cooldown_max}; and a proposal that was rejected is not made again within {@code cooldown_after_rollback}.
     */
    private boolean coolingDown(KnowledgeView knowledge, Subject target)
    {
        long now = knowledge.nowMillis();
        long base = config.safety.cooldown_after_rollback.toMilliseconds();
        long max = config.safety.cooldown_max.toMilliseconds();
        int aborts = 0;
        long last = 0;
        long rejected = 0;
        for (ActionRecord r : knowledge.recentActions(LOOKBACK_MILLIS))
        {
            if (!r.action.actuator.equals(PartitionUnsplitActuator.NAME) || !r.action.target.equals(target))
                continue;
            if (r.state == ActionState.ROLLED_BACK || r.state == ActionState.FAILED)
            {
                aborts++;
                last = Math.max(last, r.updatedMillis);
            }
            else if (r.state == ActionState.REJECTED)
            {
                rejected = Math.max(rejected, r.updatedMillis);
            }
        }
        if (now < rejected + base)
            return true;
        if (aborts == 0)
            return false;
        return now < last + Math.min(max, base << Math.min(aborts - 1, 20));
    }

    static final class NodeEnv implements Env
    {
        private final ClusterCoordinator coordinator;
        private final Function<String, Actuator> actuators;

        NodeEnv(ClusterCoordinator coordinator, Function<String, Actuator> actuators)
        {
            this.coordinator = coordinator;
            this.actuators = actuators;
        }

        @Override
        public ClusterMetadata metadata()
        {
            return ClusterMetadata.current();
        }

        @Override
        public boolean preferredProposer()
        {
            return coordinator.preferredProposer();
        }

        @Override
        public Collection<NodeId> membersNotAlive()
        {
            return new ArrayList<>(coordinator.membersNotAlive());
        }

        @Override
        public Optional<String> validate(Action action, KnowledgeView knowledge)
        {
            Actuator actuator = actuators.apply(PartitionUnsplitActuator.NAME);
            return actuator == null ? Optional.of("the partition_unsplit actuator is not registered") : actuator.validate(action, knowledge);
        }

        @Override
        public String note(Action action)
        {
            Actuator actuator = actuators.apply(PartitionUnsplitActuator.NAME);
            return actuator instanceof PartitionUnsplitActuator ? ((PartitionUnsplitActuator) actuator).modeNote(action) : "";
        }
    }
}
