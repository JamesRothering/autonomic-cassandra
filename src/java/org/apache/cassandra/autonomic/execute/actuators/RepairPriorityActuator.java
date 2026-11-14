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

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.ClusterActuator;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;

/**
 * Asks the auto-repair scheduler to repair one host next, by adding it to the repair priority of
 * {@code system_distributed.auto_repair_priority}. MAINTENANCE lane, so at most one such action runs in the cluster and
 * only under a ledger claim. Params: {@code repair_type} and {@code host_id}; the target is {@code Subject.peer(host_id)}.
 *
 * <p>The actuator owns its verification (FC4): the claim goes {@code CLAIMED} (write the priority), then
 * {@code PRIORITY_SET} (read it back), then the action ends {@code COMMITTED} with verdict IMPROVED, or INCONCLUSIVE if
 * the entry is already gone. Nothing is rolled back: no public API removes a priority entry, and the scheduler clears
 * it when the host has repaired, so the actuator is irreversible. It is a hint that only reorders work the scheduler
 * would do anyway, so under manual approval, the default for cluster actions, it is not limited to severity CRITICAL.
 */
public class RepairPriorityActuator implements ClusterActuator
{
    public static final String NAME = "repair_priority";
    public static final String PARAM_TYPE = "repair_type";
    public static final String PARAM_HOST = "host_id";
    public static final String PHASE_SET = "PRIORITY_SET";

    /** The scheduler operations used; replaced in unit tests. */
    public interface Scheduler
    {
        /** Auto-repair scheduling of this type is enabled in configuration. */
        boolean enabled(RepairType type);

        /** Broadcast address of the host, or empty if it is not in the cluster directory. */
        Optional<String> address(UUID hostId);

        /** Adds the host to the repair priority of this type. */
        void prioritise(RepairType type, String address);

        boolean prioritised(RepairType type, UUID hostId);
    }

    private final Scheduler scheduler;

    public RepairPriorityActuator(Scheduler scheduler)
    {
        this.scheduler = scheduler;
    }

    public static RepairPriorityActuator create()
    {
        return new RepairPriorityActuator(new Scheduler()
        {
            public boolean enabled(RepairType type)
            {
                return AutoRepairService.instance.getAutoRepairConfig() != null
                       && AutoRepairService.instance.getAutoRepairConfig().isAutoRepairEnabled(type);
            }

            public Optional<String> address(UUID hostId)
            {
                Directory directory = ClusterMetadata.current().directory;
                NodeId id = directory.nodeIdFromHostId(hostId);
                if (id == null || directory.getNodeAddresses(id) == null)
                    return Optional.empty();
                InetAddressAndPort address = directory.getNodeAddresses(id).broadcastAddress;
                return Optional.of(address.getHostAddressAndPort());
            }

            public void prioritise(RepairType type, String address)
            {
                AutoRepairService.instance.setRepairPriorityForHosts(type.name(), address);
            }

            public boolean prioritised(RepairType type, UUID hostId)
            {
                return AutoRepairUtils.getPriorityHostIds(type).contains(hostId);
            }
        });
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Lane lane()
    {
        return Lane.MAINTENANCE;
    }

    @Override
    public boolean reversible()
    {
        return false;
    }

    public static Map<String, String> params(RepairType type, String hostId)
    {
        return ImmutableMap.of(PARAM_TYPE, type.name(), PARAM_HOST, hostId);
    }

    @Override
    public Optional<String> validate(Action action, KnowledgeView knowledge)
    {
        Subject t = action.target;
        if (t.kind != Subject.Kind.PEER || t.detail == null || !t.detail.equals(action.params.get(PARAM_HOST)))
            return Optional.of("target must be the host named by " + PARAM_HOST);
        RepairType type;
        UUID host;
        try
        {
            type = RepairType.parse(action.params.get(PARAM_TYPE));
            host = UUID.fromString(t.detail);
        }
        catch (RuntimeException e)
        {
            return Optional.of("invalid " + PARAM_TYPE + " or " + PARAM_HOST);
        }
        if (!scheduler.enabled(type))
            return Optional.of("auto-repair of type " + type + " is disabled in configuration");
        if (!scheduler.address(host).isPresent())
            return Optional.of("host " + host + " is not in the cluster directory");
        return Optional.empty();
    }

    @Override
    public StepResult step(Action action, Claim claim, ActionContext ctx)
    {
        RepairType type = RepairType.parse(action.params.get(PARAM_TYPE));
        UUID host = UUID.fromString(action.params.get(PARAM_HOST));
        if (PHASE_SET.equals(claim.phase))
        {
            if (scheduler.prioritised(type, host))
                return StepResult.committed(Verdict.IMPROVED, "host " + host + " has repair priority for " + type);
            return StepResult.committed(Verdict.INCONCLUSIVE, "priority of host " + host + " is not visible; the scheduler may have consumed it");
        }
        if (!scheduler.enabled(type))
            return StepResult.rolledBack("auto-repair of type " + type + " is disabled in configuration");
        Optional<String> address = scheduler.address(host);
        if (!address.isPresent())
            return StepResult.rolledBack("host " + host + " is not in the cluster directory");
        scheduler.prioritise(type, address.get());
        return StepResult.advance(PHASE_SET, params(type, host.toString()), 0);
    }

    /** Before {@code PRIORITY_SET} nothing was written, or only a harmless hint; after it there is nothing to undo. */
    @Override
    public StepResult recover(Claim claim, ActionContext ctx)
    {
        if (PHASE_SET.equals(claim.phase))
            return StepResult.committedCleanupPending("priority was set; nothing to undo");
        return StepResult.rolledBack("recovered before the priority was confirmed");
    }

    @Override
    public boolean pastCommitPoint(Claim claim)
    {
        return PHASE_SET.equals(claim.phase);
    }
}
