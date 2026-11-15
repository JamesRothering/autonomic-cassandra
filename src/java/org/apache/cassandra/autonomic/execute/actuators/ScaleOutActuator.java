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

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.elastic.ProviderCalls;
import org.apache.cassandra.autonomic.elastic.ProviderException;
import org.apache.cassandra.autonomic.elastic.ProvisionStatus;
import org.apache.cassandra.autonomic.elastic.ProvisionTicket;
import org.apache.cassandra.autonomic.elastic.ScaleOutProvider;
import org.apache.cassandra.autonomic.elastic.ScaleOutProvider.Capabilities;
import org.apache.cassandra.autonomic.elastic.ScaleOutProviders;
import org.apache.cassandra.autonomic.elastic.ScaleRequest;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.ClusterActionExecution;
import org.apache.cassandra.autonomic.execute.ClusterActuator;
import org.apache.cassandra.autonomic.execute.MetricVerifier;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;

/**
 * Adds one node through the {@link ScaleOutProvider} (overview 8.3). TOPOLOGY lane: at most one in the cluster, only
 * under a ledger claim, manual approval by default. The target is {@code Subject.cluster()}; params
 * {@code datacenter}, {@code rack} and {@code reason}.
 *
 * <p>Everything a step needs is in the claim's state, so any node can continue after the owner is lost. Phases:
 * <ul>
 * <li>{@code CLAIMED}: nothing is asked of the provider yet. The request id (the id of the proposed action, which is
 *     the provider's idempotency key and survives a takeover, unlike the claim's action id), the placement and the
 *     baseline of the goal are written to the claim: phase {@code REQUESTED}. This is the write-ahead of the request.</li>
 * <li>{@code REQUESTED}: {@code provider.requestNode}, repeated with the same request id until it answers; the ticket
 *     goes into the claim: phase {@code PROVISIONING}. An owner lost between the provider's answer and that commit
 *     is covered by the repeat, which returns the same ticket. A request the provider rejects ends ROLLED_BACK.</li>
 * <li>{@code PROVISIONING}: poll {@code provider.status} until STARTED(address): phase {@code JOINING}. FAILED, or
 *     {@code provision_timeout} since the request: cancel.</li>
 * <li>{@code JOINING}: watch the directory for a node at the reported address. Whether its join has started is read
 *     from the metadata alone (state BOOTSTRAPPING or later, of a node registered after the request), never from
 *     the provider, and is recorded in the claim
 *     ({@code joining}). Before that: {@code join_timeout} or a FAILED status cancel. After it nothing is undone:
 *     {@code join_timeout}, or the node leaving the directory or the join, ends the action FAILED (CRITICAL
 *     diagnosis, layer paused) and the ticket is left with the provider for the operator. JOINED: phase
 *     {@code VERIFYING}.</li>
 * <li>{@code VERIFYING}: after {@code verify_window} the goal signal is compared with its baseline. The action ends
 *     COMMITTED whatever the verdict; a joined node is never removed.</li>
 * <li>{@code CANCELLING}: {@code provider.cancel}, repeated until it succeeds: ROLLED_BACK. If no ticket was
 *     persisted the request is repeated first to learn it. A cancel that keeps failing for {@code provision_timeout},
 *     or one that hit a node whose join had started, ends FAILED.</li>
 * </ul>
 * Kill and shutdown: before the join has started, cancel; once it has, or while verifying, the claim is released as
 * COMMITTED_CLEANUP_PENDING and the join is left to the cluster (FC5). {@link #pastCommitPoint} stays false so that
 * a restarted owner resumes watching its join instead of releasing the claim; {@link #abort} makes the distinction.
 * Takeover: the new owner restores the phase and continues; only a verification is not continued, because its
 * baseline was measured on another node (verdict INCONCLUSIVE).
 *
 * <p>Provider calls happen only here, on the actuator thread, each bounded by {@code provider_call_timeout}.
 */
public class ScaleOutActuator implements ClusterActuator
{
    private static final Logger logger = LoggerFactory.getLogger(ScaleOutActuator.class);

    public static final String NAME = "scale_out";
    public static final String PARAM_DATACENTER = "datacenter";
    public static final String PARAM_RACK = "rack";
    public static final String PARAM_REASON = "reason";

    public static final String REQUESTED = "REQUESTED";
    public static final String PROVISIONING = "PROVISIONING";
    public static final String JOINING = "JOINING";
    public static final String VERIFYING = "VERIFYING";
    public static final String CANCELLING = "CANCELLING";

    // claim state
    /** Own copy of the phase: a takeover replaces the claim's phase with RECOVERING. */
    public static final String S_PHASE = "phase";
    public static final String S_REQUEST = "request";
    public static final String S_REQUESTED_AT = "requested_at";
    /** Highest node id in the directory when the request was written: only a node registered later can be the new one. */
    public static final String S_KNOWN = "known_node";
    public static final String S_TICKET = "ticket";
    public static final String TICKET_ATTRIBUTE = "ticket.";
    public static final String S_ADDRESS = "address";
    public static final String S_PROVISIONED_AT = "provisioned_at";
    /** Node id of the new node, set once the metadata showed that its join had started: nothing is undone after. */
    public static final String S_JOINING = "joining";
    public static final String S_JOINED_AT = "joined_at";
    public static final String S_GOAL = "goal";
    public static final String S_BASELINE = "baseline";
    public static final String S_WINDOW = "window";
    public static final String S_CANCEL_SINCE = "cancel_since";
    public static final String S_WHY = "why";

    /** Relative improvement of the goal for verdict IMPROVED. */
    public static final double MIN_IMPROVEMENT = 0.05;
    private static final int MAX_TEXT = 200;
    private static final Set<NodeState> JOIN_STARTED = EnumSet.of(NodeState.BOOTSTRAPPING, NodeState.BOOT_REPLACING, NodeState.JOINED,
                                                                  NodeState.MOVING, NodeState.LEAVING);

    /** What the actuator reads of the cluster; replaced in unit tests. Reads only, safe on any thread. */
    public interface Cluster
    {
        ClusterMetadata metadata();

        /** Datacenter and rack of this node; null if it is not registered. */
        Location location();
    }

    private static final Cluster CURRENT = new Cluster()
    {
        public ClusterMetadata metadata()
        {
            return ClusterMetadata.current();
        }

        public Location location()
        {
            ClusterMetadata metadata = ClusterMetadata.current();
            NodeId self = metadata.myNodeId();
            return self == null ? null : metadata.directory.location(self);
        }
    };

    private final ScaleOutProvider provider;
    private final Capabilities capabilities;
    private final Cluster cluster;
    private final Supplier<AutonomicConfig.ScaleOutConfig> config;
    private final Supplier<KnowledgeView> knowledge;
    private final ProviderCalls calls;

    /** @param knowledge the layer's view, for the baseline and the verdict; may supply null */
    public ScaleOutActuator(ScaleOutProvider provider, Cluster cluster, Supplier<AutonomicConfig.ScaleOutConfig> config,
                            Supplier<KnowledgeView> knowledge, ProviderCalls calls)
    {
        this.provider = provider;
        this.capabilities = provider.capabilities();
        this.cluster = cluster;
        this.config = config;
        this.knowledge = knowledge;
        this.calls = calls;
    }

    /** With the provider named in configuration, or {@code NoopProvider} if it cannot be built. */
    public static ScaleOutActuator create(AutonomicConfig config, Supplier<KnowledgeView> knowledge)
    {
        return new ScaleOutActuator(ScaleOutProviders.load(config.scale_out), CURRENT, () -> config.scale_out, knowledge, ProviderCalls.production());
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Lane lane()
    {
        return Lane.TOPOLOGY;
    }

    /** A joined node is never removed by the layer. */
    @Override
    public boolean reversible()
    {
        return false;
    }

    public ScaleOutProvider provider()
    {
        return provider;
    }

    /** As read once when the actuator was built; no provider call. */
    public Capabilities capabilities()
    {
        return capabilities;
    }

    /** The provider call that has been blocking the actuator thread for longer than the timeout, or null. Any thread. */
    public String stalledProviderCall(long timeoutMillis)
    {
        String call = calls.stalled(timeoutMillis);
        return call == null ? null : "provider " + provider.name() + ' ' + call;
    }

    public Location location()
    {
        return cluster.location();
    }

    /** Directory members that have not left. */
    public int members()
    {
        ClusterMetadata metadata = cluster.metadata();
        int n = 0;
        if (metadata != null)
            for (NodeId id : metadata.directory.peerIds())
                if (metadata.directory.peerState(id) != NodeState.LEFT)
                    n++;
        return n;
    }

    /** Why no node can be added now, or empty. No provider call. */
    public Optional<String> unavailable()
    {
        if (!capabilities.canAdd)
            return Optional.of("provider " + provider.name() + " cannot add nodes");
        int members = members();
        if (members >= capabilities.maxNodes)
            return Optional.of("the cluster has " + members + " members and provider " + provider.name() + " allows " + capabilities.maxNodes);
        return Optional.empty();
    }

    public static Map<String, String> params(String datacenter, String rack, String reason)
    {
        return ImmutableMap.of(PARAM_DATACENTER, datacenter, PARAM_RACK, rack, PARAM_REASON, text(reason));
    }

    @Override
    public Optional<String> validate(Action action, KnowledgeView view)
    {
        if (action.target.kind != Subject.Kind.CLUSTER)
            return Optional.of("target must be the cluster");
        if (empty(action.params.get(PARAM_DATACENTER)) || empty(action.params.get(PARAM_RACK)))
            return Optional.of(PARAM_DATACENTER + " and " + PARAM_RACK + " are required");
        return unavailable();
    }

    // ---------------------------------------------------------------------------------------------------------
    // entry points
    // ---------------------------------------------------------------------------------------------------------

    @Override
    public StepResult step(Action action, Claim claim, ActionContext ctx)
    {
        return ClusterActionExecution.INITIAL_PHASE.equals(claim.phase) ? claimed(action, ctx) : drive(claim, ctx);
    }

    /**
     * A new owner (claim phase RECOVERING) restores the phase the lost owner had reached and continues from the
     * claim's state; the owner itself after a restart continues where it was.
     */
    @Override
    public StepResult recover(Claim claim, ActionContext ctx)
    {
        if (!Claim.RECOVERING.equals(claim.phase))
            return drive(claim, ctx);
        String phase = phaseOf(claim);
        if (phase == null)
            return StepResult.rolledBack("the previous owner had requested nothing");
        if (VERIFYING.equals(phase))
            return StepResult.committed(Verdict.INCONCLUSIVE, "node " + claim.state.get(S_JOINING) + " at " + claim.state.get(S_ADDRESS)
                                                              + " joined; the owner was lost during verification");
        return StepResult.advance(phase, claim.state, 0);
    }

    /** Kill, shutdown or a step that keeps throwing: cancel before the join has started, leave it alone after. */
    @Override
    public StepResult abort(Action action, Claim claim, ActionContext ctx)
    {
        String phase = phaseOf(claim);
        if (phase == null || ClusterActionExecution.INITIAL_PHASE.equals(phase))
            return StepResult.rolledBack("stopped before anything was requested");
        if (VERIFYING.equals(phase))
            return StepResult.committedCleanupPending("node " + claim.state.get(S_JOINING) + " joined; verification stopped");
        if (CANCELLING.equals(phase))
            return cancelling(claim, ctx);
        String address = claim.state.get(S_ADDRESS);
        if (claim.state.containsKey(S_JOINING) || joinStarted(claim))
            return StepResult.committedCleanupPending("stopped while the node at " + address + " is joining; the join is left to the cluster and ticket "
                                                      + claim.state.get(S_TICKET) + " to the provider");
        return toCancelling(claim, "stopped", ctx);
    }

    private static String phaseOf(Claim claim)
    {
        return Claim.RECOVERING.equals(claim.phase) ? claim.state.get(S_PHASE) : claim.phase;
    }

    private StepResult drive(Claim claim, ActionContext ctx)
    {
        String phase = phaseOf(claim);
        if (phase == null || ClusterActionExecution.INITIAL_PHASE.equals(phase))
            return StepResult.rolledBack("nothing was requested");
        switch (phase)
        {
            case REQUESTED:
                return requested(claim, ctx);
            case PROVISIONING:
                return provisioning(claim, ctx);
            case JOINING:
                return joining(claim, ctx);
            case VERIFYING:
                return verifying(claim, ctx);
            case CANCELLING:
                return cancelling(claim, ctx);
            default:
                return StepResult.failed("scale-out claim is in unknown phase " + phase);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // phases
    // ---------------------------------------------------------------------------------------------------------

    /** Write-ahead of the request: no provider call before the request id is in the ledger. */
    private StepResult claimed(Action action, ActionContext ctx)
    {
        Optional<String> unavailable = unavailable();
        if (unavailable.isPresent())
            return StepResult.rolledBack(unavailable.get());
        Map<String, String> state = new TreeMap<>();
        state.put(S_PHASE, REQUESTED);
        state.put(S_REQUEST, action.id.toString());
        state.put(S_REQUESTED_AT, Long.toString(ctx.nowMillis()));
        state.put(S_KNOWN, Integer.toString(highestNodeId()));
        state.put(PARAM_DATACENTER, action.params.get(PARAM_DATACENTER));
        state.put(PARAM_RACK, action.params.get(PARAM_RACK));
        state.put(PARAM_REASON, text(action.params.get(PARAM_REASON)));
        VerifySpec verify = action.verify;
        if (verify != null)
        {
            state.put(S_GOAL, MetricVerifier.key(verify.goal, verify.goalSubject));
            state.put(S_WINDOW, Long.toString(verify.baselineWindowMillis));
            KnowledgeView view = knowledge.get();
            Double baseline = view == null ? null : MetricVerifier.means(goalSpec(verify.goal, verify.goalSubject), view, verify.baselineWindowMillis)
                                                                  .get(MetricVerifier.key(verify.goal, verify.goalSubject));
            if (baseline != null)
                state.put(S_BASELINE, Double.toString(baseline));
        }
        return StepResult.advance(REQUESTED, state, 0);
    }

    private StepResult requested(Claim claim, ActionContext ctx)
    {
        long now = ctx.nowMillis();
        if (now - time(claim, S_REQUESTED_AT) > provisionTimeoutMillis())
            return toCancelling(claim, "provision_timeout before the provider confirmed the request", ctx);
        ProvisionTicket ticket;
        try
        {
            ticket = request(claim, ctx);
        }
        catch (ProviderException e)
        {
            if (e.isRejected())
                return StepResult.rolledBack("provider " + provider.name() + " rejected the request: " + e.getMessage());
            // outcome unknown: the repeat is idempotent on the request id
            logger.info("Autonomic scale-out {}: requestNode failed, will be repeated: {}", claim.state.get(S_REQUEST), e.getMessage());
            return StepResult.stay(pollMillis());
        }
        Map<String, String> state = new TreeMap<>();
        for (String key : new String[]{ S_REQUEST, S_REQUESTED_AT, S_KNOWN, S_GOAL, S_BASELINE, S_WINDOW })
            if (claim.state.containsKey(key))
                state.put(key, claim.state.get(key));
        state.put(S_PHASE, PROVISIONING);
        state.put(S_TICKET, ticket.id);
        for (Map.Entry<String, String> e : ticket.attributes.entrySet())
            state.put(TICKET_ATTRIBUTE + e.getKey(), String.valueOf(e.getValue()));
        String invalid = Claim.invalidState(withLaterEntries(state));
        if (invalid != null)
        {
            // a ticket that cannot be stored cannot be followed by another owner: give it back now
            String detail = "ticket " + text(ticket.id) + " of provider " + provider.name() + " does not fit the claim (" + invalid + ')';
            try
            {
                cancel(ticket, ctx);
            }
            catch (ProviderException e)
            {
                return StepResult.failed(detail + " and could not be cancelled: " + e.getMessage() + "; a node may still be provisioned");
            }
            return StepResult.rolledBack(detail);
        }
        return StepResult.advance(PROVISIONING, state, 0);
    }

    private StepResult provisioning(Claim claim, ActionContext ctx)
    {
        ProvisionStatus status = status(claim, ctx);
        if (status != null && status.state == ProvisionStatus.State.FAILED)
            return toCancelling(claim, "provider reported FAILED: " + status.reason, ctx);
        if (status != null && status.state == ProvisionStatus.State.STARTED)
        {
            String address;
            try
            {
                address = InetAddressAndPort.getByName(status.address).getHostAddressAndPort();
            }
            catch (Exception e)
            {
                return toCancelling(claim, "provider reported an unusable address " + status.address, ctx);
            }
            NodeId existing = cluster.metadata().directory.peerId(InetAddressAndPort.getByNameUnchecked(address));
            if (existing != null && existing.id() <= known(claim))
                return toCancelling(claim, "provider reported the address " + address + " of node " + existing.id() + ", a member from before the request", ctx);
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(S_PHASE, JOINING);
            state.put(S_ADDRESS, address);
            state.put(S_PROVISIONED_AT, Long.toString(ctx.nowMillis()));
            return StepResult.advance(JOINING, state, 0);
        }
        if (ctx.nowMillis() - time(claim, S_REQUESTED_AT) > provisionTimeoutMillis())
            return toCancelling(claim, "provision_timeout: no node was started within " + provisionTimeoutMillis() + "ms", ctx);
        return StepResult.stay(pollMillis());
    }

    private StepResult joining(Claim claim, ActionContext ctx)
    {
        String address = claim.state.get(S_ADDRESS);
        Directory directory = cluster.metadata().directory;
        NodeId id = newNode(directory, claim);
        NodeState nodeState = id == null ? null : directory.peerState(id);
        long now = ctx.nowMillis();
        boolean timedOut = now - time(claim, S_PROVISIONED_AT) > joinTimeoutMillis();
        if (nodeState == NodeState.JOINED)
        {
            Map<String, String> state = new TreeMap<>(claim.state);
            state.put(S_PHASE, VERIFYING);
            state.put(S_JOINING, Integer.toString(id.id()));
            state.put(S_JOINED_AT, Long.toString(now));
            return StepResult.advance(VERIFYING, state, Math.min(pollMillis(), verifyWindowMillis()));
        }
        boolean started = nodeState != null && JOIN_STARTED.contains(nodeState);
        if (!claim.state.containsKey(S_JOINING))
        {
            if (started)
            {
                // the point of no return, recorded before anything else is decided
                Map<String, String> state = new TreeMap<>(claim.state);
                state.put(S_JOINING, Integer.toString(id.id()));
                return StepResult.advance(JOINING, state, pollMillis());
            }
            if (timedOut)
                return toCancelling(claim, "join_timeout: the node at " + address + " never started to join", ctx);
            ProvisionStatus status = status(claim, ctx);
            if (status != null && status.state == ProvisionStatus.State.FAILED)
                return toCancelling(claim, "provider reported FAILED before the join started: " + status.reason, ctx);
            return StepResult.stay(pollMillis());
        }
        String node = "node " + claim.state.get(S_JOINING) + " at " + address;
        String left = "; nothing is undone automatically and ticket " + claim.state.get(S_TICKET) + " is left with provider " + provider.name();
        if (!started)
            return StepResult.failed("the join of " + node + " started and did not finish: it is now "
                                     + (nodeState == null ? "not in the directory" : nodeState) + left);
        if (timedOut)
            return StepResult.failed("join_timeout: " + node + " is still " + nodeState + " after " + joinTimeoutMillis() + "ms" + left);
        return StepResult.stay(pollMillis());
    }

    private StepResult verifying(Claim claim, ActionContext ctx)
    {
        long remaining = time(claim, S_JOINED_AT) + verifyWindowMillis() - ctx.nowMillis();
        if (remaining > 0)
            return StepResult.stay(Math.min(remaining, 4 * pollMillis()));
        String node = "node " + claim.state.get(S_JOINING) + " at " + claim.state.get(S_ADDRESS) + " joined";
        String goal = claim.state.get(S_GOAL);
        String baseline = claim.state.get(S_BASELINE);
        KnowledgeView view = knowledge.get();
        int at = goal == null ? -1 : goal.indexOf('@');
        if (at < 0 || baseline == null || view == null)
            return StepResult.committed(Verdict.INCONCLUSIVE, node + "; no baseline of the goal");
        VerifySpec spec = goalSpec(SignalId.of(goal.substring(0, at)), Subject.parse(goal.substring(at + 1)));
        Map<String, Double> result = MetricVerifier.means(spec, view, time(claim, S_WINDOW));
        Verdict verdict = MetricVerifier.verdict(spec, Map.of(goal, Double.parseDouble(baseline)), result);
        return StepResult.committed(verdict, node + "; " + goal + " was " + baseline + " and is " + result.get(goal));
    }

    private StepResult cancelling(Claim claim, ActionContext ctx)
    {
        String address = claim.state.get(S_ADDRESS);
        String why = claim.state.get(S_WHY);
        if (joinStarted(claim))
            return StepResult.committedCleanupPending("the node at " + address + " started to join before the request could be cancelled (" + why
                                                      + "); the join is left to the cluster and ticket " + claim.state.get(S_TICKET) + " to the provider");
        boolean deadline = ctx.nowMillis() - time(claim, S_CANCEL_SINCE) > provisionTimeoutMillis();
        ProvisionTicket ticket = ticketOf(claim);
        try
        {
            if (ticket == null)
            {
                // no ticket was persisted; the provider may hold the request all the same, and only it knows the ticket
                try
                {
                    ticket = request(claim, ctx);
                }
                catch (ProviderException e)
                {
                    if (e.isRejected())
                        return StepResult.rolledBack(why + "; provider " + provider.name() + " holds no request");
                    throw e;
                }
            }
            cancel(ticket, ctx);
        }
        catch (ProviderException e)
        {
            if (deadline)
                return StepResult.failed("request " + claim.state.get(S_REQUEST) + (ticket == null ? "" : " (ticket " + text(ticket.id) + ')')
                                         + " could not be cancelled with provider " + provider.name() + ": " + e.getMessage()
                                         + "; a node may still be provisioned (" + why + ')');
            logger.info("Autonomic scale-out {}: cancel failed, will be repeated: {}", claim.state.get(S_REQUEST), e.getMessage());
            return StepResult.stay(pollMillis());
        }
        if (joinStarted(claim))
            return StepResult.failed("ticket " + text(ticket.id) + " was cancelled, but the node at " + address
                                     + " had started to join meanwhile; an operator must check that node (" + why + ')');
        return StepResult.rolledBack(why);
    }

    private StepResult toCancelling(Claim claim, String why, ActionContext ctx)
    {
        Map<String, String> state = new TreeMap<>(claim.state);
        state.put(S_PHASE, CANCELLING);
        state.put(S_CANCEL_SINCE, Long.toString(ctx.nowMillis()));
        state.put(S_WHY, text(why));
        return StepResult.advance(CANCELLING, state, 0);
    }

    // ---------------------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------------------

    /** True once the metadata shows the new node at the reported address bootstrapping or further. */
    private boolean joinStarted(Claim claim)
    {
        if (!claim.state.containsKey(S_ADDRESS))
            return false;
        Directory directory = cluster.metadata().directory;
        NodeId id = newNode(directory, claim);
        NodeState state = id == null ? null : directory.peerState(id);
        return state != null && JOIN_STARTED.contains(state);
    }

    /** The node at the reported address, if it was registered after the request; an older member is not it. */
    private static NodeId newNode(Directory directory, Claim claim)
    {
        NodeId id = directory.peerId(InetAddressAndPort.getByNameUnchecked(claim.state.get(S_ADDRESS)));
        return id != null && id.id() > known(claim) ? id : null;
    }

    private static int known(Claim claim)
    {
        String known = claim.state.get(S_KNOWN);
        return known == null ? -1 : Integer.parseInt(known);
    }

    private int highestNodeId()
    {
        int highest = -1;
        for (NodeId id : cluster.metadata().directory.peerIds())
            highest = Math.max(highest, id.id());
        return highest;
    }

    private ProvisionTicket request(Claim claim, ActionContext ctx) throws ProviderException
    {
        ScaleRequest request = new ScaleRequest(UUID.fromString(claim.state.get(S_REQUEST)), claim.state.get(PARAM_DATACENTER),
                                                claim.state.get(PARAM_RACK), claim.state.get(PARAM_REASON));
        ProvisionTicket ticket = calls.call("requestNode", callTimeoutMillis(ctx), () -> provider.requestNode(request));
        if (ticket == null || empty(ticket.id))
            throw new ProviderException("requestNode returned no ticket");
        return ticket;
    }

    /** Null if the provider did not answer. */
    private ProvisionStatus status(Claim claim, ActionContext ctx)
    {
        ProvisionTicket ticket = ticketOf(claim);
        try
        {
            return calls.call("status", callTimeoutMillis(ctx), () -> provider.status(ticket));
        }
        catch (ProviderException e)
        {
            logger.info("Autonomic scale-out {}: status failed, will be repeated: {}", claim.state.get(S_REQUEST), e.getMessage());
            return null;
        }
    }

    private void cancel(ProvisionTicket ticket, ActionContext ctx) throws ProviderException
    {
        calls.call("cancel", callTimeoutMillis(ctx), () -> {
            provider.cancel(ticket);
            return null;
        });
    }

    private static ProvisionTicket ticketOf(Claim claim)
    {
        String id = claim.state.get(S_TICKET);
        if (id == null)
            return null;
        Map<String, String> attributes = new TreeMap<>();
        for (Map.Entry<String, String> e : claim.state.entrySet())
            if (e.getKey().startsWith(TICKET_ATTRIBUTE))
                attributes.put(e.getKey().substring(TICKET_ATTRIBUTE.length()), e.getValue());
        return new ProvisionTicket(id, attributes);
    }

    /** The state at its largest: what later phases add to it, so that a ticket that fits now fits until the end. */
    private static Map<String, String> withLaterEntries(Map<String, String> state)
    {
        Map<String, String> largest = new TreeMap<>(state);
        String text = "x".repeat(MAX_TEXT);
        largest.put(S_ADDRESS, text);
        largest.put(S_WHY, text);
        for (String key : new String[]{ S_PROVISIONED_AT, S_JOINING, S_JOINED_AT, S_CANCEL_SINCE })
            largest.put(key, Long.toString(Long.MIN_VALUE));
        return largest;
    }

    private static VerifySpec goalSpec(SignalId signal, Subject subject)
    {
        return new VerifySpec(signal, subject, true, MIN_IMPROVEMENT, java.util.List.of(), 0, 0);
    }

    private static long time(Claim claim, String key)
    {
        String value = claim.state.get(key);
        return value == null ? 0 : Long.parseLong(value);
    }

    private static boolean empty(String s)
    {
        return s == null || s.isEmpty();
    }

    private static String text(String s)
    {
        return s == null ? "" : s.length() <= MAX_TEXT ? s : s.substring(0, MAX_TEXT);
    }

    private long provisionTimeoutMillis()
    {
        return config.get().provision_timeout.toMilliseconds();
    }

    private long joinTimeoutMillis()
    {
        return config.get().join_timeout.toMilliseconds();
    }

    private long verifyWindowMillis()
    {
        return config.get().verify_window.toMilliseconds();
    }

    private long pollMillis()
    {
        return Math.max(1, config.get().poll_interval.toMilliseconds());
    }

    /** Bounded by the configured timeout and by what is left of the step. */
    private long callTimeoutMillis(ActionContext ctx)
    {
        long configured = config.get().provider_call_timeout.toMilliseconds();
        return Math.max(1, Math.min(configured, ctx.stepDeadlineMillis() - ctx.nowMillis()));
    }
}
