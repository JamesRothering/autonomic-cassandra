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
package org.apache.cassandra.autonomic.coord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;

/**
 * This node's side of the cluster ledger (overview 6.2 to 6.5): claims a lane, advances, renews and releases its
 * claims, takes over expired ones, and dispatches phase changes committed by any node.
 *
 * <p>Rules it enforces:
 * <ul>
 * <li><b>Nothing is written to the metadata log before an operator enabled cluster actions.</b> Every commit except
 *     {@link #setEnabled}{@code (true)} is refused locally unless the ledger already exists in the metadata this
 *     node sees; claims additionally need the flag to be set.</li>
 * <li><b>Success is read from the ledger, not from the commit's return path</b>, which cannot tell whether a
 *     transformation was applied. Every method commits, then looks at the ledger in the newest metadata this node
 *     has: {@link Status#OK} only if the ledger shows the intended result, {@link Status#LOST} if it shows that
 *     the claim is gone, {@link Status#UNKNOWN} otherwise. UNKNOWN is safe to retry: the transformations are
 *     idempotent and conditional on the claim's owner and step.</li>
 * <li>Every commit is bounded by {@code action_step_timeout}.</li>
 * <li>A claim that was requested but not seen (UNKNOWN) is remembered and released if it appears later
 *     ({@link #releaseAbandoned}).</li>
 * </ul>
 *
 * <p>Threading: the commit methods block and are called on the actuator thread (operator commands on a JMX
 * thread); {@link #notifyPostCommit} runs on the metadata log thread and only calls the {@link PhaseListener}, which
 * must hand over to another thread. The read methods are safe anywhere.
 */
public class ClusterCoordinator implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterCoordinator.class);

    public static final String FORCE_RELEASED = "FORCE_RELEASED";
    public static final String ABANDONED = "ABANDONED";
    private static final int MAX_ABANDONED = 64;

    /** What the coordinator needs from the node. {@link #commit} blocks; everything else must be cheap. */
    public interface Environment
    {
        /** The newest metadata this node has. */
        ClusterMetadata metadata();

        /** This node's id; null if it is not registered. */
        NodeId self();

        /** Failure detector view of another member. */
        boolean isAlive(NodeId node);

        /**
         * Commits the transformation with a deadline and returns the newest metadata this node has afterwards,
         * whatever the outcome. Must not throw.
         */
        Committed commit(Transformation transformation, long timeoutMillis);

        long nowMillis();

        long leaseMillis();

        long leaseSafetyMarginMillis();

        /** {@code action_step_timeout}: bound of one commit. */
        long commitTimeoutMillis();

        /** Cooldown supplied with a claim when the last action on the same actuator and target committed. */
        long cooldownAfterCommitMillis();

        /** Cooldown supplied with a claim when the last action on the same actuator and target did not commit. */
        long cooldownAfterRollbackMillis();

        /** A new timeuuid for a takeover claim. */
        UUID newActionId();
    }

    /** Result of {@link Environment#commit}. */
    public static final class Committed
    {
        public final ClusterMetadata metadata;
        /** Null if the commit path reported success; informational only. */
        public final String failure;

        public Committed(ClusterMetadata metadata, String failure)
        {
            this.metadata = Objects.requireNonNull(metadata);
            this.failure = failure;
        }
    }

    public enum Status
    {
        /** The ledger shows the intended result. */
        OK,
        /** The ledger shows that this node does not hold the claim. */
        LOST,
        /** The ledger shows neither; retry, or give up without assuming anything. */
        UNKNOWN
    }

    public static final class Outcome
    {
        public final Status status;
        /** OK: the claim as it is in the ledger. UNKNOWN: the claim as the ledger has it now, if this node holds it. */
        public final Claim claim;
        /** Nullable. */
        public final String detail;

        Outcome(Status status, Claim claim, String detail)
        {
            this.status = status;
            this.claim = claim;
            this.detail = detail;
        }

        public boolean ok()
        {
            return status == Status.OK;
        }

        @Override
        public String toString()
        {
            return status + (detail == null ? "" : ": " + detail);
        }
    }

    /** Receives committed phase changes; called on the metadata log thread, must not block or throw. */
    public interface PhaseListener
    {
        void onPhase(Lane lane, Claim claim);
    }

    private final Environment env;
    private volatile PhaseListener phaseListener;
    /** Claims requested but not seen, by action id, with the time after which they are forgotten. */
    private final Map<UUID, Long> abandoned = new LinkedHashMap<>();

    public ClusterCoordinator(Environment env)
    {
        this.env = env;
    }

    public void setPhaseListener(PhaseListener listener)
    {
        this.phaseListener = listener;
    }

    // ---------------------------------------------------------------------------------------------------------
    // reads
    // ---------------------------------------------------------------------------------------------------------

    public ClusterLedger ledger()
    {
        return ClusterLedger.of(env.metadata());
    }

    /** True once an operator enabled cluster actions and did not disable them since. */
    public boolean enabled()
    {
        ClusterMetadata metadata = env.metadata();
        return ClusterLedger.present(metadata) && ClusterLedger.of(metadata).clusterActionsEnabled;
    }

    public NodeId self()
    {
        NodeId self = env.self();
        return self == null || self.equals(NodeId.UNREGISTERED) ? null : self;
    }

    /**
     * Overview 6.3: this node is the JOINED member with the lowest id among those it sees alive. An optimization
     * against a herd of rejected claims, not a safety property.
     */
    public boolean preferredProposer()
    {
        NodeId self = self();
        ClusterMetadata metadata = env.metadata();
        if (self == null || metadata == null)
            return false;
        Directory directory = metadata.directory;
        for (NodeId id : directory.peerIds())
        {
            if (directory.peerState(id) != NodeState.JOINED)
                continue;
            if (id.equals(self))
                return true;
            if (env.isAlive(id))
                return false;
        }
        return false;
    }

    /**
     * ADR-038: this node's place in the takeover order, the number of JOINED members with a lower id that it sees
     * alive; 0 for the preferred proposer, -1 if this node is not a JOINED member.
     */
    public int proposerRank()
    {
        NodeId self = self();
        ClusterMetadata metadata = env.metadata();
        if (self == null || metadata == null)
            return -1;
        Directory directory = metadata.directory;
        int rank = 0;
        for (NodeId id : directory.peerIds())
        {
            if (directory.peerState(id) != NodeState.JOINED)
                continue;
            if (id.equals(self))
                return rank;
            if (env.isAlive(id))
                rank++;
        }
        return -1;
    }

    /** FC3: ids of directory members this node does not see alive; empty if all are. */
    public List<NodeId> membersNotAlive()
    {
        NodeId self = self();
        List<NodeId> down = new ArrayList<>();
        ClusterMetadata metadata = env.metadata();
        if (metadata == null)
            return down;
        for (NodeId id : metadata.directory.peerIds())
            if (!id.equals(self) && !env.isAlive(id))
                down.add(id);
        return down;
    }

    public boolean sequencesInProgress()
    {
        ClusterMetadata metadata = env.metadata();
        return metadata != null && !metadata.inProgressSequences.isEmpty();
    }

    /** The claim of the action if the ledger shows this node as its owner, otherwise null. */
    public Claim ownClaim(UUID actionId)
    {
        NodeId self = self();
        Claim claim = ledger().claim(actionId);
        return claim != null && self != null && claim.owner.equals(self) ? claim : null;
    }

    /**
     * Overview 6.2, lease and clocks: the owner acts on its claim only while its clock is before
     * {@code leaseExpiresAtMillis - lease_safety_margin}.
     */
    public boolean mayAct(Claim claim)
    {
        return env.nowMillis() < claim.leaseExpiresAtMillis - env.leaseSafetyMarginMillis();
    }

    /** Renewal is due every third of the lease. */
    public boolean renewalDue(Claim claim)
    {
        long lease = env.leaseMillis();
        return env.nowMillis() >= claim.leaseExpiresAtMillis - lease + lease / 3;
    }

    /** Claims whose lease expired on this node's clock, by lane. */
    public Map<Lane, Claim> expiredClaims()
    {
        long now = env.nowMillis();
        Map<Lane, Claim> expired = new LinkedHashMap<>();
        for (Map.Entry<Lane, Claim> e : ledger().claims.entrySet())
            if (e.getValue().leaseExpiresAtMillis < now)
                expired.put(e.getKey(), e.getValue());
        return expired;
    }

    /**
     * ADR-038: expired claims this node may take over now, by lane. The preferred proposer (rank 0) may at expiry;
     * the node of rank {@code r} one {@code lease_safety_margin} per rank later, so that an expired claim is
     * recovered even when the nodes before it in the order do not take it over (paused, killed, actuator disabled).
     * Who wins is still decided by {@link ClaimAction}.
     */
    public Map<Lane, Claim> takeoverDue()
    {
        int rank = proposerRank();
        if (rank < 0)
            return new LinkedHashMap<>();
        long now = env.nowMillis();
        long wait = rank * env.leaseSafetyMarginMillis();
        Map<Lane, Claim> due = expiredClaims();
        due.values().removeIf(claim -> claim.leaseExpiresAtMillis + wait >= now);
        return due;
    }

    // ---------------------------------------------------------------------------------------------------------
    // commits of the claim owner
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Claims the lane for a new action. OK with the claim if the ledger shows this node as its owner. LOST if
     * nothing was committed (not enabled, not registered, invalid claim). Otherwise UNKNOWN: a claim that was not
     * seen may still appear, so the action id is remembered and such a late claim is released by
     * {@link #releaseAbandoned}.
     */
    public Outcome claim(Lane lane, UUID actionId, String actuator, String target, String phase, Map<String, String> state)
    {
        return claim(lane, actionId, actuator, target, phase, state, false);
    }

    /**
     * As {@link #claim(Lane, UUID, String, String, String, Map)}; a {@code continuation} (see
     * {@code Actuator.continuation}) is claimed without the cooldown of the last action on the same target.
     */
    public Outcome claim(Lane lane, UUID actionId, String actuator, String target, String phase, Map<String, String> state, boolean continuation)
    {
        ClusterMetadata metadata = env.metadata();
        ClusterLedger ledger = ClusterLedger.of(metadata);
        NodeId self = self();
        if (!ClusterLedger.present(metadata) || !ledger.clusterActionsEnabled)
            return new Outcome(Status.LOST, null, "cluster actions are not enabled");
        if (self == null)
            return new Outcome(Status.LOST, null, "this node is not registered");
        long now = env.nowMillis();
        Claim claim;
        try
        {
            claim = new Claim(actionId, actuator, target, self, phase, 0, now, now + env.leaseMillis(), state);
        }
        catch (RuntimeException e)
        {
            return new Outcome(Status.LOST, null, "invalid claim: " + e);
        }
        if (claim.invalid() != null)
            return new Outcome(Status.LOST, null, "invalid claim: " + claim.invalid());
        Recent last = ledger.lastRecent(actuator, target);
        long cooldown = last == null || continuation ? 0
                                     : last.finalState.startsWith("COMMITTED") ? env.cooldownAfterCommitMillis() : env.cooldownAfterRollbackMillis();
        Committed committed = commit(new ClaimAction(lane, claim, now, cooldown).wrap());
        Claim held = ClusterLedger.of(committed.metadata).claims.get(lane);
        if (held != null && held.heldBy(actionId, self))
            return new Outcome(Status.OK, held, null);
        abandon(actionId, now);
        return new Outcome(Status.UNKNOWN, null, committed.failure != null ? committed.failure
                                                                           : "lane " + lane + " is " + (held == null ? "free" : "held by " + held.actionId));
    }

    /**
     * Takes over a claim whose lease expired: a new claim of this node with a new action id, phase
     * {@link Claim#RECOVERING} and the actuator, target and state of the expired claim (overview 6.4).
     */
    public Outcome takeOver(Lane lane, Claim expired, UUID newActionId)
    {
        NodeId self = self();
        if (!enabled() || self == null)
            return new Outcome(Status.LOST, null, "cluster actions are not enabled");
        long now = env.nowMillis();
        if (expired.leaseExpiresAtMillis >= now)
            return new Outcome(Status.LOST, null, "lease has not expired");
        Claim claim = new Claim(newActionId, expired.actuator, expired.target, self, Claim.RECOVERING, 0, now, now + env.leaseMillis(), expired.state);
        Committed committed = commit(new ClaimAction(lane, claim, now, 0).wrap());
        Claim held = ClusterLedger.of(committed.metadata).claims.get(lane);
        if (held != null && held.heldBy(newActionId, self))
            return new Outcome(Status.OK, held, null);
        abandon(newActionId, now);
        return new Outcome(Status.UNKNOWN, null, committed.failure);
    }

    public UUID newActionId()
    {
        return env.newActionId();
    }

    /** Advances the claim to a new phase and state and renews its lease. */
    public Outcome advance(Claim current, String phase, Map<String, String> state)
    {
        String invalid = Claim.invalidName("phase", phase, Claim.MAX_NAME_BYTES);
        if (invalid == null)
            invalid = Claim.invalidState(state);
        if (invalid != null)
            throw new IllegalArgumentException(invalid);
        Outcome refused = refuseWithoutLedger(current);
        if (refused != null)
            return refused;
        AdvanceAction advance = new AdvanceAction(current.actionId, current.owner, current.step, phase, state, env.nowMillis(), env.leaseMillis());
        return afterAdvance(commit(advance.wrap()), current, phase, advance.state);
    }

    /** Renews the lease: an advance that keeps phase and state. */
    public Outcome renew(Claim current)
    {
        return advance(current, current.phase, current.state);
    }

    /**
     * FC2: commits a transformation of the actuator that itself advances the claim from {@code current.step} to
     * {@code phase} and {@code state} (through {@link ClusterLedger#withAdvancedClaim}).
     */
    public Outcome commitInstead(Claim current, Transformation transformation, String phase, Map<String, String> state)
    {
        Outcome refused = refuseWithoutLedger(current);
        if (refused != null)
            return refused;
        return afterAdvance(commit(transformation), current, phase, state);
    }

    private Outcome afterAdvance(Committed committed, Claim before, String phase, Map<String, String> state)
    {
        Claim held = ClusterLedger.of(committed.metadata).claim(before.actionId);
        if (held == null || !held.owner.equals(before.owner))
            return new Outcome(Status.LOST, null, lostDetail(held, committed));
        if (held.step == before.step + 1 && held.phase.equals(phase) && held.state.equals(state))
            return new Outcome(Status.OK, held, null);
        // not applied, not visible yet, or the claim had already moved (an earlier commit that looked unapplied)
        return new Outcome(Status.UNKNOWN, held, committed.failure);
    }

    /** Releases the claim with a final state. OK also if the ledger shows the action as already released. */
    public Outcome release(Claim current, String finalState)
    {
        Outcome refused = refuseWithoutLedger(current);
        if (refused != null)
            return refused;
        Committed committed = commit(new ReleaseAction(current.actionId, current.owner, finalState, env.nowMillis()).wrap());
        Claim held = ClusterLedger.of(committed.metadata).claim(current.actionId);
        // metadata only moves forward: a claim this node saw and no longer sees was released, by this commit or not
        if (held == null)
            return new Outcome(Status.OK, null, null);
        if (!held.owner.equals(current.owner))
            return new Outcome(Status.LOST, null, lostDetail(held, committed));
        return new Outcome(Status.UNKNOWN, held, committed.failure);
    }

    private Outcome refuseWithoutLedger(Claim current)
    {
        // a claim can only exist in a ledger; without one in sight this node's metadata is behind, and nothing is
        // written on that basis
        if (!ClusterLedger.present(env.metadata()))
            return new Outcome(Status.UNKNOWN, current, "no cluster ledger in the metadata of this node");
        return null;
    }

    private static String lostDetail(Claim held, Committed committed)
    {
        String now = held == null ? "the claim is no longer in the ledger" : "the claim is owned by node " + held.owner.id();
        return committed.failure == null ? now : now + " (" + committed.failure + ')';
    }

    private Committed commit(Transformation transformation)
    {
        Committed committed = env.commit(transformation, env.commitTimeoutMillis());
        if (committed.failure != null)
            logger.info("Autonomic ledger commit of {} did not report success: {}", transformation, committed.failure);
        return committed;
    }

    // ---------------------------------------------------------------------------------------------------------
    // abandoned claims
    // ---------------------------------------------------------------------------------------------------------

    private synchronized void abandon(UUID actionId, long now)
    {
        if (abandoned.size() >= MAX_ABANDONED)
        {
            Iterator<UUID> it = abandoned.keySet().iterator();
            it.next();
            it.remove();
        }
        // after two leases an abandoned claim has expired and belongs to whoever takes it over
        abandoned.put(actionId, now + 2 * env.leaseMillis());
    }

    /**
     * Releases claims of this node that were requested, not seen at the time and given up, but were applied after
     * all. Nothing was done under them. Returns the number released.
     */
    public int releaseAbandoned()
    {
        List<UUID> ids;
        long now = env.nowMillis();
        synchronized (this)
        {
            abandoned.values().removeIf(until -> until < now);
            if (abandoned.isEmpty())
                return 0;
            ids = new ArrayList<>(abandoned.keySet());
        }
        int released = 0;
        for (UUID id : ids)
        {
            Claim claim = ownClaim(id);
            if (claim == null)
                continue;
            Outcome outcome = release(claim, ABANDONED);
            if (outcome.status != Status.UNKNOWN)
            {
                synchronized (this)
                {
                    abandoned.remove(id);
                }
                released++;
                logger.info("Autonomic ledger: released abandoned claim {} ({})", id, outcome);
            }
        }
        return released;
    }

    // ---------------------------------------------------------------------------------------------------------
    // operator commands
    // ---------------------------------------------------------------------------------------------------------

    /**
     * {@code cluster enable | disable}. Enabling is the operator's attestation that every node runs this fork
     * (overview 6.5); it is refused while the directory shows more than one version. Disabling a cluster that
     * never enabled cluster actions writes nothing.
     *
     * @return false if there was nothing to do and nothing was committed
     * @throws IllegalStateException if refused, or if the ledger does not show the flag after the commit
     */
    public boolean setEnabled(boolean enabled)
    {
        ClusterMetadata metadata = env.metadata();
        if (metadata == null)
            throw new IllegalStateException("Cluster metadata is not available");
        boolean present = ClusterLedger.present(metadata);
        if (present && ClusterLedger.of(metadata).clusterActionsEnabled == enabled)
            return false;
        if (!enabled && !present)
            return false;
        if (enabled)
        {
            Directory directory = metadata.directory;
            if (!Objects.equals(directory.clusterMinVersion, directory.clusterMaxVersion))
                throw new IllegalStateException("Refusing to enable cluster actions: nodes run different versions (" + directory.clusterMinVersion
                                                + " to " + directory.clusterMaxVersion + "). Every node must run the same version of this fork.");
        }
        Committed committed = commit(new SetClusterActionsEnabled(enabled).wrap());
        if (!ClusterLedger.present(committed.metadata) || ClusterLedger.of(committed.metadata).clusterActionsEnabled != enabled)
            throw new IllegalStateException("Could not " + (enabled ? "enable" : "disable") + " cluster actions"
                                            + (committed.failure == null ? "" : ": " + committed.failure));
        return true;
    }

    /**
     * {@code cluster release <lane>}: removes whatever claim holds the lane, without its owner. The owner, if it
     * still runs, finds the claim gone and stops; nothing is undone.
     *
     * @return the action id that was released
     * @throws IllegalStateException if the lane is free or the claim is still there after the commit
     */
    public UUID forceRelease(Lane lane)
    {
        ClusterMetadata metadata = env.metadata();
        Claim held = ClusterLedger.of(metadata).claims.get(lane);
        if (held == null)
            throw new IllegalStateException("Lane " + lane + " is not claimed");
        Committed committed = commit(new ReleaseAction(held.actionId, null, FORCE_RELEASED, env.nowMillis()).wrap());
        if (ClusterLedger.of(committed.metadata).claim(held.actionId) != null)
            throw new IllegalStateException("Could not release lane " + lane + (committed.failure == null ? "" : ": " + committed.failure));
        return held.actionId;
    }

    // ---------------------------------------------------------------------------------------------------------
    // phase dispatch
    // ---------------------------------------------------------------------------------------------------------

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        PhaseListener listener = phaseListener;
        if (listener == null)
            return;
        try
        {
            ExtensionValue<?> before = prev == null ? null : prev.extensions.get(ClusterLedgerValue.KEY);
            ExtensionValue<?> after = next.extensions.get(ClusterLedgerValue.KEY);
            if (before == after)
                return;
            ClusterLedger was = ClusterLedger.of(prev);
            for (Map.Entry<Lane, Claim> e : ClusterLedger.of(next).claims.entrySet())
            {
                Claim claim = e.getValue();
                Claim old = was.claims.get(e.getKey());
                if (old == null || !old.actionId.equals(claim.actionId) || !old.phase.equals(claim.phase))
                    listener.onPhase(e.getKey(), claim);
            }
        }
        catch (Throwable t)
        {
            // never into the metadata log thread
            logger.warn("Autonomic ledger phase dispatch failed: {}", t.toString(), t);
        }
    }
}
