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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.membership.NodeId;

/**
 * The cluster ledger of overview 6.2: the operator's gate for cluster-scoped actions, at most one {@link Claim} per
 * {@link Lane}, and the most recently released claims. Immutable.
 *
 * <p>The {@code with*} methods are the whole logic of the ledger transformations. They are pure functions of their
 * arguments (no clock, configuration or local state), so they give the same result on every node that replays the
 * metadata log, and other transformations may call them to change the ledger together with their own state in one
 * commit (FC2): take {@link #of}, apply a helper, and put {@code new ClusterLedgerValue(result.ledger)} into the
 * transformer under {@link ClusterLedgerValue#KEY}.
 */
public final class ClusterLedger
{
    public static final int FORMAT_VERSION = 1;
    public static final int MAX_RECENT = 32;
    /** Bound of the UTF-8 length of {@link Recent#finalState}. */
    public static final int MAX_FINAL_STATE_BYTES = 64;

    public static final ClusterLedger EMPTY = new ClusterLedger(FORMAT_VERSION, false, ImmutableMap.of(), ImmutableList.of());

    public final int formatVersion;
    /** Set only by an operator command. */
    public final boolean clusterActionsEnabled;
    /** At most one claim per lane, in lane order. */
    public final ImmutableMap<Lane, Claim> claims;
    /** Newest first, at most {@link #MAX_RECENT}. */
    public final ImmutableList<Recent> recent;

    public ClusterLedger(int formatVersion, boolean clusterActionsEnabled, Map<Lane, Claim> claims, List<Recent> recent)
    {
        if (recent.size() > MAX_RECENT)
            throw new IllegalArgumentException("ledger has " + recent.size() + " recent entries, the limit is " + MAX_RECENT);
        this.formatVersion = formatVersion;
        this.clusterActionsEnabled = clusterActionsEnabled;
        this.claims = claims.isEmpty() ? ImmutableMap.of() : Maps.immutableEnumMap(new EnumMap<>(claims));
        this.recent = ImmutableList.copyOf(recent);
    }

    /** The ledger of the given metadata; {@link #EMPTY} if no ledger was ever written. */
    public static ClusterLedger of(ClusterMetadata metadata)
    {
        ExtensionValue<?> value = metadata == null ? null : metadata.extensions.get(ClusterLedgerValue.KEY);
        return value instanceof ClusterLedgerValue ? ((ClusterLedgerValue) value).getValue() : EMPTY;
    }

    /** True if a ledger was ever written to the metadata, i.e. an operator ran {@code cluster enable} once. */
    public static boolean present(ClusterMetadata metadata)
    {
        return metadata != null && metadata.extensions.get(ClusterLedgerValue.KEY) instanceof ClusterLedgerValue;
    }

    /** The claim of the action, or null if no lane holds it. */
    public Claim claim(UUID actionId)
    {
        for (Claim claim : claims.values())
            if (claim.actionId.equals(actionId))
                return claim;
        return null;
    }

    /** The lane holding the action, or null. */
    public Lane lane(UUID actionId)
    {
        for (Map.Entry<Lane, Claim> e : claims.entrySet())
            if (e.getValue().actionId.equals(actionId))
                return e.getKey();
        return null;
    }

    /** The recent entry of the action, or null. */
    public Recent recent(UUID actionId)
    {
        for (Recent r : recent)
            if (r.actionId.equals(actionId))
                return r;
        return null;
    }

    /** The newest recent entry with this actuator and target, or null. */
    public Recent lastRecent(String actuator, String target)
    {
        Recent last = null;
        for (Recent r : recent)
            if (r.actuator.equals(actuator) && r.target.equals(target) && (last == null || r.endedAtMillis > last.endedAtMillis))
                last = r;
        return last;
    }

    // ---------------------------------------------------------------------------------------------------------
    // pure transitions
    // ---------------------------------------------------------------------------------------------------------

    /** Outcome of a transition: the resulting ledger, or why it was rejected. */
    public static final class Result
    {
        /** Null if rejected. */
        public final ClusterLedger ledger;
        /** Null if accepted. */
        public final String rejection;
        /** True if accepted and the ledger is the one the transition was applied to (an idempotent re-apply). */
        public final boolean unchanged;

        private Result(ClusterLedger ledger, String rejection, boolean unchanged)
        {
            this.ledger = ledger;
            this.rejection = rejection;
            this.unchanged = unchanged;
        }

        public boolean isRejected()
        {
            return rejection != null;
        }

        @Override
        public String toString()
        {
            return isRejected() ? "Rejected{" + rejection + '}' : "Accepted{" + ledger + (unchanged ? ", unchanged}" : "}");
        }
    }

    private Result changed(boolean enabled, Map<Lane, Claim> newClaims, List<Recent> newRecent)
    {
        return new Result(new ClusterLedger(formatVersion, enabled, newClaims, newRecent), null, false);
    }

    private Result same()
    {
        return new Result(this, null, true);
    }

    private static Result reject(String reason)
    {
        return new Result(null, reason, false);
    }

    /** {@code SetClusterActionsEnabled}: always accepted. Claims in flight are not touched by disabling. */
    public Result withEnabled(boolean enabled)
    {
        return enabled == clusterActionsEnabled ? same() : changed(enabled, claims, recent);
    }

    /**
     * {@code ClaimAction}: writes {@code claim} with step 0 into {@code lane}. Accepted when cluster actions are
     * enabled; the lane is free, or its claim's lease expired before {@code nowMillis}; no released claim with the
     * same actuator and target ended within {@code cooldownMillis}; and, for lanes that require it, no multi-step
     * metadata operation is in progress. Re-applying the claim of the action and owner that hold the lane is
     * accepted and changes nothing, whatever step that claim has reached.
     *
     * @param sequencesInProgress {@code !metadata.inProgressSequences.isEmpty()} of the metadata being transformed
     */
    public Result withClaim(Lane lane, Claim claim, long nowMillis, long cooldownMillis, boolean sequencesInProgress)
    {
        if (lane == null || claim == null)
            return reject("no lane or claim");
        Claim held = claims.get(lane);
        if (held != null && held.heldBy(claim.actionId, claim.owner))
            return same();
        if (!clusterActionsEnabled)
            return reject("cluster actions are not enabled");
        String invalid = claim.invalid();
        if (invalid != null)
            return reject(invalid);
        if (claim.leaseExpiresAtMillis <= nowMillis)
            return reject("lease of the claim is not in the future");
        if (cooldownMillis < 0)
            return reject("negative cooldown");
        if (held != null && held.leaseExpiresAtMillis >= nowMillis)
            return reject("lane " + lane + " is held by action " + held.actionId + " of node " + held.owner.id()
                          + " until " + held.leaseExpiresAtMillis);
        Lane other = lane(claim.actionId);
        if (other != null && other != lane)
            return reject("action " + claim.actionId + " holds lane " + other);
        if (recent(claim.actionId) != null)
            return reject("action " + claim.actionId + " was already released");
        for (Recent r : recent)
            if (r.actuator.equals(claim.actuator) && r.target.equals(claim.target) && nowMillis - r.endedAtMillis < cooldownMillis)
                return reject("cooldown: action " + r.actionId + " on " + r.actuator + ' ' + r.target + " ended at " + r.endedAtMillis);
        if (lane.requiresNoInProgressSequences && sequencesInProgress)
            return reject("lane " + lane + " requires that no metadata operation is in progress");
        Map<Lane, Claim> newClaims = new EnumMap<>(Lane.class);
        newClaims.putAll(claims);
        newClaims.put(lane, claim.withStep(0));
        return changed(clusterActionsEnabled, newClaims, recent);
    }

    /**
     * {@code AdvanceAction}: accepted when a lane holds {@code actionId} with that {@code owner} and
     * {@code step == expectedStep}; the claim gets the next step, the new phase and state, and a lease until
     * {@code nowMillis + leaseMillis}. Re-applying the same advance to its own result is accepted and changes
     * nothing.
     */
    public Result withAdvancedClaim(UUID actionId, NodeId owner, int expectedStep, String phase, Map<String, String> state,
                                    long nowMillis, long leaseMillis)
    {
        if (actionId == null || owner == null)
            return reject("no action id or owner");
        Lane lane = lane(actionId);
        if (lane == null)
            return reject("no lane holds action " + actionId);
        Claim held = claims.get(lane);
        if (!held.owner.equals(owner))
            return reject("action " + actionId + " is owned by node " + held.owner.id() + ", not " + owner.id());
        String invalid = Claim.invalidName("phase", phase, Claim.MAX_NAME_BYTES);
        if (invalid == null)
            invalid = Claim.invalidState(state);
        if (invalid != null)
            return reject(invalid);
        if (leaseMillis <= 0)
            return reject("lease is not positive");
        Claim advanced = held.withStep(expectedStep).advanced(phase, state, nowMillis + leaseMillis);
        if (held.equals(advanced))
            return same();
        if (held.step != expectedStep)
            return reject("action " + actionId + " is at step " + held.step + ", not " + expectedStep);
        Map<Lane, Claim> newClaims = new EnumMap<>(Lane.class);
        newClaims.putAll(claims);
        newClaims.put(lane, advanced);
        return changed(clusterActionsEnabled, newClaims, recent);
    }

    /**
     * {@code ReleaseAction}: accepted when a lane holds {@code actionId}, and {@code owner} matches unless it is
     * null (forced by an operator); removes the claim and prepends a {@link Recent}, dropping the oldest beyond
     * {@link #MAX_RECENT}. Re-applying a release that is already recorded with the same final state is accepted and
     * changes nothing.
     */
    public Result withReleased(UUID actionId, NodeId owner, String finalState, long nowMillis)
    {
        if (actionId == null)
            return reject("no action id");
        String invalid = Claim.invalidName("final state", finalState, MAX_FINAL_STATE_BYTES);
        if (invalid != null)
            return reject(invalid);
        Lane lane = lane(actionId);
        if (lane == null)
        {
            Recent released = recent(actionId);
            if (released != null && released.finalState.equals(finalState))
                return same();
            return reject("no lane holds action " + actionId);
        }
        Claim held = claims.get(lane);
        if (owner != null && !held.owner.equals(owner))
            return reject("action " + actionId + " is owned by node " + held.owner.id() + ", not " + owner.id());
        Map<Lane, Claim> newClaims = new EnumMap<>(Lane.class);
        newClaims.putAll(claims);
        newClaims.remove(lane);
        List<Recent> newRecent = new ArrayList<>(Math.min(recent.size() + 1, MAX_RECENT));
        newRecent.add(new Recent(actionId, held.actuator, held.target, lane, finalState, nowMillis));
        for (Recent r : recent)
            if (newRecent.size() < MAX_RECENT)
                newRecent.add(r);
        return changed(clusterActionsEnabled, newClaims, newRecent);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof ClusterLedger))
            return false;
        ClusterLedger l = (ClusterLedger) o;
        return formatVersion == l.formatVersion && clusterActionsEnabled == l.clusterActionsEnabled && claims.equals(l.claims) && recent.equals(l.recent);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(formatVersion, clusterActionsEnabled, claims, recent);
    }

    @Override
    public String toString()
    {
        return "ClusterLedger{v" + formatVersion + " enabled=" + clusterActionsEnabled + " claims=" + claims + " recent=" + recent + '}';
    }

    // ---------------------------------------------------------------------------------------------------------
    // Serialized form. The format version comes first and is independent of the TCM serialization version. A new
    // format may only be written once every node can read it (overview 6.5); an unknown one fails loudly.
    // ---------------------------------------------------------------------------------------------------------

    static void serialize(ClusterLedger l, DataOutputPlus out) throws IOException
    {
        if (l.formatVersion != FORMAT_VERSION)
            throw new IOException("Cannot write autonomic ledger format " + l.formatVersion);
        out.writeInt(l.formatVersion);
        out.writeBoolean(l.clusterActionsEnabled);
        out.writeByte(l.claims.size());
        for (Map.Entry<Lane, Claim> e : l.claims.entrySet())
        {
            out.writeByte(e.getKey().id);
            Claim.serialize(e.getValue(), out);
        }
        out.writeByte(l.recent.size());
        for (Recent r : l.recent)
            Recent.serialize(r, out);
    }

    static ClusterLedger deserialize(DataInputPlus in) throws IOException
    {
        int format = in.readInt();
        if (format != FORMAT_VERSION)
            throw new IOException("Unknown autonomic ledger format " + format + "; this node reads format " + FORMAT_VERSION);
        boolean enabled = in.readBoolean();
        int claimCount = in.readByte();
        Map<Lane, Claim> claims = new EnumMap<>(Lane.class);
        for (int i = 0; i < claimCount; i++)
        {
            int laneId = in.readByte();
            Lane lane = Lane.fromId(laneId);
            if (lane == null)
                throw new IOException("Unknown autonomic ledger lane id " + laneId);
            claims.put(lane, Claim.deserialize(in));
        }
        int recentCount = in.readByte();
        if (recentCount < 0 || recentCount > MAX_RECENT)
            throw new IOException("Corrupt autonomic ledger: " + recentCount + " recent entries");
        List<Recent> recent = new ArrayList<>(recentCount);
        for (int i = 0; i < recentCount; i++)
            recent.add(Recent.deserialize(in));
        return new ClusterLedger(format, enabled, claims, recent);
    }

    static long serializedSize(ClusterLedger l)
    {
        long size = TypeSizes.INT_SIZE + 1 + 1 + 1;
        for (Claim c : l.claims.values())
            size += 1 + Claim.serializedSize(c);
        for (Recent r : l.recent)
            size += Recent.serializedSize(r);
        return size;
    }
}
