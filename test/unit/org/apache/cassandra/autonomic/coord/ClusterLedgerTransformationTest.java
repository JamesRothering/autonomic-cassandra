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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** The four ledger transformations against hand-built metadata: every accept and reject case of overview 6.2. */
public class ClusterLedgerTransformationTest
{
    private static final Version VERSION = NodeVersion.CURRENT_METADATA_VERSION;
    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final UUID A = new UUID(1, 1);
    private static final UUID B = new UUID(2, 2);
    private static final long NOW = 1_000_000;
    private static final long LEASE = 600_000;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    private static ClusterMetadata empty()
    {
        return new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, DistributedSchema.empty());
    }

    private static ClusterMetadata enabled()
    {
        return apply(empty(), new SetClusterActionsEnabled(true));
    }

    private static ClusterMetadata withSequenceInProgress(ClusterMetadata metadata)
    {
        return metadata.transformer()
                       .with(metadata.inProgressSequences.with(new NodeId(77), Mockito.mock(MultiStepOperation.class)))
                       .build().metadata;
    }

    /** Executes in the form in which the transformation is committed and requires success. */
    private static ClusterMetadata apply(ClusterMetadata prev, LedgerTransformation t)
    {
        Transformation.Result result = t.wrap().execute(prev);
        assertTrue(String.valueOf(result), result.isSuccess());
        assertEquals(prev.epoch.nextEpoch(), result.success().metadata.epoch);
        return result.success().metadata;
    }

    private static String rejection(ClusterMetadata prev, LedgerTransformation t)
    {
        Transformation.Result result = t.wrap().execute(prev);
        assertTrue("expected a rejection of " + t, result.isRejected());
        return result.rejected().reason;
    }

    private static Claim claim(UUID id, NodeId owner, long now)
    {
        return new Claim(id, "scale_out", "CLUSTER", owner, "CLAIMED", 0, now, now + LEASE, ImmutableMap.of());
    }

    private static ClaimAction claimAction(Lane lane, UUID id, NodeId owner, long now)
    {
        return new ClaimAction(lane, claim(id, owner, now), now, 0);
    }

    private static ClusterLedger ledger(ClusterMetadata metadata)
    {
        return ClusterLedger.of(metadata);
    }

    // ---- SetClusterActionsEnabled ----

    @Test
    public void enableCreatesLedgerAndDisableKeepsIt()
    {
        ClusterMetadata base = empty();
        assertFalse(ClusterLedger.present(base));
        assertSame(ClusterLedger.EMPTY, ledger(base));

        ClusterMetadata on = apply(base, new SetClusterActionsEnabled(true));
        assertTrue(ClusterLedger.present(on));
        assertTrue(ledger(on).clusterActionsEnabled);
        assertEquals(ClusterLedger.FORMAT_VERSION, ledger(on).formatVersion);
        assertEquals(on.epoch, on.extensions.get(ClusterLedgerValue.KEY).lastModified());

        ClusterMetadata off = apply(on, new SetClusterActionsEnabled(false));
        assertTrue(ClusterLedger.present(off));
        assertFalse(ledger(off).clusterActionsEnabled);

        // "creates the ledger if absent" also holds for the disabling form
        ClusterMetadata created = apply(base, new SetClusterActionsEnabled(false));
        assertTrue(ClusterLedger.present(created));
        assertFalse(ledger(created).clusterActionsEnabled);
    }

    @Test
    public void enableIsIdempotent()
    {
        ClusterMetadata on = enabled();
        ClusterMetadata again = apply(on, new SetClusterActionsEnabled(true));
        assertEquals(ledger(on), ledger(again));
        // nothing changed: the value is not rewritten and keeps its last-modified epoch
        assertSame(on.extensions.get(ClusterLedgerValue.KEY), again.extensions.get(ClusterLedgerValue.KEY));
        assertEquals(on.epoch, again.extensions.get(ClusterLedgerValue.KEY).lastModified());
    }

    @Test
    public void disablingKeepsClaimsAndAllowsAdvanceAndRelease()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.MAINTENANCE, A, N1, NOW));
        md = apply(md, new SetClusterActionsEnabled(false));
        assertNotNull(ledger(md).claims.get(Lane.MAINTENANCE));
        assertTrue(rejection(md, claimAction(Lane.TOPOLOGY, B, N1, NOW)).contains("not enabled"));
        md = apply(md, new AdvanceAction(A, N1, 0, "P1", ImmutableMap.of(), NOW, LEASE));
        md = apply(md, new ReleaseAction(A, N1, "COMMITTED", NOW));
        assertTrue(ledger(md).claims.isEmpty());
    }

    // ---- ClaimAction ----

    @Test
    public void claimRejectedUnlessEnabled()
    {
        assertTrue(rejection(empty(), claimAction(Lane.MAINTENANCE, A, N1, NOW)).contains("not enabled"));
        ClusterMetadata off = apply(enabled(), new SetClusterActionsEnabled(false));
        assertTrue(rejection(off, claimAction(Lane.MAINTENANCE, A, N1, NOW)).contains("not enabled"));
    }

    @Test
    public void claimWritesStepZero()
    {
        Claim requested = claim(A, N1, NOW).withStep(7);
        ClusterMetadata md = apply(enabled(), new ClaimAction(Lane.TOPOLOGY, requested, NOW, 0));
        Claim held = ledger(md).claims.get(Lane.TOPOLOGY);
        assertEquals(requested.withStep(0), held);
        assertEquals(1, ledger(md).claims.size());
        assertSame(held, ledger(md).claim(A));
        assertEquals(Lane.TOPOLOGY, ledger(md).lane(A));
    }

    @Test
    public void oneClaimPerLaneAndLanesAreIndependent()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        assertTrue(rejection(md, claimAction(Lane.TOPOLOGY, B, N2, NOW + 1)).contains("is held by"));
        // the same action cannot hold two lanes
        assertTrue(rejection(md, claimAction(Lane.MAINTENANCE, A, N1, NOW)).contains("holds lane"));
        md = apply(md, claimAction(Lane.MAINTENANCE, B, N2, NOW));
        assertEquals(2, ledger(md).claims.size());
        assertEquals(N1, ledger(md).claims.get(Lane.TOPOLOGY).owner);
        assertEquals(N2, ledger(md).claims.get(Lane.MAINTENANCE).owner);
    }

    @Test
    public void claimIsIdempotentForTheSameActionAndOwner()
    {
        ClaimAction first = claimAction(Lane.TOPOLOGY, A, N1, NOW);
        ClusterMetadata md = apply(enabled(), first);
        ClusterMetadata again = apply(md, first);
        assertEquals(ledger(md), ledger(again));
        assertSame(md.extensions.get(ClusterLedgerValue.KEY), again.extensions.get(ClusterLedgerValue.KEY));

        // a late duplicate must not reset a claim that has advanced
        ClusterMetadata advanced = apply(md, new AdvanceAction(A, N1, 0, "P1", ImmutableMap.of("k", "v"), NOW + 5, LEASE));
        ClusterMetadata late = apply(advanced, first);
        assertEquals(ledger(advanced), ledger(late));
        assertEquals(1, ledger(late).claims.get(Lane.TOPOLOGY).step);

        // the same action id from another node is not the same claim
        assertTrue(rejection(md, claimAction(Lane.TOPOLOGY, A, N2, NOW)).contains("is held by"));
    }

    @Test
    public void expiredLeaseCanBeTakenOver()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        long expiry = NOW + LEASE;
        // strictly: leaseExpiresAtMillis < nowMillis
        assertTrue(rejection(md, claimAction(Lane.TOPOLOGY, B, N2, expiry - 1)).contains("is held by"));
        assertTrue(rejection(md, claimAction(Lane.TOPOLOGY, B, N2, expiry)).contains("is held by"));

        Claim takeover = new Claim(B, "scale_out", "CLUSTER", N2, Claim.RECOVERING, 0, expiry + 1, expiry + 1 + LEASE, ImmutableMap.of());
        ClusterMetadata taken = apply(md, new ClaimAction(Lane.TOPOLOGY, takeover, expiry + 1, 0));
        assertEquals(takeover, ledger(taken).claims.get(Lane.TOPOLOGY));
        assertNull(ledger(taken).claim(A));
        // the old owner is fenced off
        assertTrue(rejection(taken, new AdvanceAction(A, N1, 0, "P1", ImmutableMap.of(), expiry + 2, LEASE)).contains("no lane holds"));
        assertTrue(rejection(taken, new ReleaseAction(A, N1, "COMMITTED", expiry + 2)).contains("no lane holds"));
    }

    @Test
    public void cooldownFromRecent()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.MAINTENANCE, A, N1, NOW));
        md = apply(md, new ReleaseAction(A, N1, "ROLLED_BACK", NOW + 100));
        long cooldown = 1000;
        Claim next = claim(B, N2, NOW + 600);
        assertTrue(rejection(md, new ClaimAction(Lane.MAINTENANCE, next, NOW + 600, cooldown)).contains("cooldown"));
        // also from another lane: the rule is about actuator and target
        assertTrue(rejection(md, new ClaimAction(Lane.TOPOLOGY, next, NOW + 600, cooldown)).contains("cooldown"));
        assertTrue(rejection(md, new ClaimAction(Lane.MAINTENANCE, next, NOW + 1099, cooldown)).contains("cooldown"));
        // exactly cooldownMillis after the end it is over
        apply(md, new ClaimAction(Lane.MAINTENANCE, claim(B, N2, NOW + 1100), NOW + 1100, cooldown));
        // the limit is a field of the transformation
        apply(md, new ClaimAction(Lane.MAINTENANCE, next, NOW + 600, 0));
        // another target is not affected
        Claim other = new Claim(B, "scale_out", "TABLE:ks.t", N2, "CLAIMED", 0, NOW + 600, NOW + 600 + LEASE, ImmutableMap.of());
        apply(md, new ClaimAction(Lane.MAINTENANCE, other, NOW + 600, cooldown));
        assertTrue(rejection(md, new ClaimAction(Lane.MAINTENANCE, next, NOW + 600, -1)).contains("negative cooldown"));
    }

    @Test
    public void releasedActionCannotClaimAgain()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.MAINTENANCE, A, N1, NOW));
        md = apply(md, new ReleaseAction(A, N1, "COMMITTED", NOW + 1));
        assertTrue(rejection(md, claimAction(Lane.MAINTENANCE, A, N1, NOW + 2)).contains("already released"));
    }

    @Test
    public void laneRulesForInProgressSequences()
    {
        ClusterMetadata busy = withSequenceInProgress(enabled());
        assertFalse(busy.inProgressSequences.isEmpty());
        assertTrue(rejection(busy, claimAction(Lane.TOPOLOGY, A, N1, NOW)).contains("no metadata operation"));
        // FC3
        assertTrue(rejection(busy, claimAction(Lane.SCHEMA_LAYOUT, A, N1, NOW)).contains("no metadata operation"));
        apply(busy, claimAction(Lane.MAINTENANCE, A, N1, NOW));

        ClusterMetadata quiet = enabled();
        apply(quiet, claimAction(Lane.TOPOLOGY, A, N1, NOW));
        apply(quiet, claimAction(Lane.SCHEMA_LAYOUT, A, N1, NOW));
    }

    @Test
    public void takeoverOfAnExpiredClaimIsNotHeldUpByASequenceInProgress()
    {
        // the sequence in progress may be the action's own: the join that a scale-out started
        ClusterMetadata busy = withSequenceInProgress(apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW)));
        long later = NOW + LEASE + 1;
        Claim held = ledger(busy).claims.get(Lane.TOPOLOGY);
        Claim takeover = new Claim(B, held.actuator, held.target, N2, Claim.RECOVERING, 0, later, later + LEASE, held.state);
        assertEquals(takeover, ledger(apply(busy, new ClaimAction(Lane.TOPOLOGY, takeover, later, 0))).claims.get(Lane.TOPOLOGY));
        // only a recovery of the same actuator and target; a new action still waits for the sequence, and for the lease
        Claim fresh = new Claim(B, held.actuator, held.target, N2, "CLAIMED", 0, later, later + LEASE, held.state);
        assertTrue(rejection(busy, new ClaimAction(Lane.TOPOLOGY, fresh, later, 0)).contains("no metadata operation"));
        Claim other = new Claim(B, held.actuator, "TABLE:ks.other", N2, Claim.RECOVERING, 0, later, later + LEASE, held.state);
        assertTrue(rejection(busy, new ClaimAction(Lane.TOPOLOGY, other, later, 0)).contains("no metadata operation"));
        assertTrue(rejection(busy, new ClaimAction(Lane.TOPOLOGY, takeover, NOW + 1, 0)).contains("is held by"));
    }

    @Test
    public void claimWithLeaseInThePastRejected()
    {
        Claim stale = new Claim(A, "a", "t", N1, "P", 0, NOW, NOW, ImmutableMap.of());
        assertTrue(rejection(enabled(), new ClaimAction(Lane.MAINTENANCE, stale, NOW, 0)).contains("not in the future"));
    }

    // ---- size bounds: rejected, never truncated ----

    private static Map<String, String> entries(int n, int valueLength)
    {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < n; i++)
            m.put(String.format("k%02d", i), "v".repeat(valueLength));
        return m;
    }

    @Test
    public void claimStateBounds()
    {
        ClusterMetadata on = enabled();
        Map<String, String> max = entries(Claim.MAX_STATE_ENTRIES, 10);
        ClusterMetadata md = apply(on, new ClaimAction(Lane.MAINTENANCE, new Claim(A, "a", "t", N1, "P", 0, NOW, NOW + LEASE, max), NOW, 0));
        assertEquals(max, ledger(md).claims.get(Lane.MAINTENANCE).state);

        Map<String, String> tooMany = entries(Claim.MAX_STATE_ENTRIES + 1, 1);
        assertTrue(rejection(on, new ClaimAction(Lane.MAINTENANCE, new Claim(A, "a", "t", N1, "P", 0, NOW, NOW + LEASE, tooMany), NOW, 0)).contains("17 entries"));
        assertTrue(rejection(md, new AdvanceAction(A, N1, 0, "P", tooMany, NOW, LEASE)).contains("17 entries"));

        Map<String, String> tooBig = entries(4, 1100);
        assertTrue(rejection(on, new ClaimAction(Lane.MAINTENANCE, new Claim(A, "a", "t", N1, "P", 0, NOW, NOW + LEASE, tooBig), NOW, 0)).contains("bytes serialized"));
        assertTrue(rejection(md, new AdvanceAction(A, N1, 0, "P", tooBig, NOW, LEASE)).contains("bytes serialized"));
        // a rejected advance leaves the claim as it was
        assertEquals(max, ledger(md).claims.get(Lane.MAINTENANCE).state);

        // the byte bound is on the serialized state, at its exact limit
        Map<String, String> exact = new LinkedHashMap<>();
        exact.put("k", "v".repeat(Claim.MAX_STATE_BYTES - 2 - 2 - 1 - 2));
        assertEquals(Claim.MAX_STATE_BYTES, Claim.stateSize(exact));
        assertNull(Claim.invalidState(exact));
        exact.put("k", exact.get("k") + "v");
        assertNotNull(Claim.invalidState(exact));
    }

    @Test
    public void nameBounds()
    {
        ClusterMetadata on = enabled();
        String longName = "x".repeat(Claim.MAX_NAME_BYTES + 1);
        assertTrue(rejection(on, new ClaimAction(Lane.MAINTENANCE, new Claim(A, longName, "t", N1, "P", 0, NOW, NOW + LEASE, ImmutableMap.of()), NOW, 0)).contains("actuator is"));
        assertTrue(rejection(on, new ClaimAction(Lane.MAINTENANCE, new Claim(A, "a", "t", N1, longName, 0, NOW, NOW + LEASE, ImmutableMap.of()), NOW, 0)).contains("phase is"));
        assertTrue(rejection(on, new ClaimAction(Lane.MAINTENANCE, new Claim(A, "a", "t".repeat(Claim.MAX_TARGET_BYTES + 1), N1, "P", 0, NOW, NOW + LEASE, ImmutableMap.of()), NOW, 0)).contains("target is"));
        assertTrue(rejection(on, new ClaimAction(Lane.MAINTENANCE, new Claim(A, "", "t", N1, "P", 0, NOW, NOW + LEASE, ImmutableMap.of()), NOW, 0)).contains("actuator is empty"));
        // multi-byte characters count as bytes
        String wide = "é".repeat(Claim.MAX_NAME_BYTES / 2 + 1);
        assertTrue(rejection(on, new ClaimAction(Lane.MAINTENANCE, new Claim(A, wide, "t", N1, "P", 0, NOW, NOW + LEASE, ImmutableMap.of()), NOW, 0)).contains("actuator is"));

        ClusterMetadata md = apply(on, claimAction(Lane.MAINTENANCE, A, N1, NOW));
        assertTrue(rejection(md, new AdvanceAction(A, N1, 0, longName, ImmutableMap.of(), NOW, LEASE)).contains("phase is"));
        assertTrue(rejection(md, new ReleaseAction(A, N1, "s".repeat(ClusterLedger.MAX_FINAL_STATE_BYTES + 1), NOW)).contains("final state is"));
    }

    @Test
    public void recentIsBoundedNewestFirst()
    {
        ClusterMetadata md = enabled();
        for (int i = 0; i < ClusterLedger.MAX_RECENT + 5; i++)
        {
            UUID id = new UUID(9, i);
            Claim c = new Claim(id, "a", "t" + i, N1, "P", 0, NOW + i, NOW + i + LEASE, ImmutableMap.of());
            md = apply(md, new ClaimAction(Lane.MAINTENANCE, c, NOW + i, 0));
            md = apply(md, new ReleaseAction(id, N1, "COMMITTED", NOW + i));
        }
        List<Recent> recent = ledger(md).recent;
        assertEquals(ClusterLedger.MAX_RECENT, recent.size());
        assertEquals(new UUID(9, ClusterLedger.MAX_RECENT + 4), recent.get(0).actionId);
        assertEquals(new UUID(9, 5), recent.get(ClusterLedger.MAX_RECENT - 1).actionId);
        // and it still serializes
        assertEquals(ledger(md), ledger(roundTrip(md)));
    }

    // ---- AdvanceAction ----

    @Test
    public void advance()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        ClusterMetadata next = apply(md, new AdvanceAction(A, N1, 0, "PROVISIONING", ImmutableMap.of("request", "r1"), NOW + 10, LEASE));
        Claim c = ledger(next).claims.get(Lane.TOPOLOGY);
        assertEquals(1, c.step);
        assertEquals("PROVISIONING", c.phase);
        assertEquals(ImmutableMap.of("request", "r1"), c.state);
        assertEquals(NOW + 10 + LEASE, c.leaseExpiresAtMillis);
        assertEquals(NOW, c.claimedAtMillis);
        assertEquals(N1, c.owner);
        assertEquals(next.epoch, next.extensions.get(ClusterLedgerValue.KEY).lastModified());
    }

    @Test
    public void advanceRejections()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        assertTrue(rejection(md, new AdvanceAction(B, N1, 0, "P1", ImmutableMap.of(), NOW, LEASE)).contains("no lane holds"));
        assertTrue(rejection(md, new AdvanceAction(A, N2, 0, "P1", ImmutableMap.of(), NOW, LEASE)).contains("owned by node 1"));
        assertTrue(rejection(md, new AdvanceAction(A, N1, 1, "P1", ImmutableMap.of(), NOW, LEASE)).contains("at step 0"));
        assertTrue(rejection(md, new AdvanceAction(A, N1, 0, "P1", ImmutableMap.of(), NOW, 0)).contains("lease is not positive"));
        assertTrue(rejection(empty(), new AdvanceAction(A, N1, 0, "P1", ImmutableMap.of(), NOW, LEASE)).contains("no lane holds"));
        // a rejected transformation changes nothing and does not create a ledger
        assertFalse(ClusterLedger.present(empty()));
    }

    @Test
    public void advanceIsIdempotentForTheSameStep()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        AdvanceAction advance = new AdvanceAction(A, N1, 0, "P1", ImmutableMap.of("k", "v"), NOW + 10, LEASE);
        ClusterMetadata once = apply(md, advance);
        ClusterMetadata twice = apply(once, advance);
        assertEquals(ledger(once), ledger(twice));
        assertEquals(1, ledger(twice).claims.get(Lane.TOPOLOGY).step);
        assertSame(once.extensions.get(ClusterLedgerValue.KEY), twice.extensions.get(ClusterLedgerValue.KEY));

        // a different advance with the stale step is a lost compare-and-set, not a duplicate
        assertTrue(rejection(once, new AdvanceAction(A, N1, 0, "P2", ImmutableMap.of("k", "v"), NOW + 10, LEASE)).contains("at step 1"));
        assertTrue(rejection(once, new AdvanceAction(A, N1, 0, "P1", ImmutableMap.of("k", "v"), NOW + 11, LEASE)).contains("at step 1"));
        // and once the claim moved on, the old advance is rejected too
        ClusterMetadata moved = apply(once, new AdvanceAction(A, N1, 1, "P2", ImmutableMap.of(), NOW + 20, LEASE));
        assertTrue(rejection(moved, advance).contains("at step 2"));
    }

    @Test
    public void renewalIsAnAdvanceThatKeepsPhaseAndState()
    {
        ClusterMetadata md = apply(enabled(), new ClaimAction(Lane.TOPOLOGY, new Claim(A, "a", "t", N1, "P", 0, NOW, NOW + LEASE, ImmutableMap.of("k", "v")), NOW, 0));
        ClusterMetadata renewed = apply(md, new AdvanceAction(A, N1, 0, "P", ImmutableMap.of("k", "v"), NOW + LEASE / 3, LEASE));
        Claim c = ledger(renewed).claims.get(Lane.TOPOLOGY);
        assertEquals(NOW + LEASE / 3 + LEASE, c.leaseExpiresAtMillis);
        assertEquals(1, c.step);
        assertEquals("P", c.phase);
    }

    // ---- ReleaseAction ----

    @Test
    public void release()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        md = apply(md, claimAction(Lane.MAINTENANCE, B, N2, NOW));
        ClusterMetadata released = apply(md, new ReleaseAction(A, N1, "ROLLED_BACK", NOW + 50));
        assertNull(ledger(released).claims.get(Lane.TOPOLOGY));
        assertNotNull(ledger(released).claims.get(Lane.MAINTENANCE));
        assertEquals(List.of(new Recent(A, "scale_out", "CLUSTER", Lane.TOPOLOGY, "ROLLED_BACK", NOW + 50)), ledger(released).recent);
        assertEquals(ledger(released).recent.get(0), ledger(released).recent(A));
        // the lane is free again
        apply(released, new ClaimAction(Lane.TOPOLOGY, new Claim(new UUID(3, 3), "other", "t", N2, "P", 0, NOW + 60, NOW + 60 + LEASE, ImmutableMap.of()), NOW + 60, 0));
    }

    @Test
    public void releaseRejections()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        assertTrue(rejection(md, new ReleaseAction(B, N1, "COMMITTED", NOW)).contains("no lane holds"));
        assertTrue(rejection(md, new ReleaseAction(A, N2, "COMMITTED", NOW)).contains("owned by node 1"));
        assertTrue(rejection(empty(), new ReleaseAction(A, N1, "COMMITTED", NOW)).contains("no lane holds"));
    }

    @Test
    public void forcedReleaseIgnoresTheOwner()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        ClusterMetadata released = apply(md, new ReleaseAction(A, null, "FORCE_RELEASED", NOW + 1));
        assertTrue(ledger(released).claims.isEmpty());
        assertEquals("FORCE_RELEASED", ledger(released).recent.get(0).finalState);
        // the owner finds its claim gone
        assertTrue(rejection(released, new AdvanceAction(A, N1, 0, "P1", ImmutableMap.of(), NOW + 2, LEASE)).contains("no lane holds"));
    }

    @Test
    public void releaseIsIdempotent()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        ReleaseAction release = new ReleaseAction(A, N1, "COMMITTED", NOW + 1);
        ClusterMetadata once = apply(md, release);
        ClusterMetadata twice = apply(once, release);
        assertEquals(ledger(once), ledger(twice));
        assertEquals(1, ledger(twice).recent.size());
        assertSame(once.extensions.get(ClusterLedgerValue.KEY), twice.extensions.get(ClusterLedgerValue.KEY));
        // a release with another final state is not the same release
        assertTrue(rejection(once, new ReleaseAction(A, N1, "ROLLED_BACK", NOW + 1)).contains("no lane holds"));
    }

    // ---- value object, determinism, serialization ----

    @Test
    public void valueFromPrevIsNeverMutated()
    {
        ClusterMetadata prev = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        LedgerTransformation[] changes = { new SetClusterActionsEnabled(false),
                                           claimAction(Lane.MAINTENANCE, B, N2, NOW),
                                           new AdvanceAction(A, N1, 0, "P1", ImmutableMap.of("k", "v"), NOW + 1, LEASE),
                                           new ReleaseAction(A, N1, "COMMITTED", NOW + 1) };
        for (LedgerTransformation t : changes)
        {
            ExtensionValue<?> before = prev.extensions.get(ClusterLedgerValue.KEY);
            ClusterLedger ledgerBefore = ledger(prev);
            Epoch modifiedBefore = before.lastModified();
            String printed = before.toString();

            ClusterMetadata next = apply(prev, t);

            assertSame(t.toString(), before, prev.extensions.get(ClusterLedgerValue.KEY));
            assertSame(ledgerBefore, ((ClusterLedgerValue) before).getValue());
            assertEquals(modifiedBefore, before.lastModified());
            assertEquals(printed, before.toString());
            ExtensionValue<?> after = next.extensions.get(ClusterLedgerValue.KEY);
            assertNotSame(t.toString(), before, after);
            assertEquals(next.epoch, after.lastModified());
            assertTrue(next.epoch.isAfter(modifiedBefore));
        }
    }

    @Test
    public void ledgerOfAValueCannotBeReplaced()
    {
        ClusterLedgerValue value = new ClusterLedgerValue(ClusterLedger.EMPTY);
        try
        {
            value.setValue(ClusterLedger.EMPTY);
            throw new AssertionError("setValue on an initialised value must fail");
        }
        catch (IllegalStateException expected)
        {
            // immutable
        }
    }

    @Test
    public void executeIsDeterministic() throws IOException
    {
        ClusterMetadata prev = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        prev = apply(prev, new ReleaseAction(A, N1, "COMMITTED", NOW + 1));
        Map<String, String> state = new LinkedHashMap<>();
        state.put("b", "2");
        state.put("a", "1");
        LedgerTransformation[] all = { new SetClusterActionsEnabled(false),
                                       new ClaimAction(Lane.MAINTENANCE, new Claim(B, "a", "t", N2, "P", 0, NOW + 9, NOW + 9 + LEASE, state), NOW + 9, 5) };
        for (LedgerTransformation t : all)
        {
            ClusterMetadata one = apply(prev, t);
            // the same input on "another node": the transformation as it comes out of the log
            ClusterMetadata two = roundTrip(t).execute(roundTrip(prev)).success().metadata;
            assertEquals(ledger(one), ledger(two));
            assertEquals(one.extensions.get(ClusterLedgerValue.KEY), two.extensions.get(ClusterLedgerValue.KEY));
            assertEquals(bytes(one), bytes(two));
        }
        // equal state maps serialize identically whatever their insertion order
        Map<String, String> reversed = new LinkedHashMap<>();
        reversed.put("a", "1");
        reversed.put("b", "2");
        ClusterMetadata x = apply(prev, new ClaimAction(Lane.MAINTENANCE, new Claim(B, "a", "t", N2, "P", 0, NOW + 9, NOW + 9 + LEASE, state), NOW + 9, 5));
        ClusterMetadata y = apply(prev, new ClaimAction(Lane.MAINTENANCE, new Claim(B, "a", "t", N2, "P", 0, NOW + 9, NOW + 9 + LEASE, reversed), NOW + 9, 5));
        assertEquals(bytes(x), bytes(y));
    }

    /** No clock, configuration or local state on any path that {@code execute} or a serializer can reach. */
    @Test
    public void replayedCodeDoesNotReadClocksOrConfiguration() throws IOException
    {
        String[] sources = { "Lane", "Claim", "Recent", "ClusterLedger", "ClusterLedgerValue", "LedgerValueBase", "LedgerTransformation",
                             "SetClusterActionsEnabled", "ClaimAction", "AdvanceAction", "ReleaseAction", "AutonomicTransformations" };
        String[] forbidden = { "Clock", "currentTimeMillis", "nanoTime", "DatabaseDescriptor", "AutonomicService", "AutonomicConfig",
                               "FBUtilities", "Random", "randomUUID", "TimeUUID", "System.", "ClusterMetadata.current", "ClusterMetadataService",
                               "StorageService", "Instant", "new Date" };
        for (String source : sources)
        {
            String text = new String(Files.readAllBytes(Paths.get("src/java/org/apache/cassandra/autonomic/coord", source + ".java")));
            // comments may name what is forbidden
            String code = text.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("//.*", "");
            for (String word : forbidden)
                assertFalse(source + ".java must not use " + word, code.contains(word));
            for (String line : code.split("\n"))
                if (line.startsWith("import org.apache.cassandra."))
                    assertTrue(source + ".java: unexpected dependency " + line,
                               line.matches("import org\\.apache\\.cassandra\\.(tcm|io\\.util|db\\.TypeSizes|exceptions\\.ExceptionCode|autonomic\\.coord)[.;].*"));
        }
    }

    @Test
    public void metadataRoundTripWithLedger() throws IOException
    {
        ClusterMetadata md = apply(enabled(), new ClaimAction(Lane.TOPOLOGY, new Claim(A, "scale_out", "CLUSTER", N1, "CLAIMED", 0, NOW, NOW + LEASE, entries(3, 20)), NOW, 0));
        md = apply(md, claimAction(Lane.SCHEMA_LAYOUT, B, N2, NOW));
        md = apply(md, new AdvanceAction(B, N2, 0, "COPYING", ImmutableMap.of("layout", "7"), NOW + 3, LEASE));
        md = apply(md, new ReleaseAction(B, N2, "COMMITTED_CLEANUP_PENDING", NOW + 4));

        ClusterMetadata read = roundTrip(md);
        assertEquals(ledger(md), ledger(read));
        assertEquals(md.extensions.get(ClusterLedgerValue.KEY), read.extensions.get(ClusterLedgerValue.KEY));
        assertEquals(md.extensions.get(ClusterLedgerValue.KEY).lastModified(), read.extensions.get(ClusterLedgerValue.KEY).lastModified());
        assertEquals(bytes(md).remaining(), ClusterMetadata.serializer.serializedSize(md, VERSION));
        assertEquals(bytes(md), bytes(read));

        // an empty ledger too
        ClusterMetadata on = enabled();
        assertEquals(ledger(on), ledger(roundTrip(on)));
    }

    /** The snapshot path caps last-modified epochs through withLastModified and casts the result. */
    @Test
    public void forceEpochKeepsTheLedger()
    {
        ClusterMetadata md = apply(enabled(), claimAction(Lane.TOPOLOGY, A, N1, NOW));
        md = apply(md, new AdvanceAction(A, N1, 0, "P1", ImmutableMap.of(), NOW, LEASE));
        Epoch earlier = Epoch.create(md.epoch.getEpoch() - 1);
        ClusterMetadata forced = md.forceEpoch(earlier);
        assertEquals(ledger(md), ledger(forced));
        assertTrue(forced.extensions.get(ClusterLedgerValue.KEY) instanceof ClusterLedgerValue);
        assertEquals(earlier, forced.extensions.get(ClusterLedgerValue.KEY).lastModified());
    }

    @Test
    public void unknownLedgerFormatFailsLoudly() throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        new ClusterLedgerValue(ClusterLedger.EMPTY).serialize(out, VERSION);
        ByteBuffer bytes = out.buffer();
        // the format version is written first
        assertEquals(ClusterLedger.FORMAT_VERSION, bytes.getInt(0));
        bytes.putInt(0, ClusterLedger.FORMAT_VERSION + 1);
        try
        {
            new ClusterLedgerValue().deserialize(new DataInputBuffer(bytes, false), VERSION);
            throw new AssertionError("an unknown format must not be read");
        }
        catch (IOException expected)
        {
            assertTrue(expected.getMessage().contains("format"));
        }
    }

    @Test
    public void transformationRoundTrips() throws IOException
    {
        LedgerTransformation[] all = { new SetClusterActionsEnabled(true),
                                       new SetClusterActionsEnabled(false),
                                       new ClaimAction(Lane.SCHEMA_LAYOUT, new Claim(A, "partition_split", "TABLE:ks.t", N1, Claim.RECOVERING, 0, NOW, NOW + LEASE, entries(5, 9)), NOW, 1234),
                                       new AdvanceAction(A, N2, 41, "BACKFILL", entries(2, 3), NOW, LEASE),
                                       new AdvanceAction(A, N2, 0, "P", ImmutableMap.of(), NOW, LEASE),
                                       new ReleaseAction(A, N1, "COMMITTED", NOW),
                                       new ReleaseAction(A, null, "FORCE_RELEASED", NOW) };
        for (LedgerTransformation t : all)
        {
            // its own serializer
            assertEquals(t, roundTrip(t));
            assertEquals(t.hashCode(), roundTrip(t).hashCode());

            // wrapped, as it is committed
            CustomTransformation wrapped = t.wrap();
            DataOutputBuffer out = new DataOutputBuffer();
            CustomTransformation.serializer.serialize(wrapped, out, VERSION);
            assertEquals(out.getLength(), CustomTransformation.serializer.serializedSize(wrapped, VERSION));
            CustomTransformation read = CustomTransformation.serializer.deserialize(new DataInputBuffer(out.buffer(), false), VERSION);
            assertEquals(wrapped, read);
            assertEquals(t, read.child());
            assertEquals(t.getClass(), read.child().getClass());

            // and through the log's transformation serializer, keyed by Kind.CUSTOM
            assertEquals(Transformation.Kind.CUSTOM, wrapped.kind());
            ByteBuffer logged = wrapped.kind().toVersionedBytes(wrapped);
            Transformation replayed = Transformation.Kind.CUSTOM.fromVersionedBytes(logged);
            assertEquals(wrapped, replayed);
        }
    }

    @Test
    public void namesAreStableAndRegistered()
    {
        // part of the log format
        assertEquals("org.apache.cassandra.autonomic.ledger", ClusterLedgerValue.KEY.id);
        assertEquals("org.apache.cassandra.autonomic.coord.ClusterLedgerValue", ClusterLedgerValue.class.getName());
        assertEquals("org.apache.cassandra.autonomic.coord.SetClusterActionsEnabled", SetClusterActionsEnabled.NAME);
        assertEquals("org.apache.cassandra.autonomic.coord.ClaimAction", ClaimAction.NAME);
        assertEquals("org.apache.cassandra.autonomic.coord.AdvanceAction", AdvanceAction.NAME);
        assertEquals("org.apache.cassandra.autonomic.coord.ReleaseAction", ReleaseAction.NAME);
        assertSame(ClaimAction.serializer, CustomTransformation.getSerializer(ClaimAction.NAME));
        assertSame(AdvanceAction.serializer, CustomTransformation.getSerializer(AdvanceAction.NAME));
        assertSame(ReleaseAction.serializer, CustomTransformation.getSerializer(ReleaseAction.NAME));
        assertSame(SetClusterActionsEnabled.serializer, CustomTransformation.getSerializer(SetClusterActionsEnabled.NAME));
        assertEquals(0, Lane.TOPOLOGY.id);
        assertEquals(1, Lane.SCHEMA_LAYOUT.id);
        assertEquals(2, Lane.MAINTENANCE.id);
    }

    private static ByteBuffer bytes(ClusterMetadata metadata) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        ClusterMetadata.serializer.serialize(metadata, out, VERSION);
        return out.buffer();
    }

    private static ClusterMetadata roundTrip(ClusterMetadata metadata)
    {
        try
        {
            return ClusterMetadata.serializer.deserialize(new DataInputBuffer(bytes(metadata), false), VERSION);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static LedgerTransformation roundTrip(LedgerTransformation t) throws IOException
    {
        var serializer = CustomTransformation.getSerializer(t.name());
        DataOutputBuffer out = new DataOutputBuffer();
        serializer.serialize(t, out, VERSION);
        assertEquals(t + " size", out.getLength(), serializer.serializedSize(t, VERSION));
        return (LedgerTransformation) serializer.deserialize(new DataInputBuffer(out.buffer(), false), VERSION);
    }
}
