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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.coord.ClusterCoordinator.Outcome;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator.Status;
import org.apache.cassandra.autonomic.coord.FakeCluster.CommitMode;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.CassandraVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** {@link ClusterCoordinator} over a fake commit function, clock and failure detector. */
public class ClusterCoordinatorTest
{
    private FakeCluster cluster;
    private FakeCluster.Node env1;
    private FakeCluster.Node env2;
    private ClusterCoordinator c1;
    private ClusterCoordinator c2;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Before
    public void setup()
    {
        cluster = new FakeCluster(2);
        env1 = cluster.node(1);
        env2 = cluster.node(2);
        c1 = new ClusterCoordinator(env1);
        c2 = new ClusterCoordinator(env2);
    }

    private void enable()
    {
        c1.setEnabled(true);
        env2.catchUp();
    }

    private Outcome claim(ClusterCoordinator c, Lane lane, UUID id)
    {
        return c.claim(lane, id, "fake", "CLUSTER", "CLAIMED", ImmutableMap.of());
    }

    // ---- the operator gate ----

    @Test
    public void nothingIsCommittedBeforeEnable()
    {
        UUID id = cluster.newActionId();
        assertFalse(c1.enabled());
        Outcome claimed = claim(c1, Lane.MAINTENANCE, id);
        assertEquals(Status.LOST, claimed.status);
        assertTrue(claimed.detail.contains("not enabled"));

        Claim imagined = new Claim(id, "fake", "CLUSTER", env1.self, "CLAIMED", 0, 1, Long.MAX_VALUE, ImmutableMap.of());
        assertEquals(Status.UNKNOWN, c1.advance(imagined, "P1", ImmutableMap.of()).status);
        assertEquals(Status.UNKNOWN, c1.renew(imagined).status);
        assertEquals(Status.UNKNOWN, c1.release(imagined, "COMMITTED").status);
        assertEquals(Status.UNKNOWN, c1.commitInstead(imagined, new SetClusterActionsEnabled(true).wrap(), "P1", ImmutableMap.of()).status);
        assertEquals(Status.LOST, c1.takeOver(Lane.MAINTENANCE, imagined, cluster.newActionId()).status);
        assertEquals(0, c1.releaseAbandoned());
        try
        {
            c1.forceRelease(Lane.MAINTENANCE);
            throw new AssertionError("nothing to release");
        }
        catch (IllegalStateException expected)
        {
            // lane is free
        }
        // disabling what was never enabled writes nothing either
        c1.setEnabled(false);

        assertEquals("no commit was attempted", 0, cluster.attempts);
        assertFalse(ClusterLedger.present(cluster.log));
        assertTrue(cluster.log.extensions.isEmpty());
    }

    @Test
    public void enableDisable()
    {
        c1.setEnabled(true);
        assertTrue(c1.enabled());
        assertEquals(1, cluster.applied.size());
        assertTrue(cluster.applied.get(0) instanceof CustomTransformation);
        // already enabled: no further log entry
        c1.setEnabled(true);
        assertEquals(1, cluster.attempts);
        // node 2 has not seen it yet and acts on what it sees
        assertFalse(c2.enabled());
        env2.catchUp();
        assertTrue(c2.enabled());

        c2.setEnabled(false);
        assertFalse(c2.enabled());
        assertTrue(ClusterLedger.present(cluster.log));
        assertEquals(2, cluster.applied.size());
        // a disabled ledger refuses claims locally, without a commit
        env1.catchUp();
        assertEquals(Status.LOST, claim(c1, Lane.MAINTENANCE, cluster.newActionId()).status);
        assertEquals(2, cluster.attempts);
    }

    @Test
    public void enableRefusedWithMixedVersions()
    {
        NodeVersion other = new NodeVersion(new CassandraVersion("5.1.0"), Version.V10);
        cluster.log = FakeCluster.register(cluster.log, 3, other);
        env1.catchUp();
        assertFalse(cluster.log.directory.clusterMinVersion.equals(cluster.log.directory.clusterMaxVersion));
        try
        {
            c1.setEnabled(true);
            throw new AssertionError("enable must be refused");
        }
        catch (IllegalStateException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("different versions"));
        }
        assertEquals(0, cluster.attempts);
        assertFalse(ClusterLedger.present(cluster.log));
    }

    @Test
    public void enableThatDoesNotShowInTheLedgerIsAnError()
    {
        env1.mode = CommitMode.DROP;
        try
        {
            c1.setEnabled(true);
            throw new AssertionError("must report the failure");
        }
        catch (IllegalStateException expected)
        {
            assertTrue(expected.getMessage().contains("timeout"));
        }
    }

    // ---- preferred proposer ----

    @Test
    public void preferredProposerIsLowestAliveJoinedNode()
    {
        assertTrue(c1.preferredProposer());
        assertFalse(c2.preferredProposer());
        env2.down.add(env1.self);
        assertTrue("node 2 does not see node 1 alive", c2.preferredProposer());
        assertEquals(List.of(env1.self), c2.membersNotAlive());
        assertTrue(c1.membersNotAlive().isEmpty());
    }

    @Test
    public void takeoverIsDueAtExpiryForThePreferredProposerAndOneMarginPerRankLater()
    {
        FakeCluster three = new FakeCluster(3);
        ClusterCoordinator[] c = new ClusterCoordinator[4];
        FakeCluster.Node[] env = new FakeCluster.Node[4];
        for (int i = 1; i <= 3; i++)
        {
            env[i] = three.node(i);
            c[i] = new ClusterCoordinator(env[i]);
        }
        c[1].setEnabled(true);
        Claim claim = claim(c[1], Lane.MAINTENANCE, three.newActionId()).claim;
        for (int i = 2; i <= 3; i++)
            env[i].catchUp();
        for (int i = 1; i <= 3; i++)
            assertEquals(i - 1, c[i].proposerRank());

        long expiry = claim.leaseExpiresAtMillis;
        long margin = env[1].margin;
        three.now.set(expiry + 1);
        assertEquals(claim, c[1].takeoverDue().get(Lane.MAINTENANCE));
        assertTrue(c[2].takeoverDue().isEmpty());
        three.now.set(expiry + margin);
        assertTrue("one margin after expiry, not yet", c[2].takeoverDue().isEmpty());
        three.now.set(expiry + margin + 1);
        assertEquals(claim, c[2].takeoverDue().get(Lane.MAINTENANCE));
        assertTrue(c[3].takeoverDue().isEmpty());
        three.now.set(expiry + 2 * margin + 1);
        assertEquals(claim, c[3].takeoverDue().get(Lane.MAINTENANCE));

        // a node that is down in a node's view does not count
        three.now.set(expiry + 1);
        env[3].down.add(env[1].self);
        assertEquals(1, c[3].proposerRank());
        assertTrue(c[3].takeoverDue().isEmpty());
        env[3].down.add(env[2].self);
        assertEquals(0, c[3].proposerRank());
        assertEquals(claim, c[3].takeoverDue().get(Lane.MAINTENANCE));
    }

    // ---- claims ----

    @Test
    public void concurrentClaimsHaveOneOwner()
    {
        enable();
        UUID a = cluster.newActionId();
        UUID b = cluster.newActionId();
        // both nodes decide on the same view and commit; the log admits one
        Outcome first = claim(c1, Lane.TOPOLOGY, a);
        Outcome second = claim(c2, Lane.TOPOLOGY, b);
        assertTrue(first.ok());
        assertEquals(env1.self, first.claim.owner);
        assertEquals(0, first.claim.step);
        assertEquals(cluster.now.get() + env1.lease, first.claim.leaseExpiresAtMillis);
        assertEquals(Status.UNKNOWN, second.status);
        assertTrue(second.detail, second.detail.contains("is held by"));
        assertEquals(1, cluster.ledger().claims.size());
        assertSame(cluster.ledger().claims.get(Lane.TOPOLOGY), first.claim);
        assertNotNull(c1.ownClaim(a));
        assertNull(c2.ownClaim(a));
        assertNull(c2.ownClaim(b));
        // another lane is independent
        assertTrue(claim(c2, Lane.MAINTENANCE, b).ok());
    }

    @Test
    public void claimDecidedByTheLedgerNotTheCommitPath()
    {
        enable();
        UUID a = cluster.newActionId();
        // the commit path reports a timeout although the claim was applied: not seen, so not won...
        env1.mode = CommitMode.APPLY_UNSEEN;
        Outcome outcome = claim(c1, Lane.TOPOLOGY, a);
        assertEquals(Status.UNKNOWN, outcome.status);
        assertNotNull(cluster.ledger().claim(a));
        // ...and once it shows up it is released, because nothing was started under it
        env1.mode = CommitMode.APPLY;
        assertEquals(0, c1.releaseAbandoned());
        env1.catchUp();
        assertEquals(1, c1.releaseAbandoned());
        assertTrue(cluster.ledger().claims.isEmpty());
        assertEquals(ClusterCoordinator.ABANDONED, cluster.ledger().recent(a).finalState);
        assertEquals(0, c1.releaseAbandoned());
    }

    @Test
    public void cooldownSuppliedFromTheLastFinalState()
    {
        enable();
        env1.cooldownAfterCommit = 1000;
        env1.cooldownAfterRollback = 5000;
        UUID a = cluster.newActionId();
        Outcome first = claim(c1, Lane.MAINTENANCE, a);
        assertEquals(0, ((ClaimAction) ((CustomTransformation) env1.attempted.get(env1.attempted.size() - 1)).child()).cooldownMillis);
        assertTrue(c1.release(first.claim, "ROLLED_BACK").ok());

        cluster.now.addAndGet(2000);
        Outcome second = claim(c1, Lane.MAINTENANCE, cluster.newActionId());
        assertEquals(5000, ((ClaimAction) ((CustomTransformation) env1.attempted.get(env1.attempted.size() - 1)).child()).cooldownMillis);
        assertEquals(Status.UNKNOWN, second.status);
        assertTrue(second.detail.contains("cooldown"));

        cluster.now.addAndGet(3000);
        Outcome third = claim(c1, Lane.MAINTENANCE, cluster.newActionId());
        assertTrue(third.ok());
        assertTrue(c1.release(third.claim, "COMMITTED").ok());
        cluster.now.addAndGet(1000);
        assertTrue(claim(c1, Lane.MAINTENANCE, cluster.newActionId()).ok());
        assertEquals(1000, ((ClaimAction) ((CustomTransformation) env1.attempted.get(env1.attempted.size() - 1)).child()).cooldownMillis);
    }

    // ---- advance, renew, release ----

    @Test
    public void advanceAndRelease()
    {
        enable();
        UUID a = cluster.newActionId();
        Claim claim = claim(c1, Lane.TOPOLOGY, a).claim;
        cluster.now.addAndGet(500);
        Outcome advanced = c1.advance(claim, "P1", ImmutableMap.of("k", "v"));
        assertTrue(advanced.ok());
        assertEquals(1, advanced.claim.step);
        assertEquals("P1", advanced.claim.phase);
        assertEquals(cluster.now.get() + env1.lease, advanced.claim.leaseExpiresAtMillis);

        Outcome released = c1.release(advanced.claim, "COMMITTED");
        assertTrue(released.ok());
        assertTrue(cluster.ledger().claims.isEmpty());
        assertEquals("COMMITTED", cluster.ledger().recent(a).finalState);
        // releasing again finds it released
        assertTrue(c1.release(advanced.claim, "COMMITTED").ok());
    }

    @Test
    public void invalidStateIsRefusedBeforeAnyCommit()
    {
        enable();
        Claim claim = claim(c1, Lane.TOPOLOGY, cluster.newActionId()).claim;
        int before = cluster.attempts;
        Map<String, String> tooMany = new java.util.HashMap<>();
        for (int i = 0; i <= Claim.MAX_STATE_ENTRIES; i++)
            tooMany.put("k" + i, "v");
        try
        {
            c1.advance(claim, "P1", tooMany);
            throw new AssertionError("must be refused");
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage().contains("entries"));
        }
        assertEquals(before, cluster.attempts);
        assertEquals(Status.LOST, c1.claim(Lane.MAINTENANCE, cluster.newActionId(), "fake", "CLUSTER", "P", tooMany).status);
        assertEquals(before, cluster.attempts);
    }

    @Test
    public void ambiguousAdvanceIsRetriedIdempotently()
    {
        enable();
        Claim claim = claim(c1, Lane.TOPOLOGY, cluster.newActionId()).claim;

        // lost before the log: nothing changed, retry
        env1.mode = CommitMode.DROP;
        Outcome dropped = c1.advance(claim, "P1", ImmutableMap.of());
        assertEquals(Status.UNKNOWN, dropped.status);
        assertEquals(0, dropped.claim.step);
        assertEquals(0, cluster.ledger().claims.get(Lane.TOPOLOGY).step);

        // applied but not seen: still unknown to the node
        env1.mode = CommitMode.APPLY_UNSEEN;
        Outcome unseen = c1.advance(claim, "P1", ImmutableMap.of());
        assertEquals(Status.UNKNOWN, unseen.status);
        assertEquals(1, cluster.ledger().claims.get(Lane.TOPOLOGY).step);

        // the same advance again (same clock): the transformation is idempotent, the step moves once
        env1.mode = CommitMode.APPLY;
        Outcome retried = c1.advance(claim, "P1", ImmutableMap.of());
        assertTrue(retried.toString(), retried.ok());
        assertEquals(1, retried.claim.step);
        assertEquals(1, cluster.ledger().claims.get(Lane.TOPOLOGY).step);
    }

    @Test
    public void staleStepIsNotMistakenForSuccess()
    {
        enable();
        Claim claim = claim(c1, Lane.TOPOLOGY, cluster.newActionId()).claim;
        // a renewal is applied without the node seeing it
        env1.mode = CommitMode.APPLY_UNSEEN;
        assertEquals(Status.UNKNOWN, c1.renew(claim).status);
        env1.mode = CommitMode.APPLY;
        cluster.now.addAndGet(10);
        // the advance to P1 from the stale step is rejected; the ledger shows step 1 in the OLD phase: not a success
        Outcome stale = c1.advance(claim, "P1", ImmutableMap.of());
        assertEquals(Status.UNKNOWN, stale.status);
        assertEquals(1, stale.claim.step);
        assertEquals("CLAIMED", stale.claim.phase);
        // with the claim as the ledger has it, the advance goes through
        Outcome fresh = c1.advance(stale.claim, "P1", ImmutableMap.of());
        assertTrue(fresh.ok());
        assertEquals(2, fresh.claim.step);
        assertEquals("P1", fresh.claim.phase);
    }

    @Test
    public void leaseRenewalAndSafetyMargin()
    {
        enable();
        Claim claim = claim(c1, Lane.TOPOLOGY, cluster.newActionId()).claim;
        assertTrue(c1.mayAct(claim));
        assertFalse(c1.renewalDue(claim));
        cluster.now.addAndGet(env1.lease / 3 - 1);
        assertFalse(c1.renewalDue(claim));
        cluster.now.addAndGet(1);
        assertTrue(c1.renewalDue(claim));

        Outcome renewed = c1.renew(claim);
        assertTrue(renewed.ok());
        assertEquals("CLAIMED", renewed.claim.phase);
        assertEquals(1, renewed.claim.step);
        assertEquals(cluster.now.get() + env1.lease, renewed.claim.leaseExpiresAtMillis);
        assertFalse(c1.renewalDue(renewed.claim));

        // the owner stops acting at leaseExpiresAt - margin, before anyone may take over
        cluster.now.set(renewed.claim.leaseExpiresAtMillis - env1.margin - 1);
        assertTrue(c1.mayAct(renewed.claim));
        cluster.now.addAndGet(1);
        assertFalse(c1.mayAct(renewed.claim));
        assertTrue(c2.expiredClaims().isEmpty());
        cluster.now.set(renewed.claim.leaseExpiresAtMillis);
        env2.catchUp();
        assertTrue(c2.expiredClaims().isEmpty());
        cluster.now.addAndGet(1);
        assertEquals(renewed.claim, c2.expiredClaims().get(Lane.TOPOLOGY));
    }

    // ---- takeover and the returning owner ----

    @Test
    public void takeoverOfExpiredClaimAndReturningOwner()
    {
        enable();
        UUID a = cluster.newActionId();
        Claim claim = claim(c1, Lane.TOPOLOGY, a).claim;
        claim = c1.advance(claim, "PROVISIONING", ImmutableMap.of("request", "r1")).claim;
        env2.catchUp();

        UUID b = cluster.newActionId();
        // not before expiry
        assertEquals(Status.LOST, c2.takeOver(Lane.TOPOLOGY, claim, b).status);
        cluster.now.set(claim.leaseExpiresAtMillis + 1);
        Outcome taken = c2.takeOver(Lane.TOPOLOGY, claim, b);
        assertTrue(taken.toString(), taken.ok());
        assertEquals(b, taken.claim.actionId);
        assertEquals(env2.self, taken.claim.owner);
        assertEquals(Claim.RECOVERING, taken.claim.phase);
        assertEquals("fake", taken.claim.actuator);
        assertEquals("CLUSTER", taken.claim.target);
        assertEquals(ImmutableMap.of("request", "r1"), taken.claim.state);
        assertEquals(0, taken.claim.step);

        // the old owner returns with a stale view: every commit is rejected and the ledger tells it why
        Outcome advance = c1.advance(claim, "JOINING", ImmutableMap.of());
        assertEquals(Status.LOST, advance.status);
        assertEquals(Status.OK, c1.release(claim, "COMMITTED").status);
        assertNull(c1.ownClaim(a));
        // its release did not touch the new owner's claim
        assertEquals(taken.claim, cluster.ledger().claims.get(Lane.TOPOLOGY));
        assertNull(cluster.ledger().recent(a));

        assertTrue(c2.release(taken.claim, "ROLLED_BACK").ok());
        assertEquals("ROLLED_BACK", cluster.ledger().recent(b).finalState);
    }

    @Test
    public void forcedRelease()
    {
        enable();
        UUID a = cluster.newActionId();
        Claim claim = claim(c1, Lane.SCHEMA_LAYOUT, a).claim;
        env2.catchUp();
        assertEquals(a, c2.forceRelease(Lane.SCHEMA_LAYOUT));
        assertTrue(cluster.ledger().claims.isEmpty());
        assertEquals(ClusterCoordinator.FORCE_RELEASED, cluster.ledger().recent(a).finalState);
        assertEquals(Status.LOST, c1.advance(claim, "P1", ImmutableMap.of()).status);
    }

    // ---- FC2 ----

    @Test
    public void commitInsteadOfAdvance()
    {
        enable();
        Claim claim = claim(c1, Lane.SCHEMA_LAYOUT, cluster.newActionId()).claim;
        // any transformation that advances the claim through the pure helper; here the stock one
        AdvanceAction own = new AdvanceAction(claim.actionId, claim.owner, claim.step, "DUAL_WRITE", ImmutableMap.of("layout", "1"), cluster.now.get(), env1.lease);
        Outcome outcome = c1.commitInstead(claim, own.wrap(), "DUAL_WRITE", ImmutableMap.of("layout", "1"));
        assertTrue(outcome.ok());
        assertEquals(1, outcome.claim.step);
        // a transformation that did not advance the claim as announced is not a success
        Outcome wrong = c1.commitInstead(outcome.claim, new SetClusterActionsEnabled(true).wrap(), "NEXT", ImmutableMap.of());
        assertEquals(Status.UNKNOWN, wrong.status);
    }

    // ---- the governor's gate ----

    @Test
    public void clusterGate()
    {
        LedgerClusterGate gate1 = new LedgerClusterGate(c1, name -> name.equals("layout") ? Lane.SCHEMA_LAYOUT : name.equals("repair") ? Lane.MAINTENANCE : null);
        LedgerClusterGate gate2 = new LedgerClusterGate(c2, name -> name.equals("layout") ? Lane.SCHEMA_LAYOUT : name.equals("repair") ? Lane.MAINTENANCE : null);
        Action layout = new Action(cluster.newActionId(), "layout", ActionScope.CLUSTER, Subject.cluster(), ImmutableMap.of(), null);
        Action repair = new Action(cluster.newActionId(), "repair", ActionScope.CLUSTER, Subject.cluster(), ImmutableMap.of(), null);
        Action node = new Action(cluster.newActionId(), "knob", ActionScope.CLUSTER, Subject.cluster(), ImmutableMap.of(), null);

        assertFalse(gate1.enabled());
        enable();
        assertTrue(gate1.enabled());
        assertFalse(gate1.nodeState(layout).isPresent());
        assertFalse(gate1.concurrency(layout).isPresent());
        assertTrue(gate1.nodeState(node).get().contains("not a cluster actuator"));
        assertTrue(gate2.nodeState(repair).get().contains("preferred proposer"));

        // FC3: the layout lane needs every member alive; other lanes do not
        env1.down.add(env2.self);
        assertTrue(gate1.nodeState(layout).get().contains("every member alive"));
        assertFalse(gate1.nodeState(repair).isPresent());
        env1.down.clear();

        // one cluster action per lane, also while its lease is expired: it must be recovered, not overwritten
        Claim claim = claim(c1, Lane.SCHEMA_LAYOUT, cluster.newActionId()).claim;
        assertTrue(gate1.concurrency(layout).get().contains("is held by"));
        assertFalse(gate1.concurrency(repair).isPresent());
        cluster.now.set(claim.leaseExpiresAtMillis + 10);
        assertTrue(gate1.concurrency(layout).get().contains("is held by"));
    }

    // ---- phase dispatch ----

    @Test
    public void phaseChangesAreDispatched()
    {
        List<String> seen = new ArrayList<>();
        c2.setPhaseListener((lane, claim) -> seen.add(lane + ":" + claim.phase + ":" + claim.step));
        ClusterMetadata before = env2.seen;
        enable();
        c2.notifyPostCommit(before, env2.seen, false);
        assertTrue(seen.isEmpty());

        Claim claim = claim(c1, Lane.TOPOLOGY, cluster.newActionId()).claim;
        c2.notifyPostCommit(env2.catchUp(), env2.seen, false);
        assertEquals(List.of("TOPOLOGY:CLAIMED:0"), seen);

        // a renewal is not a phase change
        claim = c1.renew(claim).claim;
        c2.notifyPostCommit(env2.catchUp(), env2.seen, false);
        assertEquals(1, seen.size());

        claim = c1.advance(claim, "P1", ImmutableMap.of()).claim;
        c2.notifyPostCommit(env2.catchUp(), env2.seen, false);
        assertEquals(List.of("TOPOLOGY:CLAIMED:0", "TOPOLOGY:P1:2"), seen);

        // unrelated metadata changes are ignored, as is a release
        c1.release(claim, "COMMITTED");
        c2.notifyPostCommit(env2.catchUp(), env2.seen, true);
        c2.notifyPostCommit(env2.seen, env2.seen, false);
        assertEquals(2, seen.size());

        // a listener that throws never reaches the metadata log thread
        c2.setPhaseListener((lane, c) -> { throw new IllegalStateException("boom"); });
        Claim next = claim(c1, Lane.TOPOLOGY, cluster.newActionId()).claim;
        assertNotNull(next);
        c2.notifyPostCommit(env2.catchUp(), env2.seen, false);
    }
}
