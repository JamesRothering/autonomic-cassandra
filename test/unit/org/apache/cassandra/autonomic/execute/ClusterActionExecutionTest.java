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
package org.apache.cassandra.autonomic.execute;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.ClusterLedgerValue;
import org.apache.cassandra.autonomic.coord.FakeCluster;
import org.apache.cassandra.autonomic.coord.FakeCluster.CommitMode;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.LedgerClusterGate;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.autonomic.safety.ClusterGate;
import org.apache.cassandra.autonomic.safety.DefaultSafetyGovernor;
import org.apache.cassandra.autonomic.safety.Reasons;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Cluster-scoped actions end to end below the metadata log: the real governor, executor, cluster execution,
 * coordinator and ledger transformations, over a fake commit function, clock, scheduler and cluster actuator.
 */
public class ClusterActionExecutionTest
{
    private static final String ACTUATOR = "fake_cluster";
    private static final VerifySpec SPEC = new VerifySpec(SignalId.of("goal"), Subject.node(), true, 0.10, List.of(), 10_000, 10_000);

    /** One node: everything a started layer creates, over the shared fake log. */
    private final class Peer
    {
        final FakeCluster.Node env;
        final ClusterCoordinator coordinator;
        final AutonomicConfig config = new AutonomicConfig();
        final InMemoryKnowledgeStore store = new InMemoryKnowledgeStore();
        final ActuatorRegistry registry = new ActuatorRegistry();
        final FakeClusterActuator actuator = new FakeClusterActuator(ACTUATOR);
        final InMemoryKnowledgeView view;
        final List<String> failures = new ArrayList<>();
        volatile String gate;
        volatile boolean killed;
        ManualScheduler scheduler;
        DefaultSafetyGovernor governor;
        ActionExecutor executor;
        ClusterActionExecution execution;

        Peer(int i)
        {
            env = cluster.node(i);
            coordinator = new ClusterCoordinator(env);
            view = new InMemoryKnowledgeView(new ObservationStore(100, 100), cluster.now::get);
            view.setLedgerSource(coordinator::ledger);
            config.dry_run = false;
            config.safety.cluster_approval = AutonomicConfig.SafetyConfig.APPROVAL_AUTO;
            config.safety.max_actions_per_hour = 100;
            config.safety.max_actions_per_actuator_per_hour = 100;
            config.safety.max_cluster_actions_per_hour = 100;
            config.safety.cooldown_after_commit = new org.apache.cassandra.config.DurationSpec.IntSecondsBound("0s");
            config.safety.cooldown_after_rollback = new org.apache.cassandra.config.DurationSpec.IntSecondsBound("0s");
            config.safety.breaker_rollbacks = 100;
            config.safety.quarantine_after_rollbacks = 100;
            registry.register(actuator);
            registry.setEnabled(ACTUATOR, true);
            start();
        }

        /** A new governor, executor and execution over the same store: what a restart of the layer creates. */
        void start()
        {
            scheduler = new ManualScheduler();
            ClusterGate clusterGate = new LedgerClusterGate(coordinator, name -> {
                Actuator a = registry.get(name);
                return a instanceof ClusterActuator ? ((ClusterActuator) a).lane() : null;
            });
            governor = new DefaultSafetyGovernor(new DefaultSafetyGovernor.Environment()
            {
                public AutonomicConfig config() { return config; }
                public long nowMillis() { return cluster.now.get(); }
                public String layerGate() { return gate; }
                public Actuator actuator(String name) { return registry.get(name); }
                public boolean actuatorEnabled(String name) { return registry.isEnabled(name, config); }
                public boolean nodeReady() { return true; }
                public boolean storeAvailable() { return store.isAvailable(); }
                public ClusterGate clusterGate() { return clusterGate; }
            }, new DefaultSafetyGovernor.Journal()
            {
                public void putState(String key, String value) { store.putState(key, value, cluster.now.get()); }
                public void deleteState(String key) { store.deleteState(key); }
                public void quarantined(String policy, long untilMillis) {}
                public void breakerTripped(int rollbacks, int window) {}
            });
            ActionExecutor.Host host = new ActionExecutor.Host()
            {
                public String gate() { return gate; }
                public boolean killed() { return killed; }
                public void actionFailed(ActionRecord record, String detail) { failures.add(record.id() + " " + detail); }
            };
            executor = new ActionExecutor(cluster.now::get, () -> config, governor, store, registry, scheduler, host, () -> view);
            execution = new ClusterActionExecution(cluster.now::get, () -> config, coordinator, registry, scheduler, store, host, executor::clusterReporter);
            executor.setClusterExecution(execution);
            coordinator.setPhaseListener(execution);
            execution.start();
        }

        Action action()
        {
            return new Action(cluster.newActionId(), ACTUATOR, ActionScope.CLUSTER, Subject.cluster(), ImmutableMap.of("why", "test"), SPEC);
        }

        ActionRecord propose(Action action)
        {
            return executor.propose(new Plan(UUID.randomUUID(), UUID.randomUUID(), "policy", List.of(action), "because"), action, view);
        }

        ActionRecord record(UUID id)
        {
            return executor.record(id).orElseThrow(() -> new AssertionError("no record of " + id));
        }

        /** Runs what is queued on the actuator thread. */
        void run()
        {
            scheduler.runAll();
        }

        /** Lets every pending delay elapse once (delayed steps and the periodic tick), then runs what that queued. */
        void elapse()
        {
            scheduler.fireTimers();
            scheduler.runAll();
        }

        /** Proposes an action and runs it until its claim is in the given phase. */
        Action startedIn(String phase)
        {
            Action a = action();
            assertEquals(ActionState.APPROVED, propose(a).state);
            run();
            for (int i = 0; i < 10 && !phase.equals(phaseOf(a)); i++)
                elapse();
            assertEquals(phase, phaseOf(a));
            assertEquals(ActionState.APPLYING, record(a.id).state);
            return a;
        }

        String phaseOf(Action a)
        {
            Claim claim = cluster.ledger().claim(a.id);
            return claim == null ? null : claim.phase;
        }

        void assertTerminal(UUID id, ActionState state, String reason)
        {
            ActionRecord r = record(id);
            assertEquals(state, r.state);
            assertEquals(reason, r.stateReason);
            assertEquals("the store has the terminal record", r, store.actions.get(id));
            assertFalse("inflight key is deleted", store.state.containsKey("inflight/" + id));
            assertTrue(executor.liveActions().isEmpty());
            assertEquals("the governor's reservation is released", 0, governor.reservations());
            assertFalse(execution.running().contains(id));
        }
    }

    private FakeCluster cluster;
    private Peer p1;
    private Peer p2;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Before
    public void setup()
    {
        cluster = new FakeCluster(2);
        p1 = new Peer(1);
        p2 = new Peer(2);
        p1.coordinator.setEnabled(true);
        p2.env.catchUp();
    }

    private Claim claim(Lane lane)
    {
        return cluster.ledger().claims.get(lane);
    }

    // ---- gate and proposal ----

    @Test
    public void nothingHappensBeforeClusterEnable()
    {
        cluster = new FakeCluster(2);
        p1 = new Peer(1);
        Action a = p1.action();
        ActionRecord r = p1.propose(a);
        assertEquals(ActionState.REJECTED, r.state);
        assertEquals(Reasons.CLUSTER_DISABLED, r.stateReason);
        p1.run();
        p1.elapse();
        p1.execution.tick();
        assertEquals(0, cluster.attempts);
        assertTrue(cluster.log.extensions.isEmpty());
        assertTrue(p1.actuator.calls.isEmpty());

        // even past the governor (a hand-over that was approved earlier), the execution itself refuses
        p1.execution.start(new ActionRecord(a, ActionState.APPROVED, cluster.now.get()), p1.executor.clusterReporter());
        assertEquals(0, cluster.attempts);
        assertEquals(Reasons.CLUSTER_DISABLED, p1.record(a.id).stateReason);
        assertFalse(p1.store.state.containsKey("inflight/" + a.id));
    }

    @Test
    public void onlyThePreferredProposerProposes()
    {
        Action a = p2.action();
        ActionRecord r = p2.propose(a);
        assertEquals(ActionState.REJECTED, r.state);
        assertEquals(Reasons.NODE_STATE, r.stateReason);
        assertEquals(1, cluster.attempts);
        // with node 1 down in its view, node 2 is preferred
        p2.env.down.add(p1.env.self);
        Action b = p2.action();
        assertEquals(ActionState.APPROVED, p2.propose(b).state);
        p2.run();
        assertEquals(p2.env.self, claim(Lane.MAINTENANCE).owner);
    }

    // ---- happy path ----

    @Test
    public void stepsAdvanceTheClaimAndCommit()
    {
        Action a = p1.action();
        assertEquals(ActionState.APPROVED, p1.propose(a).state);
        assertNull("nothing is claimed on the proposal side", claim(Lane.MAINTENANCE));

        p1.run();
        // claimed, first step ran and advanced to P1, second step advanced to P2 and asked for a delay
        Claim claim = claim(Lane.MAINTENANCE);
        assertEquals(a.id, claim.actionId);
        assertEquals(p1.env.self, claim.owner);
        assertEquals(ACTUATOR, claim.actuator);
        assertEquals("CLUSTER", claim.target);
        assertEquals("P2", claim.phase);
        assertEquals(2, claim.step);
        assertEquals(ImmutableMap.of("a", "1", "b", "2"), claim.state);
        assertEquals(ActionState.APPLYING, p1.record(a.id).state);
        assertTrue(p1.store.state.get("inflight/" + a.id).endsWith("|CLUSTER|policy|" + ACTUATOR + ":CLUSTER"));
        assertEquals(List.of("step:CLAIMED:0", "step:P1:1"), p1.actuator.calls);
        assertEquals(Set.of(a.id), p1.execution.running());
        // the governor sees the lane as busy, also for an action on another target
        Action other = new Action(cluster.newActionId(), ACTUATOR, ActionScope.CLUSTER, Subject.table("ks", "t"), ImmutableMap.of(), SPEC);
        assertEquals(Reasons.CONCURRENCY, p1.propose(other).stateReason);

        p1.elapse();
        assertEquals(List.of("step:CLAIMED:0", "step:P1:1", "step:P2:2"), p1.actuator.calls);
        p1.assertTerminal(a.id, ActionState.COMMITTED, null);
        assertEquals(Verdict.IMPROVED, p1.record(a.id).verdict);
        assertNull(claim(Lane.MAINTENANCE));
        assertEquals(ClusterActionExecution.FINAL_COMMITTED, cluster.ledger().recent(a.id).finalState);
        assertEquals(List.of("->PROPOSED", "PROPOSED>APPROVED", "APPROVED>APPLYING", "APPLYING>APPLYING", "APPLYING>APPLYING", "APPLYING>COMMITTED"),
                     p1.store.transitions(a.id));
        assertTrue(p1.failures.isEmpty());
        // claim, two advances, release: a handful of log entries per action
        assertEquals(1 + 4, cluster.applied.size());
        for (Transformation t : cluster.applied)
            assertTrue(t instanceof CustomTransformation);
    }

    @Test
    public void claimNotWonRejectsWithoutCallingTheActuator()
    {
        Action a = p1.action();
        assertEquals(ActionState.APPROVED, p1.propose(a).state);
        // the commit is lost: the ledger never shows the claim
        p1.env.mode = CommitMode.DROP;
        p1.run();
        p1.assertTerminal(a.id, ActionState.REJECTED, Reasons.CLAIM_REJECTED);
        assertTrue(p1.actuator.calls.isEmpty());
        assertNull(claim(Lane.MAINTENANCE));

        // lost to another node between approval and start
        p1.env.mode = CommitMode.APPLY;
        Action b = p1.action();
        assertEquals(ActionState.APPROVED, p1.propose(b).state);
        p2.env.down.add(p1.env.self);
        Action other = p2.action();
        assertEquals(ActionState.APPROVED, p2.propose(other).state);
        p2.run();
        assertEquals(p2.env.self, claim(Lane.MAINTENANCE).owner);
        p1.run();
        p1.assertTerminal(b.id, ActionState.REJECTED, Reasons.CLAIM_REJECTED);
        assertTrue(p1.actuator.calls.isEmpty());
        assertEquals(other.id, claim(Lane.MAINTENANCE).actionId);
    }

    // ---- lease ----

    @Test
    public void leaseIsRenewedEveryThird()
    {
        p1.actuator.onStep = claim -> StepResult.stay(1000);
        Action a = p1.startedIn("CLAIMED");
        long expiry = claim(Lane.MAINTENANCE).leaseExpiresAtMillis;
        int applied = cluster.applied.size();

        p1.execution.tick();
        assertEquals("not due yet", applied, cluster.applied.size());

        cluster.now.addAndGet(p1.env.lease / 3);
        p1.execution.tick();
        Claim renewed = claim(Lane.MAINTENANCE);
        assertEquals(cluster.now.get() + p1.env.lease, renewed.leaseExpiresAtMillis);
        assertTrue(renewed.leaseExpiresAtMillis > expiry);
        assertEquals(1, renewed.step);
        assertEquals("CLAIMED", renewed.phase);
        assertEquals(applied + 1, cluster.applied.size());

        // the next step gets the renewed claim, and the action is still running
        p1.elapse();
        assertTrue(p1.actuator.calls.contains("step:CLAIMED:1"));
        assertEquals(ActionState.APPLYING, p1.record(a.id).state);
    }

    @Test
    public void ownerStopsBeforeExpiryWhenItCannotCommit()
    {
        p1.actuator.onStep = claim -> StepResult.stay(1000);
        Action a = p1.startedIn("CLAIMED");
        Claim claim = claim(Lane.MAINTENANCE);
        // partitioned from the CMS: renewals fail
        p1.env.mode = CommitMode.DROP;
        cluster.now.addAndGet(p1.env.lease / 3);
        p1.execution.tick();
        assertEquals(ActionState.APPLYING, p1.record(a.id).state);
        assertEquals(claim, claim(Lane.MAINTENANCE));

        cluster.now.set(claim.leaseExpiresAtMillis - p1.env.margin - 1);
        p1.execution.tick();
        assertEquals("still inside the lease minus the margin", ActionState.APPLYING, p1.record(a.id).state);

        cluster.now.addAndGet(1);
        int calls = p1.actuator.calls.size();
        int attempts = cluster.attempts;
        p1.execution.tick();
        p1.assertTerminal(a.id, ActionState.FAILED, Reasons.CLAIM_LOST);
        assertEquals(1, p1.failures.size());
        // no further side effects: no actuator call, no commit, and the claim is left for whoever takes over
        p1.elapse();
        p1.elapse();
        assertEquals(calls, p1.actuator.calls.size());
        assertEquals(attempts, cluster.attempts);
        assertEquals(claim, claim(Lane.MAINTENANCE));
    }

    // ---- takeover ----

    @Test
    public void expiredClaimIsTakenOverAndRecovered()
    {
        p1.actuator.onStep = claim -> ClusterActionExecution.INITIAL_PHASE.equals(claim.phase)
                                      ? StepResult.advance("PROVISIONING", ImmutableMap.of("request", "r1"), 0)
                                      : StepResult.stay(1000);
        Action a = p1.startedIn("PROVISIONING");
        Claim old = claim(Lane.MAINTENANCE);
        p2.env.catchUp();

        // node 1 crashes: it stops renewing. Node 2 does nothing while the lease is valid...
        p2.env.down.add(p1.env.self);
        p2.execution.tick();
        p2.run();
        assertEquals(old, claim(Lane.MAINTENANCE));
        assertTrue(p2.actuator.calls.isEmpty());

        // ...and takes over once it has expired
        cluster.now.set(old.leaseExpiresAtMillis + 1);
        p2.execution.tick();
        Claim taken = claim(Lane.MAINTENANCE);
        assertEquals(p2.env.self, taken.owner);
        assertEquals(Claim.RECOVERING, taken.phase);
        assertEquals(ImmutableMap.of("request", "r1"), taken.state);
        assertFalse(taken.actionId.equals(a.id));
        ActionRecord recovering = p2.record(taken.actionId);
        assertEquals(ActionState.APPLYING, recovering.state);
        assertEquals(a.id.toString(), recovering.action.params.get("recovers"));
        assertEquals(ACTUATOR + ":CLUSTER", recovering.action.dedupeKey);
        assertTrue(p2.store.state.containsKey("inflight/" + taken.actionId));

        p2.run();
        assertEquals(List.of("recover:RECOVERING:0"), p2.actuator.calls);
        p2.assertTerminal(taken.actionId, ActionState.ROLLED_BACK, Reasons.RECOVERY);
        assertNull(claim(Lane.MAINTENANCE));
        assertEquals(ClusterActionExecution.FINAL_ROLLED_BACK, cluster.ledger().recent(taken.actionId).finalState);

        // the old owner returns: its claim is gone, it fails locally and does nothing else
        int calls = p1.actuator.calls.size();
        int applied = cluster.applied.size();
        p1.env.catchUp();
        p1.elapse();
        p1.assertTerminal(a.id, ActionState.FAILED, Reasons.CLAIM_LOST);
        assertEquals(calls, p1.actuator.calls.size());
        assertEquals(applied, cluster.applied.size());
        assertEquals(1, p1.failures.size());
    }

    @Test
    public void recoveryMayResumeForward()
    {
        p1.actuator.onStep = claim -> StepResult.stay(1000);
        p1.startedIn("CLAIMED");
        Claim old = claim(Lane.MAINTENANCE);
        p2.env.catchUp();
        p2.env.down.add(p1.env.self);
        p2.actuator.onRecover = claim -> Claim.RECOVERING.equals(claim.phase) ? StepResult.advance("CLEANUP", claim.state, 0)
                                                                              : StepResult.committed(null, "completed by the new owner");
        cluster.now.set(old.leaseExpiresAtMillis + 1);
        p2.execution.tick();
        UUID id = claim(Lane.MAINTENANCE).actionId;
        p2.run();
        assertEquals(List.of("recover:RECOVERING:0", "recover:CLEANUP:1"), p2.actuator.calls);
        p2.assertTerminal(id, ActionState.COMMITTED, null);
        assertEquals(ClusterActionExecution.FINAL_COMMITTED, cluster.ledger().recent(id).finalState);
    }

    @Test
    public void noTakeoverWhilePausedOrNotPreferred()
    {
        p1.actuator.onStep = claim -> StepResult.stay(1000);
        p1.startedIn("CLAIMED");
        Claim old = claim(Lane.MAINTENANCE);
        p2.env.catchUp();
        cluster.now.set(old.leaseExpiresAtMillis + 1);

        // node 2 still sees node 1 alive: node 1 is the preferred proposer
        p2.execution.tick();
        assertEquals(old, claim(Lane.MAINTENANCE));
        p2.env.down.add(p1.env.self);
        p2.gate = Reasons.PAUSED;
        p2.execution.tick();
        assertEquals(old, claim(Lane.MAINTENANCE));
        p2.gate = null;
        p2.registry.setEnabled(ACTUATOR, false);
        p2.execution.tick();
        assertEquals(old, claim(Lane.MAINTENANCE));
        p2.registry.setEnabled(ACTUATOR, true);
        p2.execution.tick();
        assertEquals(p2.env.self, claim(Lane.MAINTENANCE).owner);
    }

    @Test
    public void claimLostMidActionByForcedRelease()
    {
        Action a = p1.startedIn("P2");
        p2.env.catchUp();
        assertEquals(a.id, p2.coordinator.forceRelease(Lane.MAINTENANCE));
        int calls = p1.actuator.calls.size();
        int applied = cluster.applied.size();
        p1.env.catchUp();
        p1.elapse();
        p1.assertTerminal(a.id, ActionState.FAILED, Reasons.CLAIM_LOST);
        assertEquals("no step after the claim was lost", calls, p1.actuator.calls.size());
        assertEquals(applied, cluster.applied.size());
        assertTrue(p1.failures.get(0).contains(Reasons.CLAIM_LOST));
    }

    // ---- kill (FC5) ----

    @Test
    public void killBeforeTheCommitPointAborts()
    {
        Action a = p1.startedIn("P2");
        p1.actuator.onAbort = claim -> StepResult.rolledBack("undone");
        p1.killed = true;
        p1.executor.rollbackAll(Reasons.KILL);
        p1.run();
        assertEquals("abort:P2:2:killed", p1.actuator.calls.get(p1.actuator.calls.size() - 1));
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.KILL);
        assertNull(claim(Lane.MAINTENANCE));
        assertEquals(ClusterActionExecution.FINAL_ROLLED_BACK, cluster.ledger().recent(a.id).finalState);
        // no step after the kill
        p1.elapse();
        assertEquals(2, p1.actuator.count("step"));
    }

    @Test
    public void abortMayTakeSeveralSteps()
    {
        Action a = p1.startedIn("P2");
        p1.actuator.onAbort = claim -> "P2".equals(claim.phase) ? StepResult.advance("UNDOING", claim.state, 0) : StepResult.rolledBack("undone");
        p1.executor.rollbackAll(Reasons.SHUTDOWN);
        p1.run();
        assertEquals("no step after the stop", 2, p1.actuator.count("step"));
        assertTrue(p1.actuator.calls.contains("abort:P2:2:killed"));
        assertTrue(p1.actuator.calls.contains("abort:UNDOING:3:killed"));
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.SHUTDOWN);
    }

    @Test
    public void killPastTheCommitPointReleasesAsCleanupPending()
    {
        p1.actuator.phasesPastCommitPoint.add("P2");
        p1.actuator.onAbort = claim -> { throw new AssertionError("abort past the commit point"); };
        p1.actuator.onRecover = claim -> { throw new AssertionError("recover past the commit point"); };
        Action a = p1.startedIn("P2");
        int calls = p1.actuator.calls.size();
        p1.executor.rollbackAll(Reasons.KILL);
        p1.run();
        assertEquals(calls, p1.actuator.calls.size());
        p1.assertTerminal(a.id, ActionState.COMMITTED, Reasons.COMMITTED_CLEANUP_PENDING);
        assertNull(claim(Lane.MAINTENANCE));
        assertEquals(ClusterActionExecution.FINAL_COMMITTED_CLEANUP_PENDING, cluster.ledger().recent(a.id).finalState);
        assertTrue(p1.failures.isEmpty());
    }

    // ---- failing steps ----

    @Test
    public void actuatorMayEndAnActionAsFailedAndTheClaimIsReleased()
    {
        Action a = p1.startedIn("P2");
        p1.actuator.onStep = claim -> StepResult.failed("the join started and did not finish");
        p1.elapse();
        p1.assertTerminal(a.id, ActionState.FAILED, Reasons.ACTUATOR_FAILED);
        // released, unlike a claim that was lost or whose recovery keeps failing: nothing is left for a next owner
        assertNull(claim(Lane.MAINTENANCE));
        assertEquals(ClusterActionExecution.FINAL_FAILED, cluster.ledger().recent(a.id).finalState);
        assertEquals(1, p1.failures.size());
        assertTrue(p1.failures.get(0), p1.failures.get(0).contains("the join started and did not finish"));
        assertEquals(0, p1.actuator.count("abort"));
        assertEquals(0, p1.actuator.count("recover"));
    }

    @Test
    public void stepThatKeepsThrowingIsAborted()
    {
        Action a = p1.startedIn("P2");
        p1.actuator.failStep = new IllegalStateException("provider unavailable");
        p1.actuator.onAbort = claim -> StepResult.rolledBack("undone");
        for (int i = 0; i < 6; i++)
            p1.elapse();
        assertEquals("retried before giving up", 2 + ClusterActionExecution.MAX_STEP_FAILURES, p1.actuator.count("step"));
        assertEquals(1, p1.actuator.count("abort"));
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.APPLY_FAILED);
        assertEquals(ClusterActionExecution.FINAL_ROLLED_BACK, cluster.ledger().recent(a.id).finalState);
    }

    @Test
    public void recoveryThatKeepsThrowingFailsAndLeavesTheClaim()
    {
        Action a = p1.startedIn("P2");
        p1.actuator.failRecover = new IllegalStateException("cannot undo");
        p1.executor.rollbackAll(Reasons.KILL);
        p1.run();
        for (int i = 0; i < 6; i++)
            p1.elapse();
        assertEquals(ClusterActionExecution.MAX_STEP_FAILURES, p1.actuator.count("recover"));
        p1.assertTerminal(a.id, ActionState.FAILED, Reasons.RECOVERY_FAILED);
        assertEquals(1, p1.failures.size());
        // not released: only the next owner's recover can judge the state of the cluster
        assertEquals(a.id, claim(Lane.MAINTENANCE).actionId);
    }

    // ---- ambiguous commits ----

    @Test
    public void ambiguousAdvanceIsRetriedWithoutCallingTheActuatorAgain()
    {
        Action a = p1.action();
        assertEquals(ActionState.APPROVED, p1.propose(a).state);
        p1.actuator.onStep = claim -> {
            // the claim is in; from now on commits are applied but time out locally
            if (ClusterActionExecution.INITIAL_PHASE.equals(claim.phase))
                p1.env.mode = CommitMode.APPLY_UNSEEN;
            return FakeClusterActuator.defaultStep(claim);
        };
        p1.run();
        assertEquals(List.of("step:CLAIMED:0"), p1.actuator.calls);
        assertEquals("P1", claim(Lane.MAINTENANCE).phase);
        assertEquals(1, claim(Lane.MAINTENANCE).step);

        // retried while still unseen: idempotent, the step moves once, the actuator is not asked again
        p1.elapse();
        assertEquals(List.of("step:CLAIMED:0"), p1.actuator.calls);
        assertEquals(1, claim(Lane.MAINTENANCE).step);

        p1.env.mode = CommitMode.APPLY;
        p1.elapse();
        p1.elapse();
        p1.elapse();
        assertEquals(List.of("step:CLAIMED:0", "step:P1:1", "step:P2:2"), p1.actuator.calls);
        p1.assertTerminal(a.id, ActionState.COMMITTED, null);
    }

    @Test
    public void ambiguousReleaseIsRecognisedInTheLedger()
    {
        Action a = p1.startedIn("P2");
        p1.env.mode = CommitMode.APPLY_UNSEEN;
        p1.elapse();
        // released in the log, not yet seen by the node: the local action is not terminal yet
        assertNull(claim(Lane.MAINTENANCE));
        assertEquals(ActionState.APPLYING, p1.record(a.id).state);
        p1.env.mode = CommitMode.APPLY;
        p1.env.catchUp();
        p1.elapse();
        p1.assertTerminal(a.id, ActionState.COMMITTED, null);
        assertEquals(3, p1.actuator.count("step"));
        assertTrue(p1.failures.isEmpty());
    }

    // ---- FC2 ----

    /** A transformation of the actuator that advances its claim and writes its own extension in the same commit. */
    public static final class AdvanceWithOwnState implements Transformation
    {
        final Claim claim;
        final String phase;
        final long nowMillis;
        final long leaseMillis;

        AdvanceWithOwnState(Claim claim, String phase, long nowMillis, long leaseMillis)
        {
            this.claim = claim;
            this.phase = phase;
            this.nowMillis = nowMillis;
            this.leaseMillis = leaseMillis;
        }

        @Override
        public Kind kind()
        {
            return Kind.CUSTOM;
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            ClusterLedger.Result result = ClusterLedger.of(prev).withAdvancedClaim(claim.actionId, claim.owner, claim.step, phase, claim.state, nowMillis, leaseMillis);
            if (result.isRejected())
                return new Rejected(ExceptionCode.INVALID, result.rejection);
            return Transformation.success(prev.transformer()
                                              .with(ClusterLedgerValue.KEY, new ClusterLedgerValue(result.ledger))
                                              .with(CustomTransformation.PokeString.METADATA_KEY, org.apache.cassandra.tcm.extensions.StringValue.create("registry at " + phase)),
                                          LockedRanges.AffectedRanges.EMPTY);
        }
    }

    @Test
    public void stepMayCommitItsOwnTransformationInsteadOfAdvance()
    {
        p1.actuator.onStep = claim -> {
            switch (claim.phase)
            {
                case ClusterActionExecution.INITIAL_PHASE:
                    return StepResult.commit(new AdvanceWithOwnState(claim, "DUAL_WRITE", cluster.now.get(), p1.env.lease), "DUAL_WRITE", claim.state, 1000);
                default:
                    return StepResult.committed(Verdict.IMPROVED, null);
            }
        };
        Action a = p1.action();
        p1.propose(a);
        p1.run();
        Claim claim = claim(Lane.MAINTENANCE);
        assertEquals("DUAL_WRITE", claim.phase);
        assertEquals(1, claim.step);
        // claim and registry changed in ONE commit: same epoch, and no AdvanceAction was committed
        ClusterMetadata log = cluster.log;
        assertEquals("registry at DUAL_WRITE", log.extensions.get(CustomTransformation.PokeString.METADATA_KEY).getValue());
        assertEquals(log.epoch, log.extensions.get(ClusterLedgerValue.KEY).lastModified());
        assertEquals(log.epoch, log.extensions.get(CustomTransformation.PokeString.METADATA_KEY).lastModified());
        assertTrue(cluster.applied.get(cluster.applied.size() - 1) instanceof AdvanceWithOwnState);
        assertEquals(1 + 2, cluster.applied.size());

        p1.elapse();
        p1.assertTerminal(a.id, ActionState.COMMITTED, null);
    }

    @Test
    public void ownTransformationThatWasNotAppliedIsRebuiltByTheActuator()
    {
        // dropped commit: the actuator is asked again with the current claim, because its transformation embeds a step
        p1.actuator.onStep = claim -> {
            if (!ClusterActionExecution.INITIAL_PHASE.equals(claim.phase))
                return StepResult.committed(null, null);
            StepResult result = StepResult.commit(new AdvanceWithOwnState(claim, "DUAL_WRITE", cluster.now.get(), p1.env.lease), "DUAL_WRITE", claim.state, 0);
            p1.env.mode = p1.actuator.count("step") == 1 ? CommitMode.DROP : CommitMode.APPLY;
            return result;
        };
        Action b = p1.action();
        p1.propose(b);
        p1.run();
        assertEquals("CLAIMED", claim(Lane.MAINTENANCE).phase);
        p1.elapse();
        p1.elapse();
        assertEquals(2, p1.actuator.count("step:CLAIMED"));
        p1.assertTerminal(b.id, ActionState.COMMITTED, null);
    }

    // ---- restart (overview 7.6 step 3, FC5) ----

    @Test
    public void restartWithOwnClaimRecovers()
    {
        Action a = p1.startedIn("P2");
        // the process dies; a new layer starts over the same store and ledger
        p1.start();
        ActionExecutor.Recovery recovery = p1.executor.recover(Set.of());
        assertTrue(recovery.complete);
        assertTrue("recovery never runs a step on the starting thread", p1.actuator.count("recover") == 0);
        p1.run();
        assertEquals(1, p1.actuator.count("recover"));
        assertEquals("recover:P2:2", p1.actuator.calls.get(p1.actuator.calls.size() - 1));
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.RESTART);
        assertNull(claim(Lane.MAINTENANCE));
        assertEquals(ClusterActionExecution.FINAL_ROLLED_BACK, cluster.ledger().recent(a.id).finalState);
        // a second recovery pass finds nothing
        assertTrue(p1.executor.recover(Set.of()).complete);
        p1.run();
        assertEquals(1, p1.actuator.count("recover"));
    }

    @Test
    public void restartPastTheCommitPointReleasesAsCleanupPending()
    {
        p1.actuator.phasesPastCommitPoint.add("P2");
        Action a = p1.startedIn("P2");
        p1.actuator.onRecover = claim -> { throw new AssertionError("recover past the commit point"); };
        p1.start();
        p1.executor.recover(Set.of());
        p1.run();
        assertEquals(0, p1.actuator.count("recover"));
        p1.assertTerminal(a.id, ActionState.COMMITTED, Reasons.COMMITTED_CLEANUP_PENDING);
        assertEquals(ClusterActionExecution.FINAL_COMMITTED_CLEANUP_PENDING, cluster.ledger().recent(a.id).finalState);
    }

    @Test
    public void restartWithoutClaimFailsAsClaimLost()
    {
        Action a = p1.startedIn("P2");
        p2.env.catchUp();
        p2.coordinator.forceRelease(Lane.MAINTENANCE);
        p1.env.catchUp();
        int applied = cluster.applied.size();
        p1.start();
        p1.executor.recover(Set.of());
        p1.run();
        p1.assertTerminal(a.id, ActionState.FAILED, Reasons.CLAIM_LOST);
        assertEquals(0, p1.actuator.count("recover"));
        assertEquals(applied, cluster.applied.size());
        assertEquals(1, p1.failures.size());
    }

    @Test
    public void restartWithExpiredClaimFailsAndIsTakenOver()
    {
        Action a = p1.startedIn("P2");
        Claim old = claim(Lane.MAINTENANCE);
        cluster.now.set(old.leaseExpiresAtMillis - p1.env.margin);
        p1.start();
        p1.executor.recover(Set.of());
        p1.run();
        p1.assertTerminal(a.id, ActionState.FAILED, Reasons.CLAIM_LOST);
        assertEquals(0, p1.actuator.count("recover"));
        assertEquals(old, claim(Lane.MAINTENANCE));

        // after expiry the preferred proposer, here the same node, takes it over like any other expired claim
        cluster.now.set(old.leaseExpiresAtMillis + 1);
        p1.execution.tick();
        p1.run();
        assertEquals(List.of("recover:RECOVERING:0"), p1.actuator.calls.subList(p1.actuator.calls.size() - 1, p1.actuator.calls.size()));
        assertNull(claim(Lane.MAINTENANCE));
    }

    @Test
    public void restartFindsItsOwnReleaseInTheLedger()
    {
        Action a = p1.startedIn("P2");
        // the release reaches the log, the process dies before the local record is written
        Claim claim = claim(Lane.MAINTENANCE);
        assertTrue(p1.coordinator.release(claim, ClusterActionExecution.FINAL_COMMITTED).ok());
        p1.start();
        p1.executor.recover(Set.of());
        p1.run();
        p1.assertTerminal(a.id, ActionState.COMMITTED, null);
        assertTrue(p1.failures.isEmpty());
    }

    // ---- onPhase ----

    @Test
    public void phaseChangesReachEveryNodeOnItsActuatorThread()
    {
        Action a = p1.action();
        ClusterMetadata before = p2.env.seen;
        p1.propose(a);
        p1.run();
        // the log follower of node 2 enacts what node 1 committed
        p2.coordinator.notifyPostCommit(before, p2.env.catchUp() == before ? p2.env.seen : p2.env.seen, false);
        assertTrue("only handed over, never run on the log thread", p2.actuator.calls.isEmpty());
        p2.run();
        assertEquals(List.of("onPhase:P2:2"), p2.actuator.calls);
        assertNotNull(a);
    }
}
