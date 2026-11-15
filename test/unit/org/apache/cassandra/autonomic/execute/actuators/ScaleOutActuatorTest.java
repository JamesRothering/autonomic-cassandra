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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.FakeCluster;
import org.apache.cassandra.autonomic.coord.FakeCluster.CommitMode;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.LedgerClusterGate;
import org.apache.cassandra.autonomic.elastic.FakeScaleOutProvider;
import org.apache.cassandra.autonomic.elastic.NoopProvider;
import org.apache.cassandra.autonomic.elastic.ProviderCalls;
import org.apache.cassandra.autonomic.elastic.ProviderException;
import org.apache.cassandra.autonomic.elastic.ProvisionStatus;
import org.apache.cassandra.autonomic.elastic.ScaleOutProvider;
import org.apache.cassandra.autonomic.elastic.ScaleOutProvider.Capabilities;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.ActionExecutor;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.ActuatorRegistry;
import org.apache.cassandra.autonomic.execute.ClusterActionExecution;
import org.apache.cassandra.autonomic.execute.ClusterActuator;
import org.apache.cassandra.autonomic.execute.ManualScheduler;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.autonomic.safety.ClusterGate;
import org.apache.cassandra.autonomic.safety.DefaultSafetyGovernor;
import org.apache.cassandra.autonomic.safety.Reasons;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.MembershipUtils;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;

import static org.apache.cassandra.autonomic.execute.actuators.ScaleOutActuator.CANCELLING;
import static org.apache.cassandra.autonomic.execute.actuators.ScaleOutActuator.JOINING;
import static org.apache.cassandra.autonomic.execute.actuators.ScaleOutActuator.PROVISIONING;
import static org.apache.cassandra.autonomic.execute.actuators.ScaleOutActuator.REQUESTED;
import static org.apache.cassandra.autonomic.execute.actuators.ScaleOutActuator.VERIFYING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Scale-out end to end below the metadata log: the real governor, executor, cluster execution, coordinator, ledger
 * transformations and {@link ScaleOutActuator}, over a fake commit function, clock and scheduler, a hand-built
 * directory and a fake provider that all nodes share, as they would share the real one.
 *
 * <p>There is no shared contract class for cluster actuators: {@code AbstractActuatorContractTest} is the
 * apply/rollback contract of node actuators, and {@code scale_out} is listed as irreversible in
 * {@code ActuatorContractCoverageTest} like {@code repair_priority}. Its contract is the phase table, tested here.
 */
public class ScaleOutActuatorTest
{
    private static final VerifySpec SPEC = new VerifySpec(Signals.DISK_USED_FRACTION, Subject.node(), true, 0.05, List.of(), 120_000, 300_000);
    /** Marker phase of {@link Peer#reach}: JOINING with the join seen in the metadata. */
    private static final String JOIN_STARTED = "JOINING*";
    private static final int NEW_NODE = 3;
    private static final long CHUNK = 120_000;

    /** One node: everything a started layer creates, over the shared fake log and the shared provider. */
    private final class Peer
    {
        final FakeCluster.Node env;
        final ClusterCoordinator coordinator;
        final AutonomicConfig config = new AutonomicConfig();
        final InMemoryKnowledgeStore store = new InMemoryKnowledgeStore();
        final ActuatorRegistry registry = new ActuatorRegistry();
        final InMemoryKnowledgeView view;
        final ScaleOutActuator actuator;
        final List<String> failures = new ArrayList<>();
        volatile String gate;
        volatile boolean killed;
        ManualScheduler scheduler;
        DefaultSafetyGovernor governor;
        ActionExecutor executor;
        ClusterActionExecution execution;

        Peer(int i, ScaleOutProvider provider)
        {
            env = cluster.node(i);
            coordinator = new ClusterCoordinator(env);
            view = new InMemoryKnowledgeView(new ObservationStore(1000, 100), cluster.now::get);
            view.setLedgerSource(coordinator::ledger);
            // cluster_approval is left at its default
            config.dry_run = false;
            config.safety.max_actions_per_hour = 100;
            config.safety.max_actions_per_actuator_per_hour = 100;
            config.safety.max_cluster_actions_per_hour = 100;
            config.safety.cooldown_after_commit = new DurationSpec.IntSecondsBound("0s");
            config.safety.cooldown_after_rollback = new DurationSpec.IntSecondsBound("0s");
            actuator = new ScaleOutActuator(provider, new ScaleOutActuator.Cluster()
            {
                public ClusterMetadata metadata() { return cluster.log; }
                public Location location() { return new Location("dc1", "rack1"); }
            }, () -> config.scale_out, () -> view, new ProviderCalls((task, delay) -> () -> {}));
            registry.register(actuator);
            registry.setEnabled(ScaleOutActuator.NAME, true);
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
                public void actionFailed(ActionRecord record, String detail) { failures.add(detail); }
            };
            executor = new ActionExecutor(cluster.now::get, () -> config, governor, store, registry, scheduler, host, () -> view);
            execution = new ClusterActionExecution(cluster.now::get, () -> config, coordinator, registry, scheduler, store, host, executor::clusterReporter);
            executor.setClusterExecution(execution);
            coordinator.setPhaseListener(execution);
            execution.start();
        }

        Action action()
        {
            return new Action(cluster.newActionId(), ScaleOutActuator.NAME, ActionScope.CLUSTER, Subject.cluster(),
                              ScaleOutActuator.params("dc1", "rack1", "disks fill up"), SPEC);
        }

        ActionRecord propose(Action action)
        {
            return executor.propose(new Plan(UUID.randomUUID(), UUID.randomUUID(), "scale_out", List.of(action), "because"), action, view);
        }

        /** Proposes, approves as an operator and runs the first steps. */
        Action approved()
        {
            Action a = action();
            assertEquals(ActionState.PROPOSED, propose(a).state);
            assertEquals(ActionState.APPROVED, executor.approve(a.id).state);
            run();
            return a;
        }

        ActionRecord record(UUID id)
        {
            return executor.record(id).orElseThrow(() -> new AssertionError("no record of " + id));
        }

        void run()
        {
            scheduler.runAll();
        }

        /** Lets every pending delay elapse once (delayed steps and the periodic tick), then runs what that queued. */
        void elapse()
        {
            executor.onControlTick(view);
            scheduler.fireTimers();
            scheduler.runAll();
        }

        /** Lets time pass in steps short enough for the lease to be renewed. */
        void pass(long millis)
        {
            for (long done = 0; done < millis; done += CHUNK)
            {
                cluster.now.addAndGet(Math.min(CHUNK, millis - done));
                elapse();
            }
        }

        /** A scale-out of this node, driven until its claim is in the given phase. */
        Action reach(String phase)
        {
            if (REQUESTED.equals(phase))
                provider.failRequest = new ProviderException("connection reset");
            Action a = approved();
            if (REQUESTED.equals(phase))
                return in(a, REQUESTED);
            in(a, PROVISIONING);
            if (PROVISIONING.equals(phase))
                return a;
            provider.statuses.put("t1", ProvisionStatus.started(address(NEW_NODE)));
            elapse();
            in(a, JOINING);
            if (JOINING.equals(phase))
                return a;
            register(NEW_NODE);
            state(NEW_NODE, NodeState.BOOTSTRAPPING);
            elapse();
            in(a, JOINING);
            assertNotNull(claim().state.get(ScaleOutActuator.S_JOINING));
            if (JOIN_STARTED.equals(phase))
                return a;
            state(NEW_NODE, NodeState.JOINED);
            elapse();
            return in(a, VERIFYING);
        }

        Action in(Action a, String phase)
        {
            assertEquals(a.id, claim().actionId);
            assertEquals(phase, claim().phase);
            assertEquals(ActionState.APPLYING, record(a.id).state);
            return a;
        }

        void assertTerminal(UUID id, ActionState state, String reason)
        {
            ActionRecord r = record(id);
            assertEquals(String.valueOf(r), state, r.state);
            assertEquals(reason, r.stateReason);
            assertEquals("the store has the terminal record", r, store.actions.get(id));
            assertFalse("inflight key is deleted", store.state.containsKey("inflight/" + id));
            assertTrue(executor.liveActions().isEmpty());
            assertEquals("the governor's reservation is released", 0, governor.reservations());
            assertFalse(execution.running().contains(id));
            assertNull("the lane is free", claim());
        }

        /** Details of the audit trail of the action, in order. */
        String audit(UUID id)
        {
            StringBuilder sb = new StringBuilder();
            store.actionEvents(id).forEach(e -> sb.append(e.detail).append('\n'));
            return sb.toString();
        }

        /** Lets the verify window end with the disk at the given usage for its last two minutes. */
        void verifyWith(double usedFraction)
        {
            pass(config.scale_out.verify_window.toMilliseconds() - CHUNK - (cluster.now.get() - Long.parseLong(claim().state.get(ScaleOutActuator.S_JOINED_AT))));
            for (int i = 0; i < 4; i++)
            {
                view.store().accept(new Observation(Signals.DISK_USED_FRACTION, Subject.node(), cluster.now.get(), usedFraction));
                cluster.now.addAndGet(CHUNK / 4);
            }
            elapse();
        }

        void kill()
        {
            killed = true;
            executor.rollbackAll(Reasons.KILL);
            run();
            elapse();
        }
    }

    private FakeCluster cluster;
    private FakeScaleOutProvider provider;
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
        setup(new FakeScaleOutProvider());
    }

    private void setup(ScaleOutProvider with)
    {
        cluster = new FakeCluster(2);
        provider = with instanceof FakeScaleOutProvider ? (FakeScaleOutProvider) with : null;
        p1 = new Peer(1, with);
        p2 = new Peer(2, with);
        p1.coordinator.setEnabled(true);
        p2.env.catchUp();
    }

    private Claim claim()
    {
        return cluster.ledger().claims.get(Lane.TOPOLOGY);
    }

    private String finalState(UUID id)
    {
        return cluster.ledger().recent(id).finalState;
    }

    private static String address(int i)
    {
        return MembershipUtils.endpoint(i).getHostAddressAndPort();
    }

    private NodeId nodeId(int i)
    {
        return cluster.log.directory.peerId(MembershipUtils.endpoint(i));
    }

    /** A process started and registered itself: in the directory, not joining yet. */
    private void register(int i)
    {
        cluster.log = cluster.log.transformer().register(new NodeAddresses(MembershipUtils.endpoint(i)), new Location("dc1", "rack1"), NodeVersion.CURRENT)
                                 .build().metadata;
    }

    /** A bootstrapping node is in the directory with its join as a sequence in progress, as the real join is. */
    private void state(int i, NodeState state)
    {
        NodeId id = nodeId(i);
        ClusterMetadata.Transformer t = cluster.log.transformer().withNodeState(id, state);
        boolean joining = cluster.log.inProgressSequences.contains(id);
        if (state == NodeState.BOOTSTRAPPING && !joining)
            t = t.with(cluster.log.inProgressSequences.with(id, Mockito.mock(MultiStepOperation.class)));
        else if (state != NodeState.BOOTSTRAPPING && joining)
            t = t.with(cluster.log.inProgressSequences.without(id));
        cluster.log = t.build().metadata;
    }

    private void disk(Peer p, double usedFraction)
    {
        p.view.store().accept(new Observation(Signals.DISK_USED_FRACTION, Subject.node(), cluster.now.get(), usedFraction));
    }

    /** Node 1 stops for good with its lease running out; node 2 takes the claim over. Returns the new claim. */
    private Claim ownerLost()
    {
        Claim old = claim();
        p2.env.catchUp();
        p2.env.down.add(p1.env.self);
        p2.execution.tick();
        assertEquals("no takeover inside the lease", old, claim());
        cluster.now.set(old.leaseExpiresAtMillis + 1);
        p2.execution.tick();
        Claim taken = claim();
        assertEquals(p2.env.self, taken.owner);
        assertEquals(Claim.RECOVERING, taken.phase);
        assertNotEquals(old.actionId, taken.actionId);
        return taken;
    }

    private void assertOneNodeAtMost(Action a)
    {
        assertTrue("one node at most: " + provider.tickets, provider.tickets.size() <= 1);
        assertTrue("only ever asked for the proposed action: " + provider.requestIds(), Set.of(a.id.toString()).containsAll(provider.requestIds()));
    }

    // ---- contract ----

    @Test
    public void isATopologyLaneClusterActuatorThatIsIrreversible()
    {
        assertEquals(Lane.TOPOLOGY, p1.actuator.lane());
        assertEquals(ActionScope.CLUSTER, p1.actuator.scope());
        assertFalse(p1.actuator.reversible());
        assertEquals(ScaleOutActuator.NAME, p1.actuator.name());
        Action a = p1.action();
        assertFalse("a restarted owner resumes its join; abort decides at a kill", p1.actuator.pastCommitPoint(new Claim(a.id, a.actuator, "CLUSTER", new NodeId(1), VERIFYING, 5, 0, 1, ImmutableMap.of())));
        try
        {
            p1.actuator.apply(a, null);
            fail();
        }
        catch (UnsupportedOperationException expected)
        {
        }
    }

    // ---- approval and gates ----

    @Test
    public void manualApprovalIsTheDefaultAndNothingIsRequestedWithoutIt()
    {
        assertEquals(AutonomicConfig.SafetyConfig.APPROVAL_MANUAL, new AutonomicConfig().safety.cluster_approval);
        int commits = cluster.attempts;
        Action a = p1.action();
        ActionRecord r = p1.propose(a);
        assertEquals(ActionState.PROPOSED, r.state);
        p1.run();
        p1.elapse();
        assertTrue(provider.calls.isEmpty());
        assertNull(claim());
        assertEquals("nothing was committed", commits, cluster.attempts);

        // the operator rejects: still nothing
        assertEquals(ActionState.REJECTED, p1.executor.reject(a.id).state);
        p1.elapse();
        assertTrue(provider.calls.isEmpty());

        // approved, it starts
        Action b = p1.approved();
        p1.in(b, PROVISIONING);
        assertEquals(List.of("request:" + b.id, "status:t1"), provider.calls);
    }

    @Test
    public void theGatesOfClusterActionsApply()
    {
        // not before `cluster enable`
        cluster = new FakeCluster(2);
        p1 = new Peer(1, provider);
        assertEquals(Reasons.CLUSTER_DISABLED, p1.propose(p1.action()).stateReason);
        assertEquals(0, cluster.attempts);

        setup();
        p1.registry.setEnabled(ScaleOutActuator.NAME, false);
        assertEquals(Reasons.ACTUATOR_DISABLED, p1.propose(p1.action()).stateReason);
        p1.registry.setEnabled(ScaleOutActuator.NAME, true);
        p1.config.dry_run = true;
        assertEquals(Reasons.DRY_RUN, p1.propose(p1.action()).stateReason);
        p1.config.dry_run = false;
        p1.gate = Reasons.PAUSED;
        assertEquals(Reasons.PAUSED, p1.propose(p1.action()).stateReason);
        p1.gate = null;
        // only the preferred proposer, and only with every member alive (overview 7.1 check 5)
        assertEquals(Reasons.NODE_STATE, p2.propose(p2.action()).stateReason);
        p1.env.down.add(p2.env.self);
        assertEquals(Reasons.NODE_STATE, p1.propose(p1.action()).stateReason);
        p1.env.down.clear();
        assertTrue(provider.calls.isEmpty());
        assertNull(claim());
        assertEquals(ActionState.PROPOSED, p1.propose(p1.action()).state);
    }

    @Test
    public void oneScaleOutAtATime()
    {
        Action a = p1.reach(PROVISIONING);
        ActionRecord second = p1.propose(p1.action());
        assertEquals(ActionState.REJECTED, second.state);
        assertTrue(second.stateReason, Set.of(Reasons.CONFLICT, Reasons.CONCURRENCY).contains(second.stateReason));
        // the lane, not only this node's bookkeeping: a restarted layer knows nothing of the action and is refused too
        p1.start();
        ActionRecord third = p1.propose(p1.action());
        assertEquals(ActionState.REJECTED, third.state);
        assertEquals(Reasons.CONCURRENCY, third.stateReason);
        assertOneNodeAtMost(a);
        assertEquals(1, provider.count("request"));
    }

    @Test
    public void providerCapabilitiesBoundTheProposal()
    {
        FakeScaleOutProvider full = new FakeScaleOutProvider();
        full.capabilities = new Capabilities(true, false, 1, 2, 1000);
        setup(full);
        ActionRecord r = p1.propose(p1.action());
        assertEquals(ActionState.REJECTED, r.state);
        assertEquals(Reasons.VALIDATION, r.stateReason);
        assertTrue(p1.actuator.unavailable().get().contains("2 members"));

        FakeScaleOutProvider cannot = new FakeScaleOutProvider();
        cannot.capabilities = new Capabilities(false, false, 1, 10, 1000);
        setup(cannot);
        assertEquals(Reasons.VALIDATION, p1.propose(p1.action()).stateReason);
        assertTrue(cannot.calls.isEmpty() && full.calls.isEmpty());

        // the limit is checked again under the claim: the cluster grew between approval and the first step
        FakeScaleOutProvider three = new FakeScaleOutProvider();
        three.capabilities = new Capabilities(true, false, 1, 3, 1000);
        setup(three);
        Action a = p1.action();
        assertEquals(ActionState.PROPOSED, p1.propose(a).state);
        assertEquals(ActionState.APPROVED, p1.executor.approve(a.id).state);
        register(NEW_NODE);
        p1.run();
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertTrue(three.calls.isEmpty());
    }

    @Test
    public void withTheNoopProviderAProposalIsRejectedAndAStepRollsBackCleanly()
    {
        setup(new NoopProvider());
        Action a = p1.action();
        ActionRecord r = p1.propose(a);
        assertEquals(ActionState.REJECTED, r.state);
        assertEquals(Reasons.VALIDATION, r.stateReason);
        assertNull(claim());
        // even under a claim (a provider reconfigured between proposal and step) nothing is requested
        Claim claim = new Claim(a.id, a.actuator, "CLUSTER", p1.env.self, ClusterActionExecution.INITIAL_PHASE, 0, 0, 1, ImmutableMap.of());
        StepResult result = p1.actuator.step(a, claim, context());
        assertEquals(StepResult.Kind.ROLLED_BACK, result.kind);
        assertTrue(result.detail, result.detail.contains("cannot add nodes"));
    }

    private ActionContext context()
    {
        return new ActionContext()
        {
            public long stepDeadlineMillis() { return cluster.now.get() + 30_000; }
            public long nowMillis() { return cluster.now.get(); }
            public boolean killed() { return false; }
            public void recordUndo(Map<String, String> undo) {}
        };
    }

    // ---- phases ----

    @Test
    public void happyPathGoesThroughEveryPhaseAndCommitsWithItsVerdict()
    {
        for (int i = 0; i < 4; i++)
        {
            disk(p1, 0.80);
            cluster.now.addAndGet(20_000);
        }
        Action a = p1.approved();
        // CLAIMED wrote the request ahead, REQUESTED asked the provider once, PROVISIONING holds the ticket
        p1.in(a, PROVISIONING);
        Map<String, String> state = claim().state;
        assertEquals(a.id.toString(), state.get(ScaleOutActuator.S_REQUEST));
        assertEquals("t1", state.get(ScaleOutActuator.S_TICKET));
        assertEquals("z1", state.get(ScaleOutActuator.TICKET_ATTRIBUTE + "zone"));
        assertEquals(0.80, Double.parseDouble(state.get(ScaleOutActuator.S_BASELINE)), 1e-9);
        assertEquals(List.of("request:" + a.id, "status:t1"), provider.calls);

        p1.elapse();
        p1.in(a, PROVISIONING);
        assertEquals("polled again", 2, provider.count("status"));

        provider.statuses.put("t1", ProvisionStatus.started(address(NEW_NODE)));
        p1.elapse();
        p1.in(a, JOINING);
        assertEquals(address(NEW_NODE), claim().state.get(ScaleOutActuator.S_ADDRESS));

        // the process is up and registered: not a join yet
        register(NEW_NODE);
        p1.elapse();
        p1.in(a, JOINING);
        assertNull(claim().state.get(ScaleOutActuator.S_JOINING));

        state(NEW_NODE, NodeState.BOOTSTRAPPING);
        p1.elapse();
        p1.in(a, JOINING);
        assertEquals(String.valueOf(nodeId(NEW_NODE).id()), claim().state.get(ScaleOutActuator.S_JOINING));

        state(NEW_NODE, NodeState.JOINED);
        p1.elapse();
        p1.in(a, VERIFYING);

        // the verify window is an hour: far beyond any dead-man deadline of node actions, and the action just waits
        p1.pass(p1.config.scale_out.verify_window.toMilliseconds() / 2);
        p1.in(a, VERIFYING);
        p1.verifyWith(0.60);
        p1.assertTerminal(a.id, ActionState.COMMITTED, null);
        assertEquals(Verdict.IMPROVED, p1.record(a.id).verdict);
        assertEquals(ClusterActionExecution.FINAL_COMMITTED, finalState(a.id));
        assertEquals(1, provider.count("request"));
        assertEquals(0, provider.count("cancel"));
        assertTrue(p1.failures.isEmpty());
        String audit = p1.audit(a.id);
        for (String phase : List.of(REQUESTED, PROVISIONING, JOINING, VERIFYING))
            assertTrue(phase + " in " + audit, audit.contains("phase " + phase));
    }

    @Test
    public void theVerdictIsRecordedAndANodeThatJoinedIsCommittedWhateverItIs()
    {
        for (int i = 0; i < 4; i++)
        {
            disk(p1, 0.50);
            cluster.now.addAndGet(20_000);
        }
        Action a = p1.reach(VERIFYING);
        p1.verifyWith(0.90);
        p1.assertTerminal(a.id, ActionState.COMMITTED, null);
        assertEquals(Verdict.REGRESSED, p1.record(a.id).verdict);
        assertEquals(NodeState.JOINED, cluster.log.directory.peerState(nodeId(NEW_NODE)));
        assertEquals(0, provider.count("cancel"));

        // without a baseline the verdict is INCONCLUSIVE, and it is still a commit
        setup();
        Action b = p1.reach(VERIFYING);
        p1.pass(p1.config.scale_out.verify_window.toMilliseconds() + CHUNK);
        p1.assertTerminal(b.id, ActionState.COMMITTED, null);
        assertEquals(Verdict.INCONCLUSIVE, p1.record(b.id).verdict);
    }

    // ---- provider failures and timeouts before the join ----

    @Test
    public void aRejectedRequestRollsBackWithNothingToCancel()
    {
        provider.failRequest = ProviderException.rejected("quota exceeded");
        Action a = p1.approved();
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertEquals(ClusterActionExecution.FINAL_ROLLED_BACK, finalState(a.id));
        assertEquals(List.of("request:" + a.id), provider.calls);
        assertTrue(provider.tickets.isEmpty());
    }

    @Test
    public void aRequestWithUnknownOutcomeIsRepeatedWithTheSameIdAndProvisionsOnce()
    {
        // the provider accepts, the answer is lost
        provider.acceptBeforeFailing = true;
        Action a = p1.reach(REQUESTED);
        p1.elapse();
        p1.elapse();
        p1.in(a, REQUESTED);
        assertEquals(3, provider.count("request"));
        provider.failRequest = null;
        p1.elapse();
        p1.in(a, PROVISIONING);
        assertEquals("t1", claim().state.get(ScaleOutActuator.S_TICKET));
        assertOneNodeAtMost(a);
        assertEquals(1, provider.tickets.size());
    }

    @Test
    public void aRequestNeverConfirmedTimesOutAndWhatTheProviderHoldsIsCancelled()
    {
        provider.acceptBeforeFailing = true;
        Action a = p1.reach(REQUESTED);
        p1.pass(p1.config.scale_out.provision_timeout.toMilliseconds() + CHUNK);
        p1.in(a, CANCELLING);
        assertNull("no ticket was ever stored", claim().state.get(ScaleOutActuator.S_TICKET));
        assertEquals(0, provider.count("cancel"));
        // the provider answers again: the repeat of the request yields the ticket, which is cancelled
        provider.failRequest = null;
        p1.elapse();
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertEquals(Set.of("t1"), provider.cancelled);
        assertEquals(0, provider.liveNodes());
        assertOneNodeAtMost(a);
    }

    @Test
    public void aCancelThatKeepsFailingEndsFailedForTheOperator()
    {
        Action a = p1.reach(PROVISIONING);
        provider.statuses.put("t1", ProvisionStatus.failed("no capacity in zone"));
        provider.failCancel = new ProviderException("503");
        p1.elapse();
        p1.in(a, CANCELLING);
        p1.pass(p1.config.scale_out.provision_timeout.toMilliseconds() - CHUNK);
        p1.in(a, CANCELLING);
        assertTrue(provider.count("cancel") > 2);
        p1.pass(3 * CHUNK);
        p1.assertTerminal(a.id, ActionState.FAILED, Reasons.ACTUATOR_FAILED);
        assertEquals(ClusterActionExecution.FINAL_FAILED, finalState(a.id));
        assertEquals(1, p1.failures.size());
        assertTrue(p1.failures.get(0), p1.failures.get(0).contains("could not be cancelled") && p1.failures.get(0).contains("t1"));
    }

    @Test
    public void providerFailedWhileProvisioningCancelsAndRollsBack()
    {
        Action a = p1.reach(PROVISIONING);
        provider.statuses.put("t1", ProvisionStatus.failed("no capacity in zone"));
        p1.elapse();
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertTrue(p1.audit(a.id), p1.audit(a.id).contains("phase " + CANCELLING) && p1.audit(a.id).contains("no capacity in zone"));
        assertEquals(Set.of("t1"), provider.cancelled);
        assertEquals(ClusterActionExecution.FINAL_ROLLED_BACK, finalState(a.id));
        assertTrue(p1.failures.isEmpty());
    }

    @Test
    public void statusFailuresAreRetriedUntilProvisionTimeoutThenCancelled()
    {
        Action a = p1.reach(PROVISIONING);
        provider.failStatus = new ProviderException("connection refused");
        long timeout = p1.config.scale_out.provision_timeout.toMilliseconds();
        p1.pass(timeout - 2 * CHUNK);
        p1.in(a, PROVISIONING);
        p1.pass(3 * CHUNK);
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertEquals(Set.of("t1"), provider.cancelled);
    }

    @Test
    public void provisionTimeoutCancelsAndRollsBack()
    {
        Action a = p1.reach(PROVISIONING);
        long timeout = p1.config.scale_out.provision_timeout.toMilliseconds();
        assertEquals(900_000, timeout);
        p1.pass(timeout - CHUNK);
        p1.in(a, PROVISIONING);
        assertEquals(0, provider.count("cancel"));
        p1.pass(3 * CHUNK);
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertEquals(Set.of("t1"), provider.cancelled);
        assertEquals(0, provider.liveNodes());
        assertTrue(p1.audit(a.id), p1.audit(a.id).contains("provision_timeout"));
    }

    @Test
    public void anUnusableAddressOrATicketThatDoesNotFitIsGivenBack()
    {
        Action a = p1.reach(PROVISIONING);
        provider.statuses.put("t1", ProvisionStatus.started("not an address:x:y"));
        p1.elapse();
        p1.elapse();
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertEquals(Set.of("t1"), provider.cancelled);

        setup();
        provider.attributes = Map.of("blob", "x".repeat(4000));
        Action b = p1.approved();
        p1.assertTerminal(b.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertEquals("cancelled at once, with the ticket still in hand", Set.of("t1"), provider.cancelled);
    }

    // ---- the join ----

    @Test
    public void theAddressOfAnExistingMemberIsNotANewNode()
    {
        // a provider that reports a member from before the request must not make the action look joined
        Action a = p1.reach(PROVISIONING);
        provider.statuses.put("t1", ProvisionStatus.started(address(2)));
        p1.elapse();
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertEquals(Set.of("t1"), provider.cancelled);
        assertTrue(p1.audit(a.id), p1.audit(a.id).contains("a member from before the request"));
    }

    @Test
    public void joinTimeoutBeforeTheJoinStartedCancelsAndRollsBack()
    {
        Action a = p1.reach(JOINING);
        register(NEW_NODE);
        long timeout = p1.config.scale_out.join_timeout.toMilliseconds();
        assertEquals(6 * 3_600_000L, timeout);
        p1.pass(timeout - CHUNK);
        p1.in(a, JOINING);
        p1.pass(3 * CHUNK);
        p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertEquals(Set.of("t1"), provider.cancelled);
        assertTrue(p1.failures.isEmpty());
    }

    @Test
    public void joinTimeoutAfterTheJoinStartedFailsAndUndoesNothing()
    {
        Action a = p1.reach(JOIN_STARTED);
        long timeout = p1.config.scale_out.join_timeout.toMilliseconds();
        p1.pass(timeout - 2 * CHUNK);
        p1.in(a, JOINING);
        p1.pass(3 * CHUNK);
        p1.assertTerminal(a.id, ActionState.FAILED, Reasons.ACTUATOR_FAILED);
        assertEquals("released, as FAILED", ClusterActionExecution.FINAL_FAILED, finalState(a.id));
        assertEquals("the ticket is left alone", 0, provider.count("cancel"));
        assertEquals("the join is left alone", NodeState.BOOTSTRAPPING, cluster.log.directory.peerState(nodeId(NEW_NODE)));
        assertEquals("a CRITICAL diagnosis is raised through the host", 1, p1.failures.size());
        assertTrue(p1.failures.get(0), p1.failures.get(0).contains("join_timeout") && p1.failures.get(0).contains(address(NEW_NODE))
                                       && p1.failures.get(0).contains("t1"));
    }

    @Test
    public void aStartedJoinThatIsAbandonedFails()
    {
        Action a = p1.reach(JOIN_STARTED);
        // the bootstrap was aborted: back to registered
        state(NEW_NODE, NodeState.REGISTERED);
        p1.elapse();
        p1.assertTerminal(a.id, ActionState.FAILED, Reasons.ACTUATOR_FAILED);
        assertEquals(0, provider.count("cancel"));
        assertTrue(p1.failures.get(0), p1.failures.get(0).contains("did not finish"));
    }

    @Test
    public void whetherTheJoinStartedIsReadFromTheMetadataNotFromTheProvider()
    {
        Action a = p1.reach(JOINING);
        // the provider says FAILED, the directory says the node is bootstrapping: the directory wins, nothing is cancelled
        register(NEW_NODE);
        state(NEW_NODE, NodeState.BOOTSTRAPPING);
        provider.statuses.put("t1", ProvisionStatus.failed("instance lost"));
        p1.elapse();
        p1.elapse();
        p1.in(a, JOINING);
        assertNotNull(claim().state.get(ScaleOutActuator.S_JOINING));
        assertEquals(0, provider.count("cancel"));
        state(NEW_NODE, NodeState.JOINED);
        p1.elapse();
        p1.in(a, VERIFYING);

        // the provider says FAILED and the node never started to join: cancelled
        setup();
        Action b = p1.reach(JOINING);
        register(NEW_NODE);
        provider.statuses.put("t1", ProvisionStatus.failed("instance lost"));
        p1.elapse();
        p1.elapse();
        p1.assertTerminal(b.id, ActionState.ROLLED_BACK, Reasons.ACTUATOR_ROLLBACK);
        assertEquals(Set.of("t1"), provider.cancelled);
    }

    @Test
    public void aJoinThatStartsWhileTheRequestIsBeingCancelledIsNotUndone()
    {
        // before the cancel reached the provider: the request is not cancelled, the join is left to the cluster
        Action a = p1.reach(JOINING);
        register(NEW_NODE);
        provider.statuses.put("t1", ProvisionStatus.failed("instance lost"));
        provider.failCancel = new ProviderException("503");
        p1.elapse();
        p1.elapse();
        p1.in(a, CANCELLING);
        state(NEW_NODE, NodeState.BOOTSTRAPPING);
        provider.failCancel = null;
        int cancels = provider.count("cancel");
        p1.elapse();
        p1.assertTerminal(a.id, ActionState.COMMITTED, Reasons.COMMITTED_CLEANUP_PENDING);
        assertEquals(cancels, provider.count("cancel"));
        assertTrue(provider.cancelled.isEmpty());

        // while the provider was cancelling: nothing can be taken back, an operator has to look
        setup();
        Action b = p1.reach(JOINING);
        register(NEW_NODE);
        provider.statuses.put("t1", ProvisionStatus.failed("instance lost"));
        provider.duringCancel = () -> state(NEW_NODE, NodeState.BOOTSTRAPPING);
        p1.elapse();
        p1.elapse();
        p1.assertTerminal(b.id, ActionState.FAILED, Reasons.ACTUATOR_FAILED);
        assertEquals(ClusterActionExecution.FINAL_FAILED, finalState(b.id));
        assertTrue(p1.failures.get(0), p1.failures.get(0).contains("had started to join"));
    }

    // ---- kill ----

    @Test
    public void killBeforeTheJoinStartedCancelsAndRollsBack()
    {
        for (String phase : List.of(REQUESTED, PROVISIONING, JOINING))
        {
            setup();
            Action a = p1.reach(phase);
            provider.failRequest = null;
            if (JOINING.equals(phase))
                register(NEW_NODE);
            p1.kill();
            p1.elapse();
            p1.assertTerminal(a.id, ActionState.ROLLED_BACK, Reasons.KILL);
            assertEquals(phase, ClusterActionExecution.FINAL_ROLLED_BACK, finalState(a.id));
            assertEquals(phase, 0, provider.liveNodes());
            if (!REQUESTED.equals(phase))
                assertEquals(phase, Set.of("t1"), provider.cancelled);
            assertOneNodeAtMost(a);
            assertTrue(p1.failures.isEmpty());
        }
    }

    @Test
    public void killOnceTheJoinStartedReleasesAsCleanupPendingAndLeavesTheJoin()
    {
        for (String phase : List.of(JOINING, JOIN_STARTED, VERIFYING))
        {
            setup();
            Action a = p1.reach(phase);
            if (JOINING.equals(phase))
            {
                // started in the metadata, not yet seen by a step: the kill must see it
                register(NEW_NODE);
                state(NEW_NODE, NodeState.BOOTSTRAPPING);
            }
            NodeState before = cluster.log.directory.peerState(nodeId(NEW_NODE));
            p1.kill();
            p1.assertTerminal(a.id, ActionState.COMMITTED, Reasons.COMMITTED_CLEANUP_PENDING);
            assertEquals(phase, ClusterActionExecution.FINAL_COMMITTED_CLEANUP_PENDING, finalState(a.id));
            assertEquals(phase, 0, provider.count("cancel"));
            assertEquals(before, cluster.log.directory.peerState(nodeId(NEW_NODE)));
            assertTrue(p1.failures.isEmpty());
        }
    }

    // ---- owner loss ----

    @Test
    public void ownerLostBeforeAnythingWasRequested()
    {
        // the claim exists, its owner never ran a step
        UUID id = cluster.newActionId();
        assertTrue(p1.coordinator.claim(Lane.TOPOLOGY, id, ScaleOutActuator.NAME, "CLUSTER", ClusterActionExecution.INITIAL_PHASE, Collections.emptyMap()).ok());
        Claim taken = ownerLost();
        p2.run();
        p2.assertTerminal(taken.actionId, ActionState.ROLLED_BACK, Reasons.RECOVERY);
        assertTrue(provider.calls.isEmpty());
    }

    @Test
    public void ownerLostWhileRequestedTheNewOwnerAsksWithTheSameId()
    {
        Action a = p1.reach(REQUESTED);
        int asked = provider.count("request");
        Claim taken = ownerLost();
        provider.failRequest = null;
        p2.run();
        // phase restored, then the request repeated under the id of the proposed action, not of the takeover
        assertEquals(PROVISIONING, claim().phase);
        assertEquals(asked + 1, provider.count("request"));
        assertOneNodeAtMost(a);
        assertEquals(1, provider.tickets.size());
        assertEquals(a.id.toString(), claim().state.get(ScaleOutActuator.S_REQUEST));
        finishOn(p2, taken.actionId);
    }

    @Test
    public void ownerLostBetweenTheProvidersAnswerAndTheTicketInTheLedgerDoesNotProvisionTwice()
    {
        // the provider accepts; from then on nothing of node 1 reaches the log
        provider.afterAccept = () -> p1.env.mode = CommitMode.DROP;
        Action a = p1.approved();
        p1.elapse();
        p1.in(a, REQUESTED);
        assertNull("the ticket did not reach the ledger", claim().state.get(ScaleOutActuator.S_TICKET));
        assertEquals(1, provider.tickets.size());
        provider.afterAccept = () -> {};

        Claim taken = ownerLost();
        p2.run();
        assertEquals(PROVISIONING, claim().phase);
        assertEquals("the same ticket", "t1", claim().state.get(ScaleOutActuator.S_TICKET));
        assertEquals("one node", 1, provider.tickets.size());
        assertOneNodeAtMost(a);

        // the old owner comes back: its claim is gone, it does nothing more
        int calls = provider.calls.size();
        p1.env.mode = CommitMode.APPLY;
        p1.env.catchUp();
        p1.elapse();
        ActionRecord lost = p1.record(a.id);
        assertEquals(ActionState.FAILED, lost.state);
        assertEquals(Reasons.CLAIM_LOST, lost.stateReason);
        assertEquals(calls, provider.calls.size());
        finishOn(p2, taken.actionId);
        assertEquals(1, provider.tickets.size());
    }

    @Test
    public void ownerLostWhileProvisioningOrJoiningTheNewOwnerContinues()
    {
        for (String phase : List.of(PROVISIONING, JOINING, JOIN_STARTED))
        {
            setup();
            Action a = p1.reach(phase);
            Map<String, String> state = claim().state;
            Claim taken = ownerLost();
            assertEquals("the state travels with the claim", state, taken.state);
            p2.run();
            assertEquals(phase, phase.equals(JOIN_STARTED) ? JOINING : phase, claim().phase);
            assertEquals(phase, 1, provider.count("request"));
            assertEquals(phase, 0, provider.count("cancel"));
            finishOn(p2, taken.actionId);
            assertOneNodeAtMost(a);
            assertEquals(phase, 1, provider.count("request"));
        }
    }

    @Test
    public void timeoutsKeepRunningAcrossATakeover()
    {
        Action a = p1.reach(JOIN_STARTED);
        Claim taken = ownerLost();
        p2.run();
        assertEquals(JOINING, claim().phase);
        p2.pass(p2.config.scale_out.join_timeout.toMilliseconds());
        p2.assertTerminal(taken.actionId, ActionState.FAILED, Reasons.ACTUATOR_FAILED);
        assertEquals(ClusterActionExecution.FINAL_FAILED, finalState(taken.actionId));
        assertEquals(0, provider.count("cancel"));
        assertTrue(p2.failures.get(0), p2.failures.get(0).contains("join_timeout"));
        assertOneNodeAtMost(a);

        setup();
        Action b = p1.reach(PROVISIONING);
        taken = ownerLost();
        p2.run();
        p2.pass(p2.config.scale_out.provision_timeout.toMilliseconds());
        p2.assertTerminal(taken.actionId, ActionState.ROLLED_BACK, Reasons.RECOVERY);
        assertEquals(Set.of("t1"), provider.cancelled);
        assertOneNodeAtMost(b);
    }

    @Test
    public void ownerLostWhileVerifyingCommitsInconclusive()
    {
        Action a = p1.reach(VERIFYING);
        Claim taken = ownerLost();
        p2.run();
        p2.assertTerminal(taken.actionId, ActionState.COMMITTED, null);
        assertEquals(Verdict.INCONCLUSIVE, p2.record(taken.actionId).verdict);
        assertEquals(ClusterActionExecution.FINAL_COMMITTED, finalState(taken.actionId));
        assertEquals(0, provider.count("cancel"));
        assertOneNodeAtMost(a);
    }

    @Test
    public void ownerLostWhileCancellingTheNewOwnerCancels()
    {
        Action a = p1.reach(PROVISIONING);
        provider.statuses.put("t1", ProvisionStatus.failed("no capacity"));
        provider.failCancel = new ProviderException("503");
        p1.elapse();
        p1.elapse();
        p1.in(a, CANCELLING);
        Claim taken = ownerLost();
        provider.failCancel = null;
        p2.run();
        p2.assertTerminal(taken.actionId, ActionState.ROLLED_BACK, Reasons.RECOVERY);
        assertEquals(Set.of("t1"), provider.cancelled);
        assertEquals(1, provider.count("request"));
    }

    @Test
    public void aRestartedOwnerResumesItsOwnAction()
    {
        for (String phase : List.of(PROVISIONING, JOIN_STARTED))
        {
            setup();
            Action a = p1.reach(phase);
            // the process dies; a new layer starts over the same store and ledger
            p1.start();
            assertTrue(p1.executor.recover(Set.of()).complete);
            p1.run();
            assertEquals(a.id, claim().actionId);
            assertEquals(phase.equals(JOIN_STARTED) ? JOINING : phase, claim().phase);
            assertEquals(ActionState.APPLYING, p1.record(a.id).state);
            assertEquals(0, provider.count("cancel"));
            finishOn(p1, a.id);
            assertEquals(phase, 1, provider.count("request"));
        }
    }

    /** Lets the action, wherever it is, run to a joined and verified node on the given peer. */
    private void finishOn(Peer p, UUID id)
    {
        provider.statuses.put("t1", ProvisionStatus.started(address(NEW_NODE)));
        p.elapse();
        if (nodeId(NEW_NODE) == null)
            register(NEW_NODE);
        state(NEW_NODE, NodeState.BOOTSTRAPPING);
        p.elapse();
        assertEquals(JOINING, claim().phase);
        assertNotNull(claim().state.get(ScaleOutActuator.S_JOINING));
        state(NEW_NODE, NodeState.JOINED);
        p.elapse();
        assertEquals(VERIFYING, claim().phase);
        p.pass(p.config.scale_out.verify_window.toMilliseconds() + CHUNK);
        ActionRecord r = p.record(id);
        assertEquals(ActionState.COMMITTED, r.state);
        assertNull(claim());
        assertEquals(ClusterActionExecution.FINAL_COMMITTED, finalState(id));
        assertEquals(0, provider.count("cancel"));
    }
}
