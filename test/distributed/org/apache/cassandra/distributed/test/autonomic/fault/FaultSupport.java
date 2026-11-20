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
package org.apache.cassandra.distributed.test.autonomic.fault;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.execute.ActionExecutor;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ClusterActionExecution;
import org.apache.cassandra.autonomic.execute.FakeClusterActuator;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.autonomic.knowledge.KnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.InjectedSensor;
import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertTrue;

/**
 * What the T7.2 fault tests share. Static methods named {@code in*} and the nested classes run inside an instance
 * (each instance has its own class loader, so each has its own copy of the static state). Faults are injected through
 * the public test seams of {@link AutonomicService} and, where the layer has only a package-private seam
 * ({@code executor()}, {@code registerDetector}, {@code KnowledgeStore.setRunner}), through reflection, so that no
 * production code changes for the tests.
 */
public final class FaultSupport
{
    public static final String KNOB = "compaction_throughput";
    /** Name of the policy of the actions a test proposes itself, as the control thread's plan sink would. */
    public static final String TEST_POLICY = "fault_test";
    public static final String COMPONENT_FAILED = "autonomic.component_failed";
    public static final String LOOP_STALLED = "autonomic.loop_stalled";
    public static final String CIRCUIT_BREAKER = "autonomic.circuit_breaker";
    public static final String ACTION_FAILED = "autonomic.action_failed";

    /** The injected sensor of this instance; null until {@link #injectSensors}. */
    static FlakySensor sensor;
    /** Every transition of this instance's executor since {@link #inRecordTransitions}: id, state, reason, knob value. */
    static final List<String> trail = Collections.synchronizedList(new ArrayList<>());

    private FaultSupport()
    {
    }

    /** Fast loops, no startup grace, the compaction backlog detector and the compaction throughput actuator armed. */
    static IInstanceConfig nodeConfig(IInstanceConfig c)
    {
        return c.set("autonomic.enabled", true)
                .set("autonomic.dry_run", false)
                .set("autonomic.startup_grace", "1s")
                .set("autonomic.monitor_interval", "1s")
                .set("autonomic.analyze_interval", "1s")
                .set("autonomic.sampling.enabled", false)
                .set("autonomic.detectors", Map.of("compaction_backlog", Map.of("pending_tasks", 10, "for_evaluations", 3, "max_signal_age_ms", 5000)))
                .set("autonomic.actuators", Map.of(KNOB, Map.of("enabled", true, "baseline_window", "5s", "observation_window", "10s")));
    }

    /**
     * For the cluster faults: the layer starts disabled so that the test can register its cluster actuator first
     * ({@link #inStartClusterLayer}); fast loops, short commit deadline and lease.
     */
    static IInstanceConfig clusterConfig(IInstanceConfig c, String lease)
    {
        return c.with(Feature.NETWORK, Feature.GOSSIP)
                .set("autonomic.enabled", false)
                .set("autonomic.dry_run", false)
                .set("autonomic.startup_grace", "1s")
                .set("autonomic.monitor_interval", "1s")
                .set("autonomic.analyze_interval", "1s")
                .set("autonomic.sampling.enabled", false)
                .set("autonomic.safety.action_step_timeout", "5s")
                .set("autonomic.coordination.lease", lease)
                .set("autonomic.coordination.lease_safety_margin", "5s");
    }

    // ---- driven from the test ----

    /** Replaces the compaction and client sensors of the node by one whose signals the test pushes. */
    static void injectSensors(IInvokableInstance node)
    {
        node.runOnInstance(() -> {
            AutonomicService.instance.removeSensor("compaction");
            AutonomicService.instance.removeSensor("client_request");
            sensor = new FlakySensor();
            AutonomicService.instance.registerSensor(sensor);
        });
    }

    /** Pending compactions as given, client latencies flat. */
    static void push(IInvokableInstance node, double pending)
    {
        node.runOnInstance(() -> {
            Subject self = Subject.node();
            sensor.injected.push(Signals.COMPACTION_PENDING_TASKS, self, pending)
                           .push(Signals.CLIENT_READ_LATENCY_P99_MICROS, self, 1000)
                           .push(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, self, 1000);
        });
    }

    static int throughput(IInvokableInstance node)
    {
        return node.callOnInstance(() -> StorageService.instance.getCompactionThroughputMbPerSec());
    }

    /**
     * Pushes a backlog of twice the detector's threshold until the production policy has raised the compaction
     * throughput and the action is VERIFYING. Returns its row (see {@link ScenarioSupport#actions}).
     */
    static Object[] driveToVerifying(Cluster cluster, int node, int configured) throws InterruptedException
    {
        long end = System.currentTimeMillis() + ScenarioSupport.DEADLINE_MILLIS;
        while (System.currentTimeMillis() < end)
        {
            Object[] row = ScenarioSupport.inState(cluster, node, KNOB, "VERIFYING");
            if (row != null && throughput(cluster.get(node)) > configured)
                return row;
            push(cluster.get(node), 20);
            Thread.sleep(500);
        }
        throw new AssertionError("no VERIFYING action of " + KNOB + ": " + ScenarioSupport.describe(ScenarioSupport.actions(cluster, node, KNOB)));
    }

    /** {@code to_state} of the transitions of an action in {@code system_autonomic.action_events}, oldest first. */
    static List<String> events(Cluster cluster, int node, UUID id)
    {
        List<String> out = new ArrayList<>();
        for (Object[] e : cluster.coordinator(node).execute("SELECT to_state FROM system_autonomic.action_events WHERE action_id = ?",
                                                            ConsistencyLevel.ONE, id))
            out.add((String) e[0]);
        return out;
    }

    /** Rows of the actions table of the actuator that are not REJECTED: what was, or was about to be, applied. */
    static List<Object[]> applied(Cluster cluster, int node, String actuator)
    {
        List<Object[]> out = new ArrayList<>();
        for (Object[] row : ScenarioSupport.actions(cluster, node, actuator))
            if (!"REJECTED".equals(row[1]))
                out.add(row);
        return out;
    }

    static String localState(Cluster cluster, int node, String key)
    {
        Object[][] rows = cluster.coordinator(node).execute("SELECT value FROM system_autonomic.local_state WHERE key = ?", ConsistencyLevel.ONE, key);
        return rows.length == 0 ? null : (String) rows[0][0];
    }

    static void assertContains(String what, List<String> list, String... expected)
    {
        for (String e : expected)
            assertTrue(what + " lacks " + e + ": " + list, list.contains(e));
    }

    // ---- inside an instance ----

    /** The test's cluster actuator on this instance; set by {@link #inStartClusterLayer}. */
    static FakeClusterActuator clusterActuator;

    /**
     * Registers a {@link FakeClusterActuator} whose action advances to P1 and then stays there (an action in the
     * middle of its work), enables it and starts the layer.
     */
    static void inStartClusterLayer(String name)
    {
        clusterActuator = new FakeClusterActuator(name);
        clusterActuator.onStep = claim -> ClusterActionExecution.INITIAL_PHASE.equals(claim.phase)
                                          ? StepResult.advance("P1", ImmutableMap.of("a", "1"), 0)
                                          : StepResult.stay(500);
        AutonomicService.instance.registerActuator(clusterActuator);
        AutonomicService.instance.setActuatorEnabled(name, true);
        AutonomicService.instance.setEnabled(true);
    }

    /** Calls of the cluster actuator of this instance whose kind (step, recover, abort, onPhase) is given. */
    static List<String> inClusterCalls(String kind)
    {
        List<String> out = new ArrayList<>();
        synchronized (clusterActuator.calls)
        {
            for (String c : clusterActuator.calls)
                if (c.startsWith(kind + ':'))
                    out.add(c);
        }
        return out;
    }

    /** What the failure detector of this instance concludes once a peer is unreachable, without waiting for phi. */
    static void inConvict(InetSocketAddress peer)
    {
        FailureDetector.instance.forceConviction(InetAddressAndPort.getByAddressOverrideDefaults(peer.getAddress(), peer.getPort()));
    }

    static boolean inAlive(InetSocketAddress peer)
    {
        return FailureDetector.instance.isAlive(InetAddressAndPort.getByAddressOverrideDefaults(peer.getAddress(), peer.getPort()));
    }

    /** {@code lane|action_id|owner|phase} of every claim in the ledger this instance sees. */
    static List<String> inClaims()
    {
        List<String> out = new ArrayList<>();
        for (Map<String, String> c : AutonomicService.instance.getClusterClaims())
            out.add(c.get("lane") + '|' + c.get("action_id") + '|' + c.get("owner") + '|' + c.get("phase"));
        return out;
    }

    /** {@code final_state} of the action in the ledger's recent list this instance sees, or null. */
    static String inRecentFinalState(String actionId)
    {
        for (Map<String, String> r : AutonomicService.instance.getClusterRecentActions())
            if (actionId.equals(r.get("action_id")))
                return r.get("final_state");
        return null;
    }

    static ActionExecutor inExecutor()
    {
        return (ActionExecutor) invoke(AutonomicService.instance, "executor", new Class<?>[0]);
    }

    /** Records every transition from now on in {@link #trail}, with the live compaction throughput at that moment. */
    static void inRecordTransitions()
    {
        inExecutor().addListener((record, from, detail) -> trail.add(record.id() + ":" + record.state + ':' + record.stateReason + ':'
                                                                     + StorageService.instance.getCompactionThroughputMbPerSec()));
    }

    /** {@code state:reason:verdict} of the executor's in-memory record of the action, or null. */
    static String inMemoryRecord(UUID id)
    {
        Optional<ActionRecord> r = inExecutor().record(id);
        return r.map(x -> x.state + ":" + x.stateReason + ':' + x.verdict).orElse(null);
    }

    static long inRejections(String reason)
    {
        return inExecutor().rejectionCounts().getOrDefault(reason, 0L);
    }

    static boolean inDiagnosed(String type)
    {
        for (Diagnosis d : AutonomicService.instance.activeDiagnoses())
            if (d.type.equals(type))
                return true;
        return false;
    }

    static boolean inComponentDisabled(String kind, String name)
    {
        for (AutonomicService.ComponentSnapshot c : AutonomicService.instance.componentSnapshots())
            if (c.kind.equals(kind) && c.name.equals(name))
                return c.disabled;
        return false;
    }

    /**
     * Proposes an action as a policy's plan would (through {@code ActionExecutor.propose}, so the governor decides), with a
     * verify spec on the pending compactions of this node. Returns {@code state|reason|id} of the record, or null if the
     * executor did not record a repeated identical rejection.
     */
    static String inPropose(String actuator, String scope, String target, Map<String, String> params, long baselineMillis, long observationMillis)
    {
        VerifySpec verify = new VerifySpec(Signals.COMPACTION_PENDING_TASKS, Subject.node(), true, 0.1, List.of(), baselineMillis, observationMillis);
        Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), actuator, ActionScope.valueOf(scope), Subject.parse(target), params, verify);
        // a diagnosis id no detector raised: the plan is the test's own
        Plan plan = new Plan(UUID.randomUUID(), UUID.randomUUID(), TEST_POLICY, List.of(action), "T7.2 fault test");
        ActionRecord record = inExecutor().propose(plan, action, AutonomicService.instance.knowledge());
        return record == null ? null : record.state + "|" + record.stateReason + '|' + record.id();
    }

    static void inRegisterDetector(Detector detector)
    {
        invoke(AutonomicService.instance, "registerDetector", new Class<?>[]{ Detector.class }, detector);
    }

    private static Object invoke(Object target, String name, Class<?>[] types, Object... args)
    {
        try
        {
            Method m = target.getClass().getDeclaredMethod(name, types);
            m.setAccessible(true);
            return m.invoke(target, args);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException(e.getCause());
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /** A sensor that passes on what the test pushes, and throws on every collect while {@link #failing}. */
    public static final class FlakySensor implements Sensor
    {
        final InjectedSensor injected = new InjectedSensor();
        volatile boolean failing;

        @Override
        public String name()
        {
            return injected.name();
        }

        @Override
        public Set<SignalId> signals()
        {
            return injected.signals();
        }

        @Override
        public long minIntervalMillis()
        {
            return 0;
        }

        @Override
        public void collect(ObservationSink sink, long nowMillis)
        {
            if (failing)
                throw new IllegalStateException("injected sensor fault");
            injected.collect(sink, nowMillis);
        }
    }

    /** A detector that blocks the control thread inside {@code analyze} between {@link #stall} and {@link #release}. */
    public static final class StallingDetector implements Detector
    {
        static volatile CountDownLatch stalled;
        static volatile boolean entered;
        /** Calls of {@link #analyze}: one per control tick. */
        static final AtomicInteger ticks = new AtomicInteger();

        static void stall()
        {
            entered = false;
            stalled = new CountDownLatch(1);
        }

        static void release()
        {
            CountDownLatch latch = stalled;
            stalled = null;
            if (latch != null)
                latch.countDown();
        }

        @Override
        public String name()
        {
            return "stalling";
        }

        @Override
        public Set<SignalId> requires()
        {
            return Set.of();
        }

        @Override
        public List<Diagnosis> analyze(KnowledgeView knowledge)
        {
            ticks.incrementAndGet();
            CountDownLatch latch = stalled;
            if (latch != null)
            {
                entered = true;
                try
                {
                    // bounded, so that a failed test cannot keep the thread forever
                    latch.await(3, TimeUnit.MINUTES);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
            return List.of();
        }
    }

    /**
     * Makes the knowledge store of this instance unwritable: every statement other than a SELECT throws, from the first
     * write whose arguments contain {@code downAtWriteOf} (if set) or from {@link #down} on. Reads keep working.
     */
    public static final class StoreFault implements InvocationHandler
    {
        static volatile boolean down;
        static volatile String downAtWriteOf;

        static void install(KnowledgeStore store)
        {
            try
            {
                Class<?> runner = Class.forName(KnowledgeStore.class.getName() + "$QueryRunner", true, KnowledgeStore.class.getClassLoader());
                Object proxy = Proxy.newProxyInstance(runner.getClassLoader(), new Class<?>[]{ runner }, new StoreFault());
                FaultSupport.invoke(store, "setRunner", new Class<?>[]{ runner }, proxy);
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
        {
            if (!"run".equals(method.getName()))
                throw new UnsupportedOperationException(method.getName());
            String cql = (String) args[0];
            Object[] values = (Object[]) args[1];
            if (!cql.trim().toUpperCase().startsWith("SELECT"))
            {
                String trigger = downAtWriteOf;
                if (!down && trigger != null)
                    for (Object v : values)
                        if (trigger.equals(String.valueOf(v)))
                            down = true;
                if (down)
                    throw new RuntimeException("injected fault: knowledge store unwritable");
            }
            return QueryProcessor.executeInternal(cql, values);
        }
    }
}
