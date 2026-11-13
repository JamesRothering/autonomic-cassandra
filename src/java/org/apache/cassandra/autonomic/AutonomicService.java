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
package org.apache.cassandra.autonomic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.analyze.Correlator;
import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.KnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.PlanSink;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Entry point of the autonomic layer. Lifecycle: DISABLED, STARTING, RUNNING, PAUSED (overview 3.6).
 * When {@code autonomic.enabled} is false only the MBean is registered and no thread exists.
 */
public class AutonomicService implements AutonomicServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(AutonomicService.class);

    public enum State
    {
        DISABLED, STARTING, RUNNING, PAUSED
    }

    public static final String MONITOR_THREAD = "AutonomicMonitor";
    public static final String CONTROL_THREAD = "AutonomicControl";
    public static final String ACTUATOR_THREAD = "AutonomicActuator";

    public static final AutonomicService instance = new AutonomicService();

    private final LongSupplier clock;
    private final BooleanSupplier nodeJoined;

    private volatile State state = State.DISABLED;
    private volatile long startedAtMillis;
    private volatile boolean armed;
    // pause requested; also honoured when requested before the loop is armed
    private volatile boolean pauseRequested;
    private volatile String pauseReason;

    public static final String COMPONENT_FAILED = "autonomic.component_failed";
    public static final String LOOP_STALLED = "autonomic.loop_stalled";

    // Static registration (overview 3.4). Empty in production until the sensors, detectors and policies of later
    // tasks are added here; tests register fakes through the package-private register* methods.
    private static List<Sensor> productionSensors()
    {
        return Collections.emptyList();
    }

    private static List<Detector> productionDetectors()
    {
        return Collections.emptyList();
    }

    private static List<Policy> productionPolicies()
    {
        return Collections.emptyList();
    }

    private static Correlator productionCorrelator()
    {
        return (raw, knowledge) -> raw;
    }

    /** Failure accounting of one sensor, detector or policy. */
    static final class ComponentHealth
    {
        final String kind;
        final String name;
        volatile int consecutiveFailures;
        volatile long totalFailures;
        volatile long slowCalls;
        volatile boolean disabled;
        volatile String lastError;

        ComponentHealth(String kind, String name)
        {
            this.kind = kind;
            this.name = name;
        }

        @Override
        public String toString()
        {
            return kind + ' ' + name + (disabled ? " DISABLED" : " ok") + " failures=" + totalFailures + " slow=" + slowCalls;
        }
    }

    private final List<Sensor> sensors = new CopyOnWriteArrayList<>(productionSensors());
    private final List<Detector> detectors = new CopyOnWriteArrayList<>(productionDetectors());
    private final List<Policy> policies = new CopyOnWriteArrayList<>(productionPolicies());
    private volatile Correlator correlator = productionCorrelator();
    // TODO T2.6: replaced by a sink that goes through the SafetyGovernor to the ActionExecutor
    private volatile PlanSink planSink = (plan, knowledge) -> logger.info("Plan produced (no executor yet): {}", plan);

    private final Map<String, ComponentHealth> health = new ConcurrentHashMap<>();
    private final Map<Sensor, Long> lastCollected = new ConcurrentHashMap<>();
    private final Map<String, Diagnosis> selfDiagnoses = new ConcurrentHashMap<>();
    private volatile List<Diagnosis> detected = Collections.emptyList();
    private final KnowledgeStore knowledgeStore;
    /** Diagnoses as last written to the store, by id; guarded by {@code this}. */
    private final Map<UUID, Diagnosis> persistedDiagnoses = new LinkedHashMap<>();
    private volatile long monitorHeartbeat;
    private volatile long controlHeartbeat;

    private volatile ObservationStore store;
    private volatile InMemoryKnowledgeView view;

    private ScheduledExecutorPlus monitor;
    private ScheduledExecutorPlus control;
    private ScheduledExecutorPlus actuator;

    private AutonomicService()
    {
        this(org.apache.cassandra.utils.Clock.Global::currentTimeMillis,
             () -> ClusterMetadata.current().myNodeState() == NodeState.JOINED,
             new KnowledgeStore());
    }

    @VisibleForTesting
    AutonomicService(LongSupplier clock, BooleanSupplier nodeJoined)
    {
        this(clock, nodeJoined, null);
    }

    /** {@code knowledgeStore} may be null, in which case nothing is persisted. */
    @VisibleForTesting
    AutonomicService(LongSupplier clock, BooleanSupplier nodeJoined, KnowledgeStore knowledgeStore)
    {
        this.clock = clock;
        this.nodeJoined = nodeJoined;
        this.knowledgeStore = knowledgeStore;
    }

    /** The node-local knowledge store; null if this instance does not persist. Usable while the layer is disabled. */
    public KnowledgeStore knowledgeStore()
    {
        return knowledgeStore;
    }

    private static AutonomicConfig config()
    {
        return DatabaseDescriptor.getAutonomicConfig();
    }

    /** Registers the MBean and, if {@code autonomic.enabled}, starts the layer. */
    public void start()
    {
        registerMBean();
        // FC1: an always-on data-plane service (layout, adaptive schema) is started here, regardless of
        // autonomic.enabled, and stopped in shutdownBlocking(). It must create no threads until it has work.
        if (config().enabled)
            startLoops();
    }

    private void registerMBean()
    {
        if (!MBeanWrapper.instance.isRegistered(MBEAN_NAME))
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    private synchronized void startLoops()
    {
        if (state != State.DISABLED)
            return;
        state = State.STARTING;
        armed = false;
        startedAtMillis = clock.getAsLong();
        // TODO T2.6: run restart recovery (overview 7.6) and read the persisted pause flag into pauseRequested
        AutonomicConfig config = config();
        store = new ObservationStore(seriesCapacity(config), config.max_series);
        view = new InMemoryKnowledgeView(store, clock);
        monitorHeartbeat = controlHeartbeat = startedAtMillis;
        detected = Collections.emptyList();
        selfDiagnoses.clear();
        persistedDiagnoses.clear();
        health.clear();
        lastCollected.clear();
        for (Sensor sensor : sensors)
            safely("start", sensor.name(), sensor::start);
        monitor = executorFactory().scheduled(false, MONITOR_THREAD);
        control = executorFactory().scheduled(false, CONTROL_THREAD);
        actuator = executorFactory().scheduled(false, ACTUATOR_THREAD);
        // starts the actuator thread; real work is event-driven and arrives with T2.6
        actuator.execute(() -> {});
        monitor.scheduleWithFixedDelay(this::monitorTick, config.monitor_interval.toMilliseconds(), config.monitor_interval.toMilliseconds(), TimeUnit.MILLISECONDS);
        control.scheduleWithFixedDelay(this::controlTick, config.analyze_interval.toMilliseconds(), config.analyze_interval.toMilliseconds(), TimeUnit.MILLISECONDS);
        logger.info("Autonomic layer started (dry_run={})", config.dry_run);
    }

    private static int seriesCapacity(AutonomicConfig config)
    {
        long points = config.observation_retention.toMilliseconds() / Math.max(1, config.monitor_interval.toMilliseconds()) + 1;
        return (int) Math.max(2, Math.min(points, 100_000));
    }

    /** Runs on the AutonomicMonitor thread. Must never throw. */
    @VisibleForTesting
    void monitorTick()
    {
        try
        {
            ObservationStore store = this.store;
            if (state == State.DISABLED || store == null)
                return;
            long now = clock.getAsLong();
            monitorHeartbeat = now;
            long budget = config().safety.sensor_budget.toMilliseconds();
            for (Sensor sensor : sensors)
            {
                ComponentHealth h = health("sensor", sensor.name());
                if (h.disabled)
                    continue;
                Long last = lastCollected.get(sensor);
                if (last != null && now - last < sensor.minIntervalMillis())
                    continue;
                lastCollected.put(sensor, now);
                long begin = clock.getAsLong();
                try
                {
                    sensor.collect(store, now);
                }
                catch (Throwable t)
                {
                    recordFailure(h, t);
                    continue;
                }
                long took = clock.getAsLong() - begin;
                if (took > budget)
                {
                    h.slowCalls++;
                    recordFailure(h, new IllegalStateException("sensor took " + took + "ms, budget " + budget + "ms"));
                }
                else
                {
                    h.consecutiveFailures = 0;
                }
            }
            checkHeartbeat("control", controlHeartbeat, config().analyze_interval.toMilliseconds(), now);
        }
        catch (Throwable t)
        {
            handleTickThrowable("monitor", t);
        }
    }

    /** Runs on the AutonomicControl thread. Must never throw. */
    @VisibleForTesting
    void controlTick()
    {
        try
        {
            InMemoryKnowledgeView view = this.view;
            if (state == State.DISABLED || view == null)
                return;
            long now = clock.getAsLong();
            controlHeartbeat = now;
            checkHeartbeat("monitor", monitorHeartbeat, config().monitor_interval.toMilliseconds(), now);
            if (!evaluateArmed())
                return;

            List<Diagnosis> raw = new ArrayList<>();
            for (Detector detector : detectors)
            {
                ComponentHealth h = health("detector", detector.name());
                if (h.disabled)
                    continue;
                try
                {
                    raw.addAll(detector.analyze(view));
                    h.consecutiveFailures = 0;
                }
                catch (Throwable t)
                {
                    recordFailure(h, t);
                }
            }
            List<Diagnosis> correlated = raw;
            try
            {
                correlated = correlator.correlate(raw, view);
            }
            catch (Throwable t)
            {
                handleThrowable("correlator", t);
            }
            detected = List.copyOf(correlated);
            refreshView();

            // PAUSED: diagnoses are recorded, no plan is produced
            if (state != State.RUNNING)
                return;
            for (Diagnosis diagnosis : correlated)
            {
                for (Policy policy : policies)
                {
                    ComponentHealth h = health("policy", policy.name());
                    if (h.disabled)
                        continue;
                    try
                    {
                        if (!policy.handles().contains(diagnosis.type))
                            continue;
                        Optional<Plan> plan = policy.plan(diagnosis, view);
                        h.consecutiveFailures = 0;
                        if (plan.isPresent())
                            handOver(plan.get(), view);
                    }
                    catch (Throwable t)
                    {
                        recordFailure(h, t);
                    }
                }
            }
        }
        catch (Throwable t)
        {
            handleTickThrowable("control", t);
        }
    }

    private void handOver(Plan plan, KnowledgeView knowledge)
    {
        try
        {
            planSink.accept(plan, knowledge);
        }
        catch (Throwable t)
        {
            handleThrowable("plan sink", t);
        }
    }

    private ComponentHealth health(String kind, String name)
    {
        return health.computeIfAbsent(kind + ':' + name, k -> new ComponentHealth(kind, name));
    }

    private void recordFailure(ComponentHealth h, Throwable t)
    {
        handleThrowable(h.kind + ' ' + h.name, t);
        h.totalFailures++;
        h.lastError = t.toString();
        if (++h.consecutiveFailures >= config().safety.component_failure_threshold && !h.disabled)
        {
            h.disabled = true;
            logger.warn("Autonomic {} {} disabled after {} consecutive failures; resume() re-enables it", h.kind, h.name, h.consecutiveFailures);
            long now = clock.getAsLong();
            raiseSelfDiagnosis(COMPONENT_FAILED + ':' + h.kind + ':' + h.name, COMPONENT_FAILED, Severity.WARN,
                               h.kind + ' ' + h.name + " disabled after " + h.consecutiveFailures + " consecutive failures",
                               Map.of("component", h.kind + ':' + h.name, "last_error", String.valueOf(h.lastError)), now);
        }
    }

    private void handleThrowable(String what, Throwable t)
    {
        if (t instanceof Error)
        {
            try
            {
                JVMStabilityInspector.inspectThrowable(t);
            }
            catch (Throwable ignored)
            {
                // the inspector may rethrow (for example OutOfMemoryError); the tick must still survive
            }
        }
        logger.warn("Autonomic {} failed: {}", what, t.toString(), t);
    }

    private void handleTickThrowable(String loop, Throwable t)
    {
        handleThrowable(loop + " tick", t);
    }

    private void safely(String op, String name, Runnable r)
    {
        try
        {
            r.run();
        }
        catch (Throwable t)
        {
            handleThrowable(op + ' ' + name, t);
        }
    }

    private void checkHeartbeat(String loop, long heartbeat, long intervalMillis, long now)
    {
        String key = LOOP_STALLED + ':' + loop;
        if (now - heartbeat > 3 * intervalMillis)
        {
            if (!selfDiagnoses.containsKey(key))
                raiseSelfDiagnosis(key, LOOP_STALLED, Severity.CRITICAL,
                                   loop + " loop has not run for " + (now - heartbeat) + "ms",
                                   Map.of("loop", loop, "stalled_millis", String.valueOf(now - heartbeat)), now);
        }
        else if (selfDiagnoses.remove(key) != null)
        {
            refreshView();
        }
    }

    private void raiseSelfDiagnosis(String key, String type, Severity severity, String summary, Map<String, String> evidence, long now)
    {
        Diagnosis existing = selfDiagnoses.get(key);
        UUID id = existing != null ? existing.id : UUID.randomUUID();
        long first = existing != null ? existing.firstSeenMillis : now;
        selfDiagnoses.put(key, new Diagnosis(id, type, Subject.node(), severity, 1.0, first, now, summary, evidence, null));
        refreshView();
    }

    private synchronized void refreshView()
    {
        InMemoryKnowledgeView v = view;
        if (v == null)
            return;
        List<Diagnosis> all = new ArrayList<>(detected);
        all.addAll(selfDiagnoses.values());
        v.setActiveDiagnoses(all);
        persistDiagnoses(all);
    }

    /** Writes diagnoses that opened, changed severity or root cause, or cleared since the last call; off the calling thread. */
    private void persistDiagnoses(List<Diagnosis> active)
    {
        ScheduledExecutorPlus writer = actuator;
        if (knowledgeStore == null || writer == null)
            return;
        long now = clock.getAsLong();
        List<Runnable> writes = new ArrayList<>();
        Map<UUID, Diagnosis> current = new LinkedHashMap<>();
        for (Diagnosis d : active)
        {
            current.put(d.id, d);
            Diagnosis old = persistedDiagnoses.get(d.id);
            if (old == null || old.severity != d.severity || !Objects.equals(old.rootCause, d.rootCause))
                writes.add(() -> knowledgeStore.upsertDiagnosis(d));
        }
        for (Diagnosis old : persistedDiagnoses.values())
            if (!current.containsKey(old.id))
                writes.add(() -> knowledgeStore.clearDiagnosis(old, now));
        if (writes.isEmpty())
            return;
        persistedDiagnoses.clear();
        persistedDiagnoses.putAll(current);
        try
        {
            // the actuator thread is idle until T2.6; store failures are absorbed inside KnowledgeStore
            writer.execute(() -> writes.forEach(Runnable::run));
        }
        catch (Throwable t)
        {
            logger.debug("Could not schedule diagnosis persistence", t);
        }
    }

    @VisibleForTesting
    void registerSensor(Sensor sensor)
    {
        sensors.add(sensor);
    }

    @VisibleForTesting
    void registerDetector(Detector detector)
    {
        detectors.add(detector);
    }

    @VisibleForTesting
    void setCorrelator(Correlator correlator)
    {
        this.correlator = correlator;
    }

    @VisibleForTesting
    void registerPolicy(Policy policy)
    {
        policies.add(policy);
    }

    /** Seam for T2.6: replaces the logging sink with the SafetyGovernor / ActionExecutor hand-over. */
    @VisibleForTesting
    void setPlanSink(PlanSink sink)
    {
        this.planSink = sink;
    }

    @VisibleForTesting
    ObservationStore store()
    {
        return store;
    }

    @VisibleForTesting
    KnowledgeView view()
    {
        return view;
    }

    @VisibleForTesting
    ComponentHealth componentHealth(String kind, String name)
    {
        return health.get(kind + ':' + name);
    }

    /**
     * Re-evaluates the armed condition: node JOINED and {@code startup_grace} elapsed since start. Moves STARTING to
     * RUNNING (or PAUSED when a pause was requested earlier).
     */
    @VisibleForTesting
    synchronized boolean evaluateArmed()
    {
        if (state == State.DISABLED)
            return false;
        if (!armed)
        {
            long graceMillis = config().startup_grace.toMilliseconds();
            if (clock.getAsLong() - startedAtMillis < graceMillis || !nodeJoined.getAsBoolean())
                return false;
            armed = true;
            if (state == State.STARTING)
                state = pauseRequested ? State.PAUSED : State.RUNNING;
            logger.info("Autonomic layer armed, state {}", state);
        }
        return true;
    }

    public void shutdownBlocking() throws ExecutionException, InterruptedException
    {
        try
        {
            shutdownBlocking(1L, TimeUnit.MINUTES);
        }
        catch (TimeoutException e)
        {
            throw new ExecutionException(e);
        }
    }

    /** Stops the loops and releases all threads; the MBean stays registered so the layer can be re-enabled. */
    public synchronized void shutdownBlocking(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        // FC1: stop the always-on data-plane service here.
        if (state == State.DISABLED)
            return;
        state = State.DISABLED;
        armed = false;
        List<ScheduledExecutorPlus> executors = new ArrayList<>();
        executors.add(monitor);
        executors.add(control);
        executors.add(actuator);
        monitor = control = actuator = null;
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executors);
        for (Sensor sensor : sensors)
            safely("stop", sensor.name(), sensor::stop);
        logger.info("Autonomic layer stopped");
    }

    public State state()
    {
        return state;
    }

    public boolean isArmed()
    {
        return armed;
    }

    @Override
    public String getState()
    {
        return state.name();
    }

    @Override
    public Map<String, String> getStatus()
    {
        Map<String, String> status = new LinkedHashMap<>();
        status.put("state", state.name());
        status.put("enabled", String.valueOf(config().enabled));
        status.put("dry_run", String.valueOf(config().dry_run));
        status.put("armed", String.valueOf(armed));
        status.put("paused", String.valueOf(pauseRequested));
        List<String> disabled = new ArrayList<>();
        for (ComponentHealth h : health.values())
            if (h.disabled)
                disabled.add(h.kind + ':' + h.name);
        status.put("disabled_components", String.join(",", disabled));
        if (pauseReason != null)
            status.put("pause_reason", pauseReason);
        return status;
    }

    @Override
    public boolean isEnabled()
    {
        return config().enabled;
    }

    @Override
    public void setEnabled(boolean enabled)
    {
        config().enabled = enabled;
        if (enabled)
        {
            startLoops();
            return;
        }
        try
        {
            shutdownBlocking();
        }
        catch (ExecutionException | InterruptedException e)
        {
            if (e instanceof InterruptedException)
                Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isDryRun()
    {
        return config().dry_run;
    }

    @Override
    public void setDryRun(boolean dryRun)
    {
        config().dry_run = dryRun;
    }

    @Override
    public synchronized void pause(String reason)
    {
        // TODO T2.6: persist the pause flag so a restart does not re-arm; add kill()
        if (state == State.DISABLED)
            throw new IllegalStateException("Autonomic layer is not enabled");
        pauseRequested = true;
        pauseReason = reason;
        if (state == State.RUNNING)
            state = State.PAUSED;
    }

    @Override
    public synchronized void resume()
    {
        if (state == State.DISABLED)
            throw new IllegalStateException("Autonomic layer is not enabled");
        pauseRequested = false;
        pauseReason = null;
        if (state == State.PAUSED)
            state = State.RUNNING;
        for (ComponentHealth h : health.values())
        {
            h.disabled = false;
            h.consecutiveFailures = 0;
        }
        selfDiagnoses.keySet().removeIf(k -> k.startsWith(COMPONENT_FAILED));
        refreshView();
    }
}
