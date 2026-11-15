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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.analyze.Correlator;
import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.RuleCorrelator;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.detectors.BaselineDriftDetector;
import org.apache.cassandra.autonomic.analyze.detectors.Detectors;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.LedgerClusterGate;
import org.apache.cassandra.autonomic.coord.Recent;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.execute.ActionExecutor;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.ActuatorRegistry;
import org.apache.cassandra.autonomic.execute.ClusterActionExecution;
import org.apache.cassandra.autonomic.execute.ClusterActuator;
import org.apache.cassandra.autonomic.execute.StepScheduler;
import org.apache.cassandra.autonomic.execute.actuators.Actuators;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.KnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.ClientRequestSensor;
import org.apache.cassandra.autonomic.monitor.sensors.CompactionSensor;
import org.apache.cassandra.autonomic.monitor.sensors.CpuSensor;
import org.apache.cassandra.autonomic.monitor.sensors.DiskSensor;
import org.apache.cassandra.autonomic.monitor.sensors.GcSensor;
import org.apache.cassandra.autonomic.monitor.sensors.HotPartitionSensor;
import org.apache.cassandra.autonomic.monitor.sensors.PeerSensor;
import org.apache.cassandra.autonomic.monitor.sensors.TableSensor;
import org.apache.cassandra.autonomic.monitor.sensors.ThreadPoolSensor;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.PlanSink;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.autonomic.plan.policies.Policies;
import org.apache.cassandra.autonomic.safety.ClusterGate;
import org.apache.cassandra.autonomic.safety.DefaultSafetyGovernor;
import org.apache.cassandra.autonomic.safety.Reasons;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.LocalizeString;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Entry point of the autonomic layer. Lifecycle: DISABLED, STARTING, RUNNING, PAUSED (overview 3.6).
 * When {@code autonomic.enabled} is false only the MBean is registered and no thread exists.
 *
 * <p>Threads: sensors run on the monitor thread; detectors, policies, the governor's evaluation and all
 * non-critical persistence (diagnoses) on the control thread; actuators on the actuator thread, which does nothing
 * else (see {@link ActionExecutor}). The lock on {@code this} guards lifecycle state only and is never held across
 * a store write or a wait for another autonomic thread once the loops run.
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
    private final BooleanSupplier draining;

    private volatile State state = State.DISABLED;
    private volatile long startedAtMillis;
    private volatile boolean armed;
    // pause requested; also honoured when requested before the loop is armed
    private volatile boolean pauseRequested;
    private volatile String pauseReason;
    // kill(): paused, and everything in flight is rolled back; cleared by resume()
    private volatile boolean killed;
    // shutdown in progress: nothing new is applied while in-flight actions are restored
    private volatile boolean stopping;
    // restart recovery (overview 7.6) has completed; the layer does not arm before
    private volatile boolean recovered;
    private volatile boolean diagnosesSwept;

    public static final String COMPONENT_FAILED = "autonomic.component_failed";
    public static final String LOOP_STALLED = "autonomic.loop_stalled";
    public static final String ACTION_FAILED = "autonomic.action_failed";
    public static final String CIRCUIT_BREAKER = "autonomic.circuit_breaker";
    public static final String POLICY_QUARANTINED = "autonomic.policy_quarantined";
    private static final String ACTUATOR_STALLED_KEY = LOOP_STALLED + ":actuator";
    private static final String KILLED_PREFIX = "KILLED|";
    private static final String PAUSED_PREFIX = "PAUSED|";

    // Static registration (overview 3.4). Detectors and policies of later tasks are added here; tests register
    // fakes through the package-private register* methods.
    private static List<Sensor> productionSensors()
    {
        return Arrays.asList(new TableSensor(), new ClientRequestSensor(), new ThreadPoolSensor(), new CompactionSensor(),
                             new DiskSensor(), new GcSensor(), new CpuSensor(), new PeerSensor(), new HotPartitionSensor());
    }

    private static Correlator productionCorrelator()
    {
        return RuleCorrelator.defaults();
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
    private final List<Detector> detectors = new CopyOnWriteArrayList<>();
    /** Built from the configuration at each start, so nothing exists while the layer is disabled. */
    private volatile List<Detector> builtinDetectors = Collections.emptyList();
    private final List<Policy> policies = new CopyOnWriteArrayList<>();
    private volatile List<Policy> builtinPolicies = Collections.emptyList();
    private volatile Correlator correlator = productionCorrelator();
    private final ActuatorRegistry actuatorRegistry = new ActuatorRegistry();
    /** Plans go through the SafetyGovernor to the ActionExecutor. */
    private final PlanSink executorSink = (plan, knowledge) -> {
        ActionExecutor ex = this.executor;
        if (ex != null)
            ex.accept(plan, knowledge);
    };
    private volatile PlanSink planSink = executorSink;
    private volatile DefaultSafetyGovernor governor;
    private volatile ActionExecutor executor;
    /**
     * This node's side of the cluster ledger. It exists while the layer is disabled too: the operator commands and
     * the ledger views work then. It creates no thread and commits nothing by itself; it is registered as a metadata
     * listener only while the loops run.
     */
    private final ClusterCoordinator coordinator;
    private final ClusterGate clusterGate;
    private volatile ClusterActionExecution clusterExecution;
    /** Actions a shutdown of the loops left applied because their rollback did not complete in time. */
    private final Set<UUID> notRestored = ConcurrentHashMap.newKeySet();
    /** Orders writes of the pause flag; see persistPauseFlag. */
    private final Object pausePersistLock = new Object();
    /** Diagnosis writes, produced on any thread, executed by the control thread. */
    private final ConcurrentLinkedQueue<Runnable> diagnosisWrites = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queuedDiagnosisWrites = new AtomicInteger();
    private static final int MAX_QUEUED_DIAGNOSIS_WRITES = 1024;

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
    /** Null while the layer is disabled; created by startLoops, released by shutdownBlocking. */
    private volatile AutonomicMetrics metrics;

    private ScheduledExecutorPlus monitor;
    private ScheduledExecutorPlus control;
    private ScheduledExecutorPlus actuator;

    private AutonomicService()
    {
        this(org.apache.cassandra.utils.Clock.Global::currentTimeMillis,
             () -> ClusterMetadata.current().myNodeState() == NodeState.JOINED,
             () -> StorageService.instance.isDraining(),
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
        this(clock, nodeJoined, () -> false, knowledgeStore);
    }

    private AutonomicService(LongSupplier clock, BooleanSupplier nodeJoined, BooleanSupplier draining, KnowledgeStore knowledgeStore)
    {
        this.clock = clock;
        this.nodeJoined = nodeJoined;
        this.draining = draining;
        this.knowledgeStore = knowledgeStore;
        this.coordinator = new ClusterCoordinator(new TcmEnvironment(clock, AutonomicService::config));
        this.clusterGate = new LedgerClusterGate(coordinator, name -> {
            Actuator actuator = actuatorRegistry.get(name);
            return actuator instanceof ClusterActuator ? ((ClusterActuator) actuator).lane() : null;
        });
    }

    /** Built when the layer starts, not before: nothing of the actuators exists while the layer is disabled. */
    private void registerProductionActuators(AutonomicConfig config)
    {
        for (Actuator actuator : Actuators.build(config))
            if (actuatorRegistry.get(actuator.name()) == null)
                actuatorRegistry.register(actuator);
    }

    private List<Detector> productionDetectors(AutonomicConfig config)
    {
        List<Detector> out = new ArrayList<>(Detectors.build(config));
        if (Detectors.enabled(config, BaselineDriftDetector.NAME))
            out.add(new BaselineDriftDetector(this::knobActuators, BaselineDriftDetector.LOOKBACK_MILLIS));
        return out;
    }

    private List<KnobActuator> knobActuators()
    {
        List<KnobActuator> knobs = new ArrayList<>();
        for (Actuator a : actuatorRegistry.all())
            if (a instanceof KnobActuator)
                knobs.add((KnobActuator) a);
        return knobs;
    }

    /** The node-local knowledge store; null if this instance does not persist. Usable while the layer is disabled. */
    public KnowledgeStore knowledgeStore()
    {
        return knowledgeStore;
    }

    /** The observation store of the running layer; null while the layer is disabled. */
    public ObservationStore observationStore()
    {
        return state == State.DISABLED ? null : store;
    }

    /** Diagnoses active in memory; empty while the layer is disabled. */
    public List<Diagnosis> activeDiagnoses()
    {
        InMemoryKnowledgeView v = view;
        if (state == State.DISABLED || v == null)
            return Collections.emptyList();
        return new ArrayList<>(v.activeDiagnoses());
    }

    /** Point-in-time copies of the failure accounting; empty while the layer is disabled. */
    public List<ComponentSnapshot> componentSnapshots()
    {
        if (state == State.DISABLED)
            return Collections.emptyList();
        List<ComponentSnapshot> out = new ArrayList<>();
        for (ComponentHealth h : health.values())
            out.add(new ComponentSnapshot(h));
        return out;
    }

    /** Read-only copy of one sensor's, detector's or policy's failure accounting. */
    public static final class ComponentSnapshot
    {
        public final String kind;
        public final String name;
        public final boolean disabled;
        public final int consecutiveFailures;
        public final String lastError;

        ComponentSnapshot(ComponentHealth h)
        {
            this.kind = h.kind;
            this.name = h.name;
            this.disabled = h.disabled;
            this.consecutiveFailures = h.consecutiveFailures;
            this.lastError = h.lastError;
        }
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
        stopping = false;
        startedAtMillis = clock.getAsLong();
        AutonomicConfig config = config();
        metrics = new AutonomicMetrics(this);
        store = new ObservationStore(seriesCapacity(config), config.max_series);
        view = new InMemoryKnowledgeView(store, clock);
        view.setLedgerSource(coordinator::ledger);
        monitorHeartbeat = controlHeartbeat = startedAtMillis;
        detected = Collections.emptyList();
        selfDiagnoses.clear();
        persistedDiagnoses.clear();
        health.clear();
        lastCollected.clear();
        diagnosisWrites.clear();
        queuedDiagnosisWrites.set(0);
        registerProductionActuators(config);
        builtinDetectors = productionDetectors(config);
        builtinPolicies = Policies.build(config, actuatorRegistry::get, name -> actuatorRegistry.isEnabled(name, config()));
        for (Detector detector : builtinDetectors)
            safely("start", detector.name(), detector::start);
        for (Sensor sensor : sensors)
            safely("start", sensor.name(), sensor::start);
        monitor = executorFactory().scheduled(false, MONITOR_THREAD);
        control = executorFactory().scheduled(false, CONTROL_THREAD);
        actuator = executorFactory().scheduled(false, ACTUATOR_THREAD);
        // starts the actuator thread; its work is event-driven
        actuator.execute(() -> {});
        DefaultSafetyGovernor gov = new DefaultSafetyGovernor(new GovernorEnvironment(), new GovernorJournal());
        ExecutorHost host = new ExecutorHost();
        StepScheduler steps = StepScheduler.of(actuator);
        ActionExecutor ex = new ActionExecutor(clock, AutonomicService::config, gov, knowledgeStore, actuatorRegistry,
                                               steps, host, () -> view);
        // cluster-scoped actions: set before recovery, which hands cluster inflight rows to it
        ClusterActionExecution cx = new ClusterActionExecution(clock, AutonomicService::config, coordinator, actuatorRegistry,
                                                               steps, knowledgeStore, host, ex::clusterReporter);
        ex.setClusterExecution(cx);
        view.setActionSource(ex::recentActions);
        governor = gov;
        ex.addListener(metrics);
        executor = ex;
        clusterExecution = cx;
        coordinator.setPhaseListener(cx);
        setLedgerListener(true);
        cx.start();
        // STARTING: recovery runs and the persisted pause flag is read before either loop exists (overview 3.6);
        // if the store cannot be read yet the control thread retries and the layer stays unarmed
        diagnosesSwept = false;
        if (knowledgeStore == null && !notRestored.isEmpty())
        {
            // no store to record it in: an earlier stop in this process left changes applied
            raiseSelfDiagnosis(ACTION_FAILED, ACTION_FAILED, Severity.CRITICAL,
                               "the layer was stopped while actions were applied and their rollback did not complete",
                               Map.of("actions", notRestored.toString()), startedAtMillis);
            selfPause(Reasons.ACTION_FAILED);
        }
        recovered = runRecovery();
        monitor.scheduleWithFixedDelay(this::monitorTick, config.monitor_interval.toMilliseconds(), config.monitor_interval.toMilliseconds(), TimeUnit.MILLISECONDS);
        control.scheduleWithFixedDelay(this::controlTick, config.analyze_interval.toMilliseconds(), config.analyze_interval.toMilliseconds(), TimeUnit.MILLISECONDS);
        logger.info("Autonomic layer started (dry_run={})", config.dry_run);
    }

    /** Phase changes of the ledger are dispatched only while the loops run (overview 3.6: no listeners when disabled). */
    private void setLedgerListener(boolean registered)
    {
        try
        {
            ClusterMetadataService service = ClusterMetadataService.instance();
            if (service == null)
                return;
            service.log().removeListener(coordinator);
            if (registered)
                service.log().addListener(coordinator);
        }
        catch (Throwable t)
        {
            handleThrowable("ledger listener registration", t);
        }
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
        long began = nanoTime();
        try
        {
            ObservationStore store = this.store;
            if (state == State.DISABLED || store == null)
                return;
            long now = clock.getAsLong();
            monitorHeartbeat = now;
            long budget = config().safety.sensor_budget.toMilliseconds();
            ObservationSink sink = o -> {
                store.accept(o);
                AutonomicMetrics m = metrics;
                if (m != null)
                    m.observationAccepted();
            };
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
                long collectBegin = nanoTime();
                try
                {
                    sensor.collect(sink, now);
                }
                catch (Throwable t)
                {
                    recordFailure(h, t);
                    continue;
                }
                finally
                {
                    AutonomicMetrics m = metrics;
                    if (m != null)
                        m.sensorCollected(sensor.name(), nanoTime() - collectBegin);
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
        finally
        {
            AutonomicMetrics m = metrics;
            if (m != null)
                m.monitorTick(nanoTime() - began);
        }
    }

    /** Runs on the AutonomicControl thread. Must never throw. */
    @VisibleForTesting
    void controlTick()
    {
        long began = nanoTime();
        try
        {
            InMemoryKnowledgeView view = this.view;
            if (state == State.DISABLED || view == null)
                return;
            long now = clock.getAsLong();
            controlHeartbeat = now;
            checkHeartbeat("monitor", monitorHeartbeat, config().monitor_interval.toMilliseconds(), now);
            if (!recovered)
                recovered = runRecovery();
            if (!evaluateArmed())
                return;
            superviseExecution(view, now);

            List<Diagnosis> raw = new ArrayList<>();
            List<Detector> allDetectors = new ArrayList<>(builtinDetectors);
            allDetectors.addAll(detectors);
            for (Detector detector : allDetectors)
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
            List<Policy> allPolicies = new ArrayList<>(builtinPolicies);
            allPolicies.addAll(policies);
            for (Diagnosis diagnosis : correlated)
            {
                // an effect with a root cause is recorded but not acted on: the policies get the root
                if (diagnosis.rootCause != null)
                    continue;
                for (Policy policy : allPolicies)
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
        finally
        {
            drainDiagnosisWrites();
            AutonomicMetrics m = metrics;
            if (m != null)
                m.controlTick(nanoTime() - began, state);
        }
    }

    /**
     * Control thread: verification triggers, early abort and proposal expiry (delegated to the executor), detection
     * of a stalled actuator thread (overview 7.5), and the end of policy quarantines.
     */
    private void superviseExecution(KnowledgeView view, long now)
    {
        ActionExecutor ex = executor;
        DefaultSafetyGovernor gov = governor;
        if (ex == null || gov == null)
            return;
        ex.onControlTick(view);
        String stalled = ex.stalledStep(config().safety.action_step_timeout.toMilliseconds());
        if (stalled != null && !selfDiagnoses.containsKey(ACTUATOR_STALLED_KEY))
        {
            raiseSelfDiagnosis(ACTUATOR_STALLED_KEY, LOOP_STALLED, Severity.CRITICAL,
                               "actuator thread has been in step '" + stalled + "' for longer than action_step_timeout",
                               Map.of("loop", "actuator", "step", stalled), now);
            selfPause(Reasons.ACTUATOR_STALLED);
        }
        Map<String, Long> quarantined = gov.quarantines();
        String prefix = POLICY_QUARANTINED + ':';
        if (selfDiagnoses.keySet().removeIf(k -> k.startsWith(prefix) && !quarantined.containsKey(k.substring(prefix.length()))))
            refreshView();
    }

    /**
     * Restart recovery (overview 7.6) and the persisted pause flag. Runs on the starting thread before the loops
     * exist, then on the control thread until it has completed. Returns false if it must run again.
     */
    private boolean runRecovery()
    {
        ActionExecutor ex = executor;
        if (ex == null)
            return false;
        ActionExecutor.Recovery recovery;
        try
        {
            recovery = ex.recover(notRestored);
        }
        catch (Throwable t)
        {
            handleThrowable("restart recovery", t);
            return false;
        }
        if (!recovery.scanned)
        {
            logger.warn("Autonomic restart recovery could not read local_state; the layer stays unarmed until it can");
            return false;
        }
        if (recovery.paused != null)
        {
            synchronized (this)
            {
                pauseRequested = true;
                killed |= recovery.paused.startsWith(KILLED_PREFIX);
                int sep = recovery.paused.indexOf('|');
                pauseReason = sep < 0 || sep == recovery.paused.length() - 1 ? null : recovery.paused.substring(sep + 1);
                if (state == State.RUNNING)
                    state = State.PAUSED;
            }
            logger.info("Autonomic layer is paused by a persisted flag: {}", recovery.paused);
        }
        if (recovery.rolledBack > 0)
            logger.info("Autonomic restart recovery closed {} in-flight action(s)", recovery.rolledBack);
        sweepDiagnoses();
        if (recovery.complete)
            notRestored.clear();
        return recovery.complete;
    }

    /** Marks diagnoses a previous run left OPEN as cleared; once per start, best effort. */
    private void sweepDiagnoses()
    {
        if (knowledgeStore == null || diagnosesSwept)
            return;
        diagnosesSwept = true;
        try
        {
            long now = clock.getAsLong();
            long since = now - TimeUnit.SECONDS.toMillis(Math.min(config().history_ttl.toSeconds(), TimeUnit.DAYS.toSeconds(31)));
            int swept = 0;
            for (KnowledgeStore.StoredDiagnosis stored : knowledgeStore.recentDiagnoses(since, now, 10_000))
            {
                Diagnosis d = stored.diagnosis;
                if (stored.cleared || d.lastSeenMillis >= startedAtMillis)
                    continue;
                Map<String, String> evidence = new LinkedHashMap<>(d.evidence);
                evidence.put("cleared_reason", Reasons.RESTART);
                knowledgeStore.clearDiagnosis(new Diagnosis(d.id, d.type, d.subject, d.severity, d.confidence, d.firstSeenMillis,
                                                            d.lastSeenMillis, d.summary, evidence, d.rootCause), now);
                swept++;
            }
            if (swept > 0)
                logger.info("Autonomic layer cleared {} diagnosis row(s) left open by a previous run", swept);
        }
        catch (Throwable t)
        {
            handleThrowable("diagnosis sweep", t);
        }
    }

    private void drainDiagnosisWrites()
    {
        Runnable write;
        while ((write = diagnosisWrites.poll()) != null)
        {
            queuedDiagnosisWrites.decrementAndGet();
            try
            {
                write.run();
            }
            catch (Throwable t)
            {
                handleThrowable("diagnosis write", t);
            }
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
        AutonomicMetrics m = metrics;
        if (m != null && "sensor".equals(h.kind))
            m.sensorFailed(h.name);
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
        AutonomicMetrics m = metrics;
        if (m != null)
            m.diagnoses(all);
        persistDiagnoses(all);
    }

    /**
     * Queues writes for diagnoses that opened, changed severity or root cause, or cleared since the last call. The
     * control thread executes them at the end of its tick (overview 3.5): never the monitor thread, which must not do
     * I/O, and never the actuator thread, where a slow write could delay a rollback.
     */
    private void persistDiagnoses(List<Diagnosis> active)
    {
        if (knowledgeStore == null)
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
        for (Runnable write : writes)
        {
            if (queuedDiagnosisWrites.incrementAndGet() > MAX_QUEUED_DIAGNOSIS_WRITES)
            {
                queuedDiagnosisWrites.decrementAndGet();
                logger.debug("Autonomic diagnosis write dropped: the control thread is not draining them");
                continue;
            }
            diagnosisWrites.add(write);
        }
    }

    /** Public only for distributed tests, which cannot reach package-private members. */
    @VisibleForTesting
    public void registerSensor(Sensor sensor)
    {
        sensors.add(sensor);
    }

    /** As {@link #registerSensor}: lets a distributed test replace a production sensor with an injected one. */
    @VisibleForTesting
    public void removeSensor(String name)
    {
        sensors.removeIf(s -> s.name().equals(name));
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

    /**
     * Registration is static (overview 3.4); this is public only so that distributed tests, which cannot reach
     * package-private members, can register a fake. Must be called before the layer is enabled.
     */
    @VisibleForTesting
    public void registerActuator(Actuator actuator)
    {
        actuatorRegistry.register(actuator);
    }

    /** Replaces the hand-over to the SafetyGovernor and ActionExecutor. */
    @VisibleForTesting
    void setPlanSink(PlanSink sink)
    {
        this.planSink = sink;
    }

    @VisibleForTesting
    ActionExecutor executor()
    {
        return executor;
    }

    @VisibleForTesting
    DefaultSafetyGovernor governor()
    {
        return governor;
    }

    @VisibleForTesting
    AutonomicMetrics metrics()
    {
        return metrics;
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
            if (!recovered || clock.getAsLong() - startedAtMillis < graceMillis || !nodeJoined.getAsBoolean())
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

    /**
     * Stops the loops and releases all threads; the MBean stays registered so the layer can be re-enabled. In-flight
     * node-scoped actions are rolled back first: the process may live on (runtime disable), in which case nothing
     * else would restore them.
     */
    public void shutdownBlocking(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        // FC1: stop the always-on data-plane service here.
        ActionExecutor ex;
        synchronized (this)
        {
            if (state == State.DISABLED)
                return;
            stopping = true;
            ex = executor;
        }
        if (ex != null)
        {
            // not under the lock: the rollback may raise a diagnosis or pause the layer
            long waitMillis = Math.min(unit.toMillis(timeout), config().safety.action_step_timeout.toMilliseconds());
            try
            {
                ex.rollbackAll(Reasons.SHUTDOWN).get(waitMillis, TimeUnit.MILLISECONDS);
            }
            catch (ExecutionException | TimeoutException e)
            {
                logger.error("Autonomic layer could not roll back in-flight actions while stopping", e);
            }
            Set<UUID> applied = ex.appliedActions();
            if (!applied.isEmpty())
            {
                logger.error("Autonomic layer stopped with actions still applied: {}; they are reported as FAILED when it is next enabled", applied);
                notRestored.addAll(applied);
            }
        }
        synchronized (this)
        {
            if (state == State.DISABLED)
                return;
            state = State.DISABLED;
            armed = false;
            ClusterActionExecution cx = clusterExecution;
            if (cx != null)
                cx.stop();
            clusterExecution = null;
            coordinator.setPhaseListener(null);
            setLedgerListener(false);
            List<ScheduledExecutorPlus> executors = new ArrayList<>();
            executors.add(monitor);
            executors.add(control);
            executors.add(actuator);
            monitor = control = actuator = null;
            executor = null;
            governor = null;
            ExecutorUtils.shutdownNowAndWait(timeout, unit, executors);
            for (Detector detector : builtinDetectors)
                safely("stop", detector.name(), detector::stop);
            for (Sensor sensor : sensors)
                safely("stop", sensor.name(), sensor::stop);
            AutonomicMetrics m = metrics;
            metrics = null;
            if (m != null)
                m.release();
            stopping = false;
            logger.info("Autonomic layer stopped");
        }
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
        status.put("killed", String.valueOf(killed));
        status.put("recovered", String.valueOf(recovered));
        ActionExecutor ex = executor;
        DefaultSafetyGovernor gov = governor;
        if (ex != null && gov != null)
        {
            int awaitingApproval = 0;
            int inFlight = 0;
            for (ActionRecord r : ex.liveActions())
            {
                if (r.state == org.apache.cassandra.autonomic.execute.ActionState.PROPOSED)
                    awaitingApproval++;
                else
                    inFlight++;
            }
            status.put("in_flight_actions", String.valueOf(inFlight));
            status.put("awaiting_approval", String.valueOf(awaitingApproval));
            status.put("cooldowns", String.valueOf(gov.cooldowns().size()));
            status.put("quarantined_policies", String.join(",", gov.quarantines().keySet()));
            String stalled = ex.stalledStep(config().safety.action_step_timeout.toMilliseconds());
            if (stalled != null)
                status.put("actuator_stalled_in", stalled);
        }
        if (knowledgeStore != null)
            status.put("store_available", String.valueOf(knowledgeStore.isAvailable()));
        status.put("cluster_actions_enabled", String.valueOf(coordinator.enabled()));
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
    public void pause(String reason)
    {
        synchronized (this)
        {
            if (state == State.DISABLED)
                throw new IllegalStateException("Autonomic layer is not enabled");
            pauseRequested = true;
            pauseReason = reason;
            if (state == State.RUNNING)
                state = State.PAUSED;
        }
        persistPauseFlag();
    }

    /** A pause the layer imposes on itself (overview 7.5); it lasts until an operator resumes. Any thread. */
    private void selfPause(String reason)
    {
        synchronized (this)
        {
            if (state == State.DISABLED || pauseRequested)
                return;
            pauseRequested = true;
            pauseReason = reason;
            if (state == State.RUNNING)
                state = State.PAUSED;
        }
        logger.warn("Autonomic layer paused itself: {}", reason);
        persistPauseFlag();
    }

    @Override
    public void kill(String reason)
    {
        ActionExecutor ex;
        synchronized (this)
        {
            if (state == State.DISABLED)
                throw new IllegalStateException("Autonomic layer is not enabled");
            killed = true;
            pauseRequested = true;
            pauseReason = reason;
            if (state == State.RUNNING)
                state = State.PAUSED;
            ex = executor;
        }
        logger.warn("Autonomic layer killed: {}", reason);
        // the rollback is requested before the flag is written: a slow store must not delay it
        if (ex != null)
            ex.rollbackAll(Reasons.KILL);
        persistPauseFlag();
    }

    @Override
    public void resume()
    {
        DefaultSafetyGovernor gov;
        synchronized (this)
        {
            if (state == State.DISABLED)
                throw new IllegalStateException("Autonomic layer is not enabled");
            pauseRequested = false;
            killed = false;
            pauseReason = null;
            if (state == State.PAUSED)
                state = State.RUNNING;
            for (ComponentHealth h : health.values())
            {
                h.disabled = false;
                h.consecutiveFailures = 0;
            }
            selfDiagnoses.keySet().removeIf(k -> k.startsWith(COMPONENT_FAILED) || k.startsWith(ACTION_FAILED)
                                                 || k.startsWith(CIRCUIT_BREAKER) || k.equals(ACTUATOR_STALLED_KEY));
            refreshView();
            gov = governor;
        }
        if (gov != null)
            gov.resetBreaker();
        persistPauseFlag();
    }

    /**
     * Writes the current pause state to {@code local_state} so that a restart does not re-arm the layer. Writers are
     * serialised by {@link #pausePersistLock}, each writing the state it reads after acquiring it, so the last write
     * matches memory. Must not be called holding {@code this} once the loops run. A failed write is only logged:
     * the pause is effective in memory.
     */
    private void persistPauseFlag()
    {
        if (knowledgeStore == null)
            return;
        synchronized (pausePersistLock)
        {
            boolean ok;
            if (pauseRequested)
            {
                String reason = pauseReason;
                ok = knowledgeStore.putState(ActionExecutor.PAUSED_KEY, (killed ? KILLED_PREFIX : PAUSED_PREFIX) + (reason == null ? "" : reason), clock.getAsLong());
            }
            else
            {
                ok = knowledgeStore.deleteState(ActionExecutor.PAUSED_KEY);
            }
            if (!ok)
                logger.warn("Autonomic pause state could not be persisted; it will not survive a restart");
        }
    }

    /** Check 1 of the governor: null if actions may be approved and applied. */
    private String gate()
    {
        if (state == State.DISABLED || stopping)
            return Reasons.NOT_ARMED;
        if (killed)
            return Reasons.KILLED;
        if (pauseRequested || state == State.PAUSED)
            return Reasons.PAUSED;
        return armed && state == State.RUNNING ? null : Reasons.NOT_ARMED;
    }

    private final class GovernorEnvironment implements DefaultSafetyGovernor.Environment
    {
        @Override
        public AutonomicConfig config()
        {
            return AutonomicService.config();
        }

        @Override
        public long nowMillis()
        {
            return clock.getAsLong();
        }

        @Override
        public String layerGate()
        {
            return gate();
        }

        @Override
        public Actuator actuator(String name)
        {
            return actuatorRegistry.get(name);
        }

        @Override
        public boolean actuatorEnabled(String name)
        {
            return actuatorRegistry.isEnabled(name, AutonomicService.config());
        }

        @Override
        public boolean nodeReady()
        {
            return nodeJoined.getAsBoolean() && !draining.getAsBoolean();
        }

        @Override
        public boolean storeAvailable()
        {
            return knowledgeStore == null || knowledgeStore.isAvailable();
        }

        @Override
        public ClusterGate clusterGate()
        {
            return clusterGate;
        }
    }

    private final class GovernorJournal implements DefaultSafetyGovernor.Journal
    {
        @Override
        public void putState(String key, String value)
        {
            if (knowledgeStore != null)
                knowledgeStore.putState(key, value, clock.getAsLong());
        }

        @Override
        public void deleteState(String key)
        {
            if (knowledgeStore != null)
                knowledgeStore.deleteState(key);
        }

        @Override
        public void quarantined(String policy, long untilMillis)
        {
            logger.warn("Autonomic policy {} quarantined until {}", policy, untilMillis);
            raiseSelfDiagnosis(POLICY_QUARANTINED + ':' + policy, POLICY_QUARANTINED, Severity.WARN,
                               "policy " + policy + " is quarantined after repeated rollbacks",
                               Map.of("policy", policy, "until_millis", String.valueOf(untilMillis)), clock.getAsLong());
        }

        @Override
        public void breakerTripped(int rollbacks, int window)
        {
            raiseSelfDiagnosis(CIRCUIT_BREAKER, CIRCUIT_BREAKER, Severity.CRITICAL,
                               rollbacks + " of the last " + window + " actions rolled back or failed",
                               Map.of("rollbacks", String.valueOf(rollbacks), "window", String.valueOf(window)), clock.getAsLong());
            selfPause(Reasons.CIRCUIT_BREAKER);
        }
    }

    private final class ExecutorHost implements ActionExecutor.Host
    {
        @Override
        public String gate()
        {
            return AutonomicService.this.gate();
        }

        @Override
        public boolean killed()
        {
            return killed || stopping || state == State.DISABLED;
        }

        @Override
        public void actionFailed(ActionRecord record, String detail)
        {
            raiseSelfDiagnosis(ACTION_FAILED + ':' + record.id(), ACTION_FAILED, Severity.CRITICAL, detail,
                               Map.of("action", record.id().toString(), "actuator", record.action.actuator,
                                      "target", record.action.target.asString()), clock.getAsLong());
            selfPause(Reasons.ACTION_FAILED);
        }
    }

    // ---- operator interface ----

    @Override
    public void revert()
    {
        // overview 7.3 rule 11; an action in flight on a reverted knob ends SUPERSEDED, as for any operator change
        List<String> failed = new ArrayList<>();
        for (KnobActuator knob : knobActuators())
        {
            try
            {
                if (knob.revert())
                    logger.info("Autonomic revert: {} restored to {}", knob.name(), KnobActuator.format(knob.configured()));
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.warn("Autonomic revert of {} failed", knob.name(), t);
                failed.add(knob.name());
            }
        }
        if (!failed.isEmpty())
            throw new IllegalStateException("revert failed for " + failed);
    }

    @Override
    public void approve(String actionId)
    {
        requireExecutor().approve(UUID.fromString(actionId));
    }

    @Override
    public void reject(String actionId)
    {
        requireExecutor().reject(UUID.fromString(actionId));
    }

    private ActionExecutor requireExecutor()
    {
        ActionExecutor ex = executor;
        if (ex == null)
            throw new IllegalStateException("Autonomic layer is not enabled");
        return ex;
    }

    @Override
    public void setActuatorEnabled(String actuator, boolean enabled)
    {
        actuatorRegistry.setEnabled(actuator, enabled);
    }

    @Override
    public Map<String, String> getActuators()
    {
        Map<String, String> out = new LinkedHashMap<>();
        for (Actuator a : actuatorRegistry.all())
            out.put(a.name(), a.scope() + (a.reversible() ? "" : " irreversible") + (actuatorRegistry.isEnabled(a.name(), config()) ? " enabled" : " disabled"));
        return out;
    }

    @Override
    public List<Map<String, String>> getInFlightActions()
    {
        List<Map<String, String>> out = new ArrayList<>();
        ActionExecutor ex = executor;
        if (ex != null)
            for (ActionRecord r : ex.liveActions())
                out.add(describe(r));
        return out;
    }

    @Override
    public List<Map<String, String>> getHistory(int limit)
    {
        long now = clock.getAsLong();
        List<ActionRecord> records;
        if (knowledgeStore != null)
        {
            records = knowledgeStore.recentActions(now - TimeUnit.DAYS.toMillis(7), now, Math.max(0, limit));
        }
        else
        {
            ActionExecutor ex = executor;
            records = ex == null ? new ArrayList<>() : new ArrayList<>(ex.recentActions(TimeUnit.DAYS.toMillis(7)));
            records.sort((a, b) -> Long.compare(b.updatedMillis, a.updatedMillis));
        }
        List<Map<String, String>> out = new ArrayList<>();
        for (ActionRecord r : records)
            if (out.size() < limit)
                out.add(describe(r));
        return out;
    }

    @Override
    public List<Map<String, String>> getActionEvents(String actionId)
    {
        List<Map<String, String>> out = new ArrayList<>();
        if (knowledgeStore == null)
            return out;
        for (KnowledgeStore.ActionEvent e : knowledgeStore.actionEvents(UUID.fromString(actionId)))
        {
            Map<String, String> m = new LinkedHashMap<>();
            m.put("at", String.valueOf(TimeUUID.fromUuid(e.at).unix(TimeUnit.MILLISECONDS)));
            m.put("from_state", String.valueOf(e.fromState));
            m.put("to_state", e.toState);
            m.put("detail", e.detail == null ? "" : e.detail);
            out.add(m);
        }
        return out;
    }

    @Override
    public List<Map<String, String>> getDiagnosisHistory(int limit)
    {
        List<Map<String, String>> out = new ArrayList<>();
        long now = clock.getAsLong();
        if (knowledgeStore != null)
        {
            for (KnowledgeStore.StoredDiagnosis stored : knowledgeStore.recentDiagnoses(now - TimeUnit.DAYS.toMillis(1), now, Math.max(0, limit)))
                out.add(describe(stored.diagnosis, stored.cleared));
            return out;
        }
        InMemoryKnowledgeView v = view;
        if (v != null)
            for (Diagnosis d : v.activeDiagnoses())
                if (out.size() < limit)
                    out.add(describe(d, false));
        return out;
    }

    // ---- cluster ledger (overview 6.5, 10.2) ----

    @VisibleForTesting
    ClusterCoordinator coordinator()
    {
        return coordinator;
    }

    @VisibleForTesting
    ClusterActionExecution clusterExecution()
    {
        return clusterExecution;
    }

    @Override
    public void enableClusterActions(boolean enabled)
    {
        if (coordinator.setEnabled(enabled))
            logger.warn("Autonomic cluster actions {} by operator command", enabled ? "enabled" : "disabled");
    }

    @Override
    public boolean isClusterActionsEnabled()
    {
        return coordinator.enabled();
    }

    @Override
    public String releaseClaim(String lane)
    {
        Lane l;
        try
        {
            l = Lane.valueOf(LocalizeString.toUpperCaseLocalized(lane.trim(), java.util.Locale.ROOT));
        }
        catch (RuntimeException e)
        {
            throw new IllegalStateException("Unknown lane '" + lane + "'; lanes are " + Arrays.toString(Lane.values()));
        }
        UUID released = coordinator.forceRelease(l);
        logger.warn("Autonomic claim {} on lane {} released by operator command", released, l);
        return released.toString();
    }

    @Override
    public Map<String, String> getClusterStatus()
    {
        Map<String, String> status = new LinkedHashMap<>();
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        ClusterLedger ledger = ClusterLedger.of(metadata);
        long now = clock.getAsLong();
        status.put("cluster_actions_enabled", String.valueOf(coordinator.enabled()));
        status.put("ledger_present", String.valueOf(ClusterLedger.present(metadata)));
        status.put("ledger_format", String.valueOf(ledger.formatVersion));
        status.put("epoch", metadata == null ? "" : String.valueOf(metadata.epoch.getEpoch()));
        status.put("node_id", coordinator.self() == null ? "" : String.valueOf(coordinator.self().id()));
        status.put("preferred_proposer", String.valueOf(coordinator.preferredProposer()));
        status.put("recent_actions", String.valueOf(ledger.recent.size()));
        for (Lane lane : Lane.values())
        {
            Claim claim = ledger.claims.get(lane);
            status.put("lane." + lane, claim == null ? "free"
                                                     : claim.actuator + ' ' + claim.target + " action " + claim.actionId + " owner " + claim.owner.id()
                                                       + " phase " + claim.phase + " step " + claim.step
                                                       + (claim.leaseExpiresAtMillis < now ? " lease EXPIRED at " : " lease until ") + claim.leaseExpiresAtMillis);
        }
        return status;
    }

    @Override
    public List<Map<String, String>> getClusterClaims()
    {
        List<Map<String, String>> out = new ArrayList<>();
        long now = clock.getAsLong();
        for (Map.Entry<Lane, Claim> e : coordinator.ledger().claims.entrySet())
        {
            Claim claim = e.getValue();
            Map<String, String> m = new LinkedHashMap<>();
            m.put("lane", e.getKey().name());
            m.put("action_id", claim.actionId.toString());
            m.put("actuator", claim.actuator);
            m.put("target", claim.target);
            m.put("owner", String.valueOf(claim.owner.id()));
            m.put("phase", claim.phase);
            m.put("step", String.valueOf(claim.step));
            m.put("claimed_at", String.valueOf(claim.claimedAtMillis));
            m.put("lease_expires_at", String.valueOf(claim.leaseExpiresAtMillis));
            m.put("expired", String.valueOf(claim.leaseExpiresAtMillis < now));
            m.put("state", claim.state.toString());
            out.add(m);
        }
        return out;
    }

    @Override
    public List<Map<String, String>> getClusterRecentActions()
    {
        List<Map<String, String>> out = new ArrayList<>();
        for (Recent r : coordinator.ledger().recent)
        {
            Map<String, String> m = new LinkedHashMap<>();
            m.put("lane", r.lane.name());
            m.put("action_id", r.actionId.toString());
            m.put("actuator", r.actuator);
            m.put("target", r.target);
            m.put("final_state", r.finalState);
            m.put("ended_at", String.valueOf(r.endedAtMillis));
            out.add(m);
        }
        return out;
    }

    private static Map<String, String> describe(ActionRecord r)
    {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("id", r.id().toString());
        m.put("state", r.state.name());
        m.put("state_reason", r.stateReason == null ? "" : r.stateReason);
        m.put("actuator", r.action.actuator);
        m.put("scope", r.action.scope.name());
        m.put("target", r.action.target.asString());
        m.put("params", r.action.params.toString());
        m.put("policy", r.policy == null ? "" : r.policy);
        m.put("diagnosis_id", r.diagnosisId == null ? "" : r.diagnosisId.toString());
        m.put("dry_run", String.valueOf(r.dryRun));
        m.put("verdict", r.verdict == null ? "" : r.verdict.name());
        m.put("baseline", r.baseline.toString());
        m.put("result", r.result.toString());
        m.put("proposed_at", String.valueOf(r.proposedMillis));
        m.put("updated_at", String.valueOf(r.updatedMillis));
        return m;
    }

    private static Map<String, String> describe(Diagnosis d, boolean cleared)
    {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("id", d.id.toString());
        m.put("type", d.type);
        m.put("subject", d.subject.asString());
        m.put("severity", d.severity.name());
        m.put("state", cleared ? KnowledgeStore.STATE_CLEARED : KnowledgeStore.STATE_OPEN);
        m.put("first_seen", String.valueOf(d.firstSeenMillis));
        m.put("last_seen", String.valueOf(d.lastSeenMillis));
        m.put("summary", d.summary == null ? "" : d.summary);
        return m;
    }
}
