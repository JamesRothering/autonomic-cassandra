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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.execute.ActionExecutor;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.knowledge.KnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics of the autonomic layer itself (overview 10.3), type {@code Autonomic}. Created when the layer starts and
 * released when it stops, so a disabled layer registers nothing. Recording never throws into a tick.
 *
 * <p>Also the one {@link ActionExecutor.TransitionListener} of the executor: it counts terminal states, rollbacks and
 * rejections, and publishes {@link AutonomicEvent}s. Diagnosis transitions are counted from
 * {@link AutonomicService}'s active list, on the thread that refreshes it.
 */
public final class AutonomicMetrics implements ActionExecutor.TransitionListener
{
    public static final String TYPE_NAME = "Autonomic";
    private static final Logger logger = LoggerFactory.getLogger(AutonomicMetrics.class);
    private static final String UNKNOWN = "unknown";

    private final AutonomicService service;
    /** Every name registered by this instance, removed again by {@link #release()}. */
    private final ConcurrentLinkedQueue<MetricName> names = new ConcurrentLinkedQueue<>();

    private final Timer monitorTickDuration;
    private final Timer controlTickDuration;
    private final Counter observationsAccepted;
    private final Counter rollbacks;
    private final Map<ActionState, Counter> terminal = new EnumMap<>(ActionState.class);

    private final Map<String, Timer> sensorDuration = new ConcurrentHashMap<>();
    private final Map<String, Counter> sensorFailures = new ConcurrentHashMap<>();
    private final Map<String, Counter> diagnosesOpened = new ConcurrentHashMap<>();
    private final Map<String, Counter> diagnosesCleared = new ConcurrentHashMap<>();
    private final Map<String, Counter> actionsByActuator = new ConcurrentHashMap<>();
    private final Map<String, Counter> rejectionsByReason = new ConcurrentHashMap<>();

    /** Diagnoses active as of the last refresh, by id; guarded by {@code this}. */
    private final Map<UUID, Diagnosis> open = new HashMap<>();
    /** Last state published as LAYER_STATE; written by the control thread only. */
    private AutonomicService.State lastState;

    // TODO T2.8: ledger commit latency and ledger rejections

    AutonomicMetrics(AutonomicService service)
    {
        this.service = service;
        monitorTickDuration = Metrics.timer(name("MonitorTickDuration", null, null));
        controlTickDuration = Metrics.timer(name("ControlTickDuration", null, null));
        observationsAccepted = counter("ObservationsAccepted", null, null);
        rollbacks = counter("Rollbacks", null, null);
        for (ActionState s : ActionState.values())
            if (s.isTerminal())
                terminal.put(s, counter("ActionsTerminal", "state", s.name()));

        gauge("SeriesCount", new Gauge<Integer>()
        {
            public Integer getValue()
            {
                ObservationStore store = service.observationStore();
                return store == null ? 0 : store.seriesCount();
            }
        });
        gauge("SeriesEvictions", new Gauge<Long>()
        {
            public Long getValue()
            {
                ObservationStore store = service.observationStore();
                return store == null ? 0L : store.evictions();
            }
        });
        gauge("StoreWriteFailures", new Gauge<Long>()
        {
            public Long getValue()
            {
                KnowledgeStore store = service.knowledgeStore();
                return store == null ? 0L : store.writeFailures();
            }
        });
        gauge("StoreReadFailures", new Gauge<Long>()
        {
            public Long getValue()
            {
                KnowledgeStore store = service.knowledgeStore();
                return store == null ? 0L : store.readFailures();
            }
        });
        gauge("LayerState", new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return service.state().ordinal();
            }
        });
    }

    // ---------------------------------------------------------------------------------------------------------
    // recording: never throws
    // ---------------------------------------------------------------------------------------------------------

    /** Monitor thread, end of every tick. */
    void monitorTick(long nanos)
    {
        guard(() -> monitorTickDuration.update(nanos, TimeUnit.NANOSECONDS));
    }

    /** Control thread, end of every tick; also publishes LAYER_STATE when the state differs from the last one seen. */
    void controlTick(long nanos, AutonomicService.State state)
    {
        guard(() -> controlTickDuration.update(nanos, TimeUnit.NANOSECONDS));
        guard(() -> {
            if (state != null && state != lastState)
            {
                lastState = state;
                AutonomicEvent.publishLayerState(state);
            }
        });
    }

    /** Monitor thread, after one sensor collect, successful or not. */
    void sensorCollected(String sensor, long nanos)
    {
        guard(() -> sensorDuration.computeIfAbsent(sensor, s -> timer("SensorCollectDuration", "sensor", s))
                                  .update(nanos, TimeUnit.NANOSECONDS));
    }

    /** A sensor failed or overran its budget (counted once per failed collect). */
    void sensorFailed(String sensor)
    {
        guard(() -> sensorFailures.computeIfAbsent(sensor, s -> counter("SensorFailures", "sensor", s)).inc());
    }

    void observationAccepted()
    {
        guard(observationsAccepted::inc);
    }

    /** Called with the diagnoses active after each refresh; diffs them against the previous refresh. */
    synchronized void diagnoses(List<Diagnosis> active)
    {
        Map<UUID, Diagnosis> now = new HashMap<>();
        for (Diagnosis d : active)
            now.put(d.id, d);
        for (Diagnosis d : now.values())
            if (!open.containsKey(d.id))
                guard(() -> opened(d));
        for (Diagnosis d : open.values())
            if (!now.containsKey(d.id))
                guard(() -> cleared(d));
        open.clear();
        open.putAll(now);
    }

    private void opened(Diagnosis d)
    {
        diagnosesOpened.computeIfAbsent(d.type, t -> counter("DiagnosesOpened", "diagnosis", t)).inc();
        logger.info("Autonomic diagnosis {} opened: {} on {} severity {}: {}", d.id, d.type, d.subject, d.severity, d.summary);
        AutonomicEvent.publishDiagnosis(AutonomicEvent.AutonomicEventType.DIAGNOSIS_OPENED, d);
    }

    private void cleared(Diagnosis d)
    {
        diagnosesCleared.computeIfAbsent(d.type, t -> counter("DiagnosesCleared", "diagnosis", t)).inc();
        logger.info("Autonomic diagnosis {} cleared: {} on {}", d.id, d.type, d.subject);
        AutonomicEvent.publishDiagnosis(AutonomicEvent.AutonomicEventType.DIAGNOSIS_CLEARED, d);
    }

    /** Any thread: the executor calls this for every action transition, after logging it. */
    @Override
    public void onTransition(ActionRecord record, ActionState from, String detail)
    {
        guard(() -> {
            AutonomicEvent.publishAction(record, from, detail);
            if (record.state == ActionState.ROLLING_BACK)
                rollbacks.inc();
            if (record.state.isTerminal())
            {
                terminal.get(record.state).inc();
                actionsByActuator.computeIfAbsent(record.action.actuator, a -> counter("ActionsTerminalByActuator", "actuator", a)).inc();
            }
            if (record.state == ActionState.REJECTED)
                rejectionsByReason.computeIfAbsent(record.stateReason == null ? UNKNOWN : record.stateReason,
                                                   r -> counter("GovernorRejections", "reason", r)).inc();
        });
    }

    // ---------------------------------------------------------------------------------------------------------
    // registration
    // ---------------------------------------------------------------------------------------------------------

    /** Removes every metric this instance registered; called when the layer stops. */
    void release()
    {
        MetricName n;
        while ((n = names.poll()) != null)
            Metrics.remove(n);
    }

    /** Test access: the value of a lazily created counter, 0 if none exists yet. */
    @VisibleForTesting
    long count(String family, String value)
    {
        Map<String, Counter> map;
        switch (family)
        {
            case "DiagnosesOpened": map = diagnosesOpened; break;
            case "DiagnosesCleared": map = diagnosesCleared; break;
            case "SensorFailures": map = sensorFailures; break;
            case "GovernorRejections": map = rejectionsByReason; break;
            case "ActionsTerminalByActuator": map = actionsByActuator; break;
            default: throw new IllegalArgumentException(family);
        }
        Counter c = map.get(value);
        return c == null ? 0 : c.getCount();
    }

    /** Test access: the counter of a terminal state. */
    @VisibleForTesting
    long terminal(ActionState state)
    {
        return terminal.get(state).getCount();
    }

    long observationsAccepted()
    {
        return observationsAccepted.getCount();
    }

    long rollbacks()
    {
        return rollbacks.getCount();
    }

    long monitorTicks()
    {
        return monitorTickDuration.getCount();
    }

    long controlTicks()
    {
        return controlTickDuration.getCount();
    }

    private Counter counter(String metric, String key, String value)
    {
        MetricName n = name(metric, key, value);
        names.add(n);
        return Metrics.counter(n);
    }

    private Timer timer(String metric, String key, String value)
    {
        MetricName n = name(metric, key, value);
        names.add(n);
        return Metrics.timer(n);
    }

    private void gauge(String metric, Gauge<?> gauge)
    {
        MetricName n = name(metric, null, null);
        names.add(n);
        Metrics.register(n, gauge);
    }

    private static MetricName name(String metric, String key, String value)
    {
        StringBuilder mbean = new StringBuilder(DefaultNameFactory.GROUP_NAME).append(":type=").append(TYPE_NAME)
                                                                              .append(",name=").append(metric);
        String scope = "";
        if (key != null)
        {
            scope = key + '=' + sanitize(value);
            mbean.append(',').append(scope);
        }
        return new MetricName(DefaultNameFactory.GROUP_NAME, TYPE_NAME, metric, scope, mbean.toString());
    }

    /** Values become part of an MBean name, so they are restricted to characters that need no quoting. */
    private static String sanitize(String value)
    {
        return value == null || value.isEmpty() ? UNKNOWN : value.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    private static void guard(Runnable r)
    {
        try
        {
            r.run();
        }
        catch (Throwable t)
        {
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, "autonomic.metrics", 5, TimeUnit.MINUTES,
                             "Autonomic metric update failed: {}", t.toString());
        }
    }
}
