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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.ExecutorUtils;
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

    private ScheduledExecutorPlus monitor;
    private ScheduledExecutorPlus control;
    private ScheduledExecutorPlus actuator;

    private AutonomicService()
    {
        this(org.apache.cassandra.utils.Clock.Global::currentTimeMillis,
             () -> ClusterMetadata.current().myNodeState() == NodeState.JOINED);
    }

    @VisibleForTesting
    AutonomicService(LongSupplier clock, BooleanSupplier nodeJoined)
    {
        this.clock = clock;
        this.nodeJoined = nodeJoined;
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
        monitor = executorFactory().scheduled(false, MONITOR_THREAD);
        control = executorFactory().scheduled(false, CONTROL_THREAD);
        actuator = executorFactory().scheduled(false, ACTUATOR_THREAD);
        // starts the actuator thread; real work is event-driven and arrives with T2.6
        actuator.execute(() -> {});
        AutonomicConfig config = config();
        monitor.scheduleWithFixedDelay(this::monitorTick, config.monitor_interval.toSeconds(), config.monitor_interval.toSeconds(), TimeUnit.SECONDS);
        control.scheduleWithFixedDelay(this::controlTick, config.analyze_interval.toSeconds(), config.analyze_interval.toSeconds(), TimeUnit.SECONDS);
        logger.info("Autonomic layer started (dry_run={})", config.dry_run);
    }

    /** Runs on the AutonomicMonitor thread. Extension point for T2.2: sensor collection. */
    private void monitorTick()
    {
    }

    /** Runs on the AutonomicControl thread. Extension point for T2.2: detectors, policies, governor. */
    private void controlTick()
    {
        boolean nowArmed = evaluateArmed();
        if (!nowArmed)
            return;
        // T2.2: detectors, correlator and policies run here, only once armed
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
            long graceMillis = TimeUnit.SECONDS.toMillis(config().startup_grace.toSeconds());
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
    }
}
