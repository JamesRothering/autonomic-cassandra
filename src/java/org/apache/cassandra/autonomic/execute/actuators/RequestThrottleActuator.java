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

import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ClientResourceLimits;

/**
 * The native transport request rate limiter, controlled by AIMD: multiplicative decrease while the node is in
 * trouble, additive increase while it recovers, and finally the limiter is switched off again.
 *
 * <p>The knob is the allowed rate in requests per second. An unthrottled node is at the {@code ceiling}
 * ({@code max_requests_per_second}, default the configured limit); the envelope is {@code min_requests_per_second}
 * (default 1000, the floor) to the ceiling. Setting the ceiling restores the configured limit and enabled state,
 * setting anything below it sets the limit and enables the limiter, so rollback and {@code revert} restore both.
 * Enabling the limiter changes what clients see, so the actuator is off by default and its policy only acts at
 * CRITICAL. Other options: {@code increase_step} (default a tenth of the floor), the additive increase.
 *
 * <p>The step limit applies while the limiter is engaged; engaging from, and releasing to, the ceiling is exempt
 * because the rate it starts from is the observed demand, not a configured value.
 */
public class RequestThrottleActuator extends KnobActuator
{
    public static final String NAME = "request_throttle";
    public static final double DEFAULT_FLOOR = 1000;
    /** Multiplicative decrease: each step takes this fraction off, within the default step limit of 25%. */
    public static final double DECREASE_FRACTION = 0.2;

    /** The limiter settings; the production one is {@code ClientResourceLimits} and {@code StorageService}. */
    public interface Limiter
    {
        boolean enabled();

        int limit();

        /** Writes both; the limit first when enabling and the flag first when disabling. */
        void set(int limit, boolean enabled);
    }

    private final double ceiling;

    public RequestThrottleActuator(Limiter limiter, Map<String, Object> options)
    {
        this(limiter, options, limiter.limit(), limiter.enabled());
    }

    private RequestThrottleActuator(Limiter limiter, Map<String, Object> options, int configuredLimit, boolean configuredEnabled)
    {
        super(NAME, handle(limiter, ceiling(options, configuredLimit, configuredEnabled), configuredLimit, configuredEnabled),
              Math.min(option(options, "min_requests_per_second", DEFAULT_FLOOR), ceiling(options, configuredLimit, configuredEnabled)),
              ceiling(options, configuredLimit, configuredEnabled),
              Math.max(1, option(options, "increase_step", Math.max(1, option(options, "min_requests_per_second", DEFAULT_FLOOR) / 10))), true);
        this.ceiling = ceiling(options, configuredLimit, configuredEnabled);
    }

    /** An enabled limiter is never raised above its configured limit. */
    private static double ceiling(Map<String, Object> options, int configuredLimit, boolean configuredEnabled)
    {
        return configuredEnabled ? configuredLimit : Math.max(configuredLimit, option(options, "max_requests_per_second", configuredLimit));
    }

    private static Handle handle(Limiter limiter, double ceiling, int configuredLimit, boolean configuredEnabled)
    {
        return new Handle()
        {
            public double get()
            {
                return limiter.enabled() ? Math.min(limiter.limit(), ceiling) : ceiling;
            }

            public void set(double value)
            {
                if (value >= ceiling)
                    limiter.set(configuredLimit, configuredEnabled);
                else
                    limiter.set((int) Math.round(value), true);
            }
        };
    }

    public static RequestThrottleActuator create(Map<String, Object> options)
    {
        return new RequestThrottleActuator(new Limiter()
        {
            public boolean enabled()
            {
                return StorageService.instance.getNativeTransportRateLimitingEnabled();
            }

            public int limit()
            {
                return ClientResourceLimits.getNativeTransportMaxRequestsPerSecond();
            }

            public void set(int limit, boolean enabled)
            {
                if (enabled)
                {
                    ClientResourceLimits.setNativeTransportMaxRequestsPerSecond(limit);
                    StorageService.instance.setNativeTransportRateLimitingEnabled(true);
                }
                else
                {
                    StorageService.instance.setNativeTransportRateLimitingEnabled(false);
                    ClientResourceLimits.setNativeTransportMaxRequestsPerSecond(limit);
                }
            }
        }, options);
    }

    /** True while the limiter holds the rate below the ceiling. */
    public boolean engaged()
    {
        return current() < ceiling;
    }

    public double ceiling()
    {
        return ceiling;
    }

    /**
     * The first limit when the limiter is off: the observed request rate less one decrease step, at least the floor.
     * Empty if the limiter is already engaged, the rate is unknown or the result would not be below the ceiling.
     */
    public Optional<Double> engageAt(double observedRate)
    {
        if (engaged() || operatorOverride() || !(observedRate > 0))
            return Optional.empty();
        double target = Math.max(min(), Math.ceil(Math.min(observedRate, ceiling) * (1 - DECREASE_FRACTION)));
        return target < ceiling ? Optional.of(target) : Optional.empty();
    }

    /** Multiplicative decrease of an engaged limiter; empty at the floor. */
    public Optional<Double> decrease(String diagnosisId)
    {
        return engaged() ? nextTarget(Direction.DOWN, DECREASE_FRACTION, diagnosisId) : Optional.empty();
    }

    /**
     * Additive increase of an engaged limiter; once the limit is at least {@code releaseFactor} times the observed
     * rate it no longer binds and the limiter is released (the ceiling). Empty if not engaged, overridden or locked.
     */
    public Optional<Double> increase(double observedRate, double releaseFactor, String diagnosisId)
    {
        if (!engaged() || operatorOverride())
            return Optional.empty();
        if (observedRate >= 0 && current() >= observedRate * releaseFactor)
            return Optional.of(ceiling);
        return nextTarget(Direction.UP, 0, diagnosisId);
    }

    @Override
    public Optional<String> blastRadius(Action action, KnowledgeView knowledge, double maxStepFraction)
    {
        double target = Double.parseDouble(action.params.get(PARAM_VALUE));
        if (!engaged() || target >= ceiling)
            return target < min() || target > ceiling ? Optional.of("target is outside the envelope") : Optional.empty();
        return super.blastRadius(action, knowledge, maxStepFraction);
    }
}
