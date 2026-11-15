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
package org.apache.cassandra.autonomic.plan.policies;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.plan.Policy;

/**
 * The built-in policies. They are always built when the layer is enabled and do not look at actuator gates: the
 * governor rejects, or in dry-run only records, an action whose actuator is disabled. Verification windows come from
 * {@code baseline_window} and {@code observation_window} in the options of the actuator the action uses.
 */
public final class Policies
{
    private Policies()
    {
    }

    public static List<Policy> build(AutonomicConfig config, Function<String, Actuator> actuators)
    {
        return build(config, actuators, name -> true);
    }

    /**
     * @param enabled whether an actuator is enabled; the load spike ladder skips steps whose actuators are not,
     *                where the other policies leave that to the governor
     */
    public static List<Policy> build(AutonomicConfig config, Function<String, Actuator> actuators, Predicate<String> enabled)
    {
        Function<String, Windows> windows = actuator -> {
            Map<String, Object> options = config.actuators == null ? null : config.actuators.get(actuator);
            return Windows.fromOptions(options);
        };
        List<Policy> out = new ArrayList<>();
        out.add(new CompactionBacklogPolicy(actuators, windows));
        out.add(new TombstoneOverloadPolicy(actuators, windows));
        out.add(new GcPressurePolicy(actuators, windows));
        // pool_saturation, latency_slo_breach and degraded_self are answered by the ladder, in the order of overview 8.1
        ScaleOutPolicy scaleOut = new ScaleOutPolicy(actuators, windows);
        out.add(LoadSpikePolicy.standard(actuators, windows, enabled, scaleOut.step()));
        out.add(new RequestPoolPolicy(actuators, windows, RequestPoolPolicy.Mode.RECOVERY));
        out.add(new RequestThrottlePolicy(actuators, windows, RequestThrottlePolicy.Mode.RECOVERY));
        out.add(new RepairOverduePolicy(windows));
        // capacity_exhausted by disk trend; by saturation it is the last step of the ladder
        out.add(scaleOut);
        return out;
    }
}
