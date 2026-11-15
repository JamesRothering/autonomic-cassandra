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
        Function<String, Windows> windows = actuator -> {
            Map<String, Object> options = config.actuators == null ? null : config.actuators.get(actuator);
            return Windows.fromOptions(options);
        };
        List<Policy> out = new ArrayList<>();
        out.add(new CompactionBacklogPolicy(actuators, windows));
        out.add(new TombstoneOverloadPolicy(actuators, windows));
        out.add(new GcPressurePolicy(actuators, windows));
        out.add(new DegradedSelfPolicy(actuators, windows));
        out.add(new RequestPoolPolicy(actuators, windows));
        out.add(new RequestThrottlePolicy(actuators, windows, RequestThrottlePolicy.Mode.BREACH));
        out.add(new RequestThrottlePolicy(actuators, windows, RequestThrottlePolicy.Mode.RECOVERY));
        out.add(new RepairOverduePolicy(windows));
        return out;
    }
}
