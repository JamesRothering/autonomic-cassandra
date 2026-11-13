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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.autonomic.AutonomicConfig;

/**
 * Actuators by name, with the per-actuator gate: the {@code enabled} option of {@code autonomic.actuators.<name>},
 * overridden at runtime by {@link #setEnabled}. Unknown or unconfigured actuators are disabled. Thread-safe.
 */
public class ActuatorRegistry
{
    private final Map<String, Actuator> actuators = new ConcurrentHashMap<>();
    private final Map<String, Boolean> overrides = new ConcurrentHashMap<>();

    public void register(Actuator actuator)
    {
        if (actuators.putIfAbsent(actuator.name(), actuator) != null)
            throw new IllegalArgumentException("Actuator already registered: " + actuator.name());
    }

    /** Null if unknown. */
    public Actuator get(String name)
    {
        return actuators.get(name);
    }

    public Collection<Actuator> all()
    {
        return actuators.values();
    }

    /** Runtime override of the configured gate; lost at restart. */
    public void setEnabled(String name, boolean enabled)
    {
        if (!actuators.containsKey(name))
            throw new IllegalArgumentException("Unknown actuator: " + name);
        overrides.put(name, enabled);
    }

    public boolean isEnabled(String name, AutonomicConfig config)
    {
        if (!actuators.containsKey(name))
            return false;
        Boolean override = overrides.get(name);
        if (override != null)
            return override;
        Map<String, Object> options = config.actuators == null ? null : config.actuators.get(name);
        Object enabled = options == null ? null : options.get("enabled");
        return enabled != null && Boolean.parseBoolean(enabled.toString());
    }
}
