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
package org.apache.cassandra.autonomic.analyze.detectors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Detector;

/** The built-in detectors, built from {@code autonomic.detectors}; a detector is on unless its {@code enabled} is false. */
public final class Detectors
{
    private Detectors()
    {
    }

    public static List<Detector> build(AutonomicConfig config)
    {
        List<Detector> out = new ArrayList<>();
        if (enabled(config, HotPartitionDetector.NAME))
            out.add(HotPartitionDetector.fromConfig(options(config, HotPartitionDetector.NAME)));
        if (enabled(config, LatencySloBreachDetector.NAME))
            out.add(LatencySloBreachDetector.fromConfig(options(config, LatencySloBreachDetector.NAME), config.slo));
        if (enabled(config, TombstoneOverloadDetector.NAME))
            out.add(TombstoneOverloadDetector.fromConfig(options(config, TombstoneOverloadDetector.NAME)));
        if (enabled(config, CompactionBacklogDetector.NAME))
            out.add(CompactionBacklogDetector.fromConfig(options(config, CompactionBacklogDetector.NAME)));
        if (enabled(config, DiskPressureDetector.NAME))
            out.add(DiskPressureDetector.fromConfig(options(config, DiskPressureDetector.NAME)));
        if (enabled(config, GcPressureDetector.NAME))
            out.add(GcPressureDetector.fromConfig(options(config, GcPressureDetector.NAME)));
        if (enabled(config, PoolSaturationDetector.NAME))
            out.add(PoolSaturationDetector.fromConfig(options(config, PoolSaturationDetector.NAME)));
        if (enabled(config, NodeImbalanceDetector.NAME))
            out.add(NodeImbalanceDetector.fromConfig(options(config, NodeImbalanceDetector.NAME)));
        if (enabled(config, DegradedPeerDetector.NAME))
            out.add(DegradedPeerDetector.fromConfig(options(config, DegradedPeerDetector.NAME)));
        if (enabled(config, DegradedSelfDetector.NAME))
            out.add(DegradedSelfDetector.fromConfig(options(config, DegradedSelfDetector.NAME), config.slo));
        if (enabled(config, RepairOverdueDetector.NAME))
            out.add(RepairOverdueDetector.fromConfig(options(config, RepairOverdueDetector.NAME)));
        return out;
    }

    public static boolean enabled(AutonomicConfig config, String name)
    {
        Object enabled = options(config, name).get("enabled");
        return enabled == null || Boolean.parseBoolean(enabled.toString());
    }

    private static Map<String, Object> options(AutonomicConfig config, String name)
    {
        Map<String, Object> o = config.detectors == null ? null : config.detectors.get(name);
        return o == null ? Collections.emptyMap() : o;
    }
}
