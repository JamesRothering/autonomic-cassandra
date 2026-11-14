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

import org.apache.cassandra.service.StorageService;

/**
 * Compaction throughput in MiB/s through {@code StorageService.setCompactionThroughputMbPerSec}. Envelope
 * {@code min_factor} (default 0.25) to {@code max_factor} (default 4.0) times the configured value; a configured
 * value of 0 (unthrottled) is not managed.
 */
public class CompactionThroughputActuator extends KnobActuator
{
    public static final String NAME = "compaction_throughput";

    public CompactionThroughputActuator(Handle handle, Map<String, Object> options)
    {
        super(NAME, handle, handle.get() * option(options, "min_factor", 0.25), handle.get() * option(options, "max_factor", 4.0), 1, true);
    }

    public static CompactionThroughputActuator create(Map<String, Object> options)
    {
        return new CompactionThroughputActuator(new Handle()
        {
            public double get()
            {
                return StorageService.instance.getCompactionThroughputMbPerSec();
            }

            public void set(double value)
            {
                StorageService.instance.setCompactionThroughputMbPerSec((int) Math.round(value));
            }
        }, options);
    }
}
