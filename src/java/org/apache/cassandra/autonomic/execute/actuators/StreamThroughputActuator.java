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
 * Outbound stream throughput in MiB/s through {@code StorageService.setStreamThroughputMebibytesPerSec}. Envelope
 * {@code min_factor} (default 0.25) to {@code max_factor} (default 2.0) times the configured value; a configured
 * value of 0 (unlimited) is not managed.
 */
public class StreamThroughputActuator extends KnobActuator
{
    public static final String NAME = "stream_throughput";

    public StreamThroughputActuator(Handle handle, Map<String, Object> options)
    {
        super(NAME, handle, handle.get() * option(options, "min_factor", 0.25), handle.get() * option(options, "max_factor", 2.0), 1, true);
    }

    public static StreamThroughputActuator create(Map<String, Object> options)
    {
        return new StreamThroughputActuator(new Handle()
        {
            public double get()
            {
                return StorageService.instance.getStreamThroughputMebibytesPerSec();
            }

            public void set(double value)
            {
                StorageService.instance.setStreamThroughputMebibytesPerSec((int) Math.round(value));
            }
        }, options);
    }
}
