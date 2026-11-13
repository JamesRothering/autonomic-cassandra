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
package org.apache.cassandra.autonomic.monitor;

import java.util.Set;

/**
 * Monitor. Pull-based; called on the monitor thread. Must not block on I/O or locks held by request paths.
 */
public interface Sensor
{
    String name();

    Set<SignalId> signals();

    /** The loop never calls {@link #collect} more often than this. */
    long minIntervalMillis();

    void collect(ObservationSink sink, long nowMillis) throws Exception;

    /** For example opens a sampling window. */
    default void start()
    {
    }

    default void stop()
    {
    }
}
