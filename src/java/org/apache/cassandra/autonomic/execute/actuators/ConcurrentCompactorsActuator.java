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

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Concurrent compactors through {@code StorageService.setConcurrentCompactors}. Envelope: 1 to half the available
 * processors, widened to include the configured value.
 */
public class ConcurrentCompactorsActuator extends KnobActuator
{
    public static final String NAME = "concurrent_compactors";

    public ConcurrentCompactorsActuator(Handle handle, int maxCompactors)
    {
        super(NAME, handle, 1, maxCompactors, 1, true);
    }

    public static ConcurrentCompactorsActuator create()
    {
        return new ConcurrentCompactorsActuator(new Handle()
        {
            public double get()
            {
                return StorageService.instance.getConcurrentCompactors();
            }

            public void set(double value)
            {
                StorageService.instance.setConcurrentCompactors((int) Math.round(value));
            }
        }, FBUtilities.getAvailableProcessors() / 2);
    }
}
