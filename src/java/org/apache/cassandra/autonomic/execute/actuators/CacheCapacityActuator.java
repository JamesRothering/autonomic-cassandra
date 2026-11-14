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

import org.apache.cassandra.service.CacheService;

/**
 * Capacity in MiB of the key, row or counter cache through {@code CacheService.set*CacheCapacityInMB}; one instance
 * per cache. Envelope {@code min_factor} (default 0.5) to {@code max_factor} (default 2.0) times the configured
 * capacity; a cache configured to 0 (the row cache by default) is not managed.
 */
public class CacheCapacityActuator extends KnobActuator
{
    public static final String KEY = "key_cache_capacity";
    public static final String ROW = "row_cache_capacity";
    public static final String COUNTER = "counter_cache_capacity";
    private static final long MIB = 1024 * 1024;

    public CacheCapacityActuator(String name, Handle handle, Map<String, Object> options)
    {
        super(name, handle, handle.get() * option(options, "min_factor", 0.5), handle.get() * option(options, "max_factor", 2.0), 1, true);
    }

    public static CacheCapacityActuator key(Map<String, Object> options)
    {
        return new CacheCapacityActuator(KEY, new Handle()
        {
            public double get()
            {
                return CacheService.instance.keyCache.getCapacity() / MIB;
            }

            public void set(double value)
            {
                CacheService.instance.setKeyCacheCapacityInMB(Math.round(value));
            }
        }, options);
    }

    public static CacheCapacityActuator row(Map<String, Object> options)
    {
        return new CacheCapacityActuator(ROW, new Handle()
        {
            public double get()
            {
                return CacheService.instance.rowCache.getCapacity() / MIB;
            }

            public void set(double value)
            {
                CacheService.instance.setRowCacheCapacityInMB(Math.round(value));
            }
        }, options);
    }

    public static CacheCapacityActuator counter(Map<String, Object> options)
    {
        return new CacheCapacityActuator(COUNTER, new Handle()
        {
            public double get()
            {
                return CacheService.instance.counterCache.getCapacity() / MIB;
            }

            public void set(double value)
            {
                CacheService.instance.setCounterCacheCapacityInMB(Math.round(value));
            }
        }, options);
    }
}
