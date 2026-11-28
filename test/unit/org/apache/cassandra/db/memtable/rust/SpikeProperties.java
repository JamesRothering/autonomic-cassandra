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

package org.apache.cassandra.db.memtable.rust;

/**
 * System properties of the Rust memtable spike's tests and benchmarks. They are spike-only, so they are read here
 * rather than added to {@code CassandraRelevantProperties}.
 */
final class SpikeProperties
{
    private SpikeProperties()
    {
    }

    static String get(String name, String defaultValue)
    {
        return System.getProperty(name, defaultValue); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    }

    static void setIfUnset(String name, String value)
    {
        if (System.getProperty(name) == null) // checkstyle: suppress nearby 'blockSystemPropertyUsage'
            System.setProperty(name, value); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    }

    static int getInt(String name, int defaultValue)
    {
        return Integer.parseInt(get(name, Integer.toString(defaultValue)));
    }

    static long getLong(String name, long defaultValue)
    {
        return Long.parseLong(get(name, Long.toString(defaultValue)));
    }
}
