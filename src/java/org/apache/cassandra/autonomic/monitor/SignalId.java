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

import java.util.Objects;

/** Name of a measured quantity, for example {@code table.read_latency_p99_micros}. */
public final class SignalId
{
    public final String name;

    public SignalId(String name)
    {
        this.name = Objects.requireNonNull(name);
    }

    public static SignalId of(String name)
    {
        return new SignalId(name);
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof SignalId && ((SignalId) o).name.equals(name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public String toString()
    {
        return name;
    }
}
