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

/** What an observation, diagnosis or action is about. */
public final class Subject
{
    public enum Kind { NODE, KEYSPACE, TABLE, PARTITION, THREAD_POOL, PEER, CLUSTER }

    public final Kind kind;
    /** Nullable. */
    public final String keyspace;
    /** Nullable. */
    public final String table;
    /** Partition key as lowercase hex of the raw key bytes (see sensors.PartitionKeys), pool name or peer host id; nullable. */
    public final String detail;

    public Subject(Kind kind, String keyspace, String table, String detail)
    {
        this.kind = Objects.requireNonNull(kind);
        this.keyspace = keyspace;
        this.table = table;
        this.detail = detail;
    }

    public static Subject node()
    {
        return new Subject(Kind.NODE, null, null, null);
    }

    public static Subject cluster()
    {
        return new Subject(Kind.CLUSTER, null, null, null);
    }

    public static Subject keyspace(String keyspace)
    {
        return new Subject(Kind.KEYSPACE, keyspace, null, null);
    }

    public static Subject table(String keyspace, String table)
    {
        return new Subject(Kind.TABLE, keyspace, table, null);
    }

    public static Subject partition(String keyspace, String table, String key)
    {
        return new Subject(Kind.PARTITION, keyspace, table, key);
    }

    public static Subject threadPool(String name)
    {
        return new Subject(Kind.THREAD_POOL, null, null, name);
    }

    public static Subject peer(String hostId)
    {
        return new Subject(Kind.PEER, null, null, hostId);
    }

    /** Stable form such as {@code TABLE:ks.tbl} or {@code PARTITION:ks.tbl:key}, used as storage key. */
    public String asString()
    {
        switch (kind)
        {
            case KEYSPACE:
                return "KEYSPACE:" + keyspace;
            case TABLE:
                return "TABLE:" + keyspace + '.' + table;
            case PARTITION:
                return "PARTITION:" + keyspace + '.' + table + ':' + detail;
            case THREAD_POOL:
            case PEER:
                return kind.name() + ':' + detail;
            default:
                return kind.name();
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Subject))
            return false;
        Subject s = (Subject) o;
        return kind == s.kind && Objects.equals(keyspace, s.keyspace) && Objects.equals(table, s.table) && Objects.equals(detail, s.detail);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, keyspace, table, detail);
    }

    @Override
    public String toString()
    {
        return asString();
    }
}
