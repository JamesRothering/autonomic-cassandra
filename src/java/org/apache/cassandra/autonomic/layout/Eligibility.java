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
package org.apache.cassandra.autonomic.layout;

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;

/**
 * The structural part of the eligibility rules (design section 6): the features of a table that make a split
 * impossible or unsafe, decided from the schema alone. Pure: the result depends only on the two arguments, so
 * {@code RegisterKey} can call it inside {@code execute} and every node reaches the same verdict.
 *
 * <p>Not here, because they are not schema facts: LWT activity and the shape profile ({@code Eligibility.runtime},
 * T6.11), and the size of a partition key (bounded by {@code RegisterKey} itself).
 */
public final class Eligibility
{
    /**
     * Smallest {@code gc_grace_seconds} of an eligible table: {@code 2 * (straggler_window + 1h)} with the default
     * straggler window of 3 hours (design 4.5, section 6). A constant, not configuration, so that replay is deterministic.
     */
    public static final int MIN_GC_GRACE_SECONDS = 2 * (3 + 1) * 3600;

    /**
     * Test seam (T6.17d): the minimum in force, so that a test of a slow reverse can use a table whose gc_grace is a
     * few seconds. Set on every node before any layout is registered; never set in production, where replay must be
     * deterministic.
     */
    private static volatile int minGcGraceSeconds = MIN_GC_GRACE_SECONDS;

    @VisibleForTesting
    public static void setMinGcGraceSecondsForTesting(int seconds)
    {
        minGcGraceSeconds = seconds;
    }

    /** A feature that blocks a split. The names are stable: they are reported to operators. */
    public enum Blocker
    {
        ACCORD("the table is, or is migrating from, Accord transactional mode"),
        COUNTER("counter tables are not supported"),
        INDEX("the table has secondary indexes"),
        MATERIALIZED_VIEW("a materialized view is defined on the table, or the table is a view"),
        CDC("the table has cdc enabled"),
        TRIGGER("the table has triggers"),
        STATIC_COLUMN("the table has static columns"),
        MASKED_COLUMN("the table has masked columns"),
        COMPACT_STORAGE("the table uses compact storage"),
        VIRTUAL_TABLE("the table is virtual"),
        SYSTEM_KEYSPACE("the table is in a system keyspace"),
        NO_CLUSTERING("the table has no clustering columns, there is nothing to split"),
        TRANSIENT_REPLICATION("the keyspace uses transient replication"),
        RESERVED_COLUMN("the table has a column named " + TableLayout.BUCKET_COLUMN),
        GC_GRACE_TOO_SMALL("gc_grace_seconds is below " + MIN_GC_GRACE_SECONDS);

        public final String description;

        Blocker(String description)
        {
            this.description = description;
        }
    }

    private Eligibility()
    {
    }

    /** The blockers of the table, in the order of {@link Blocker}; empty if the table may be split. */
    public static List<Blocker> structural(TableMetadata table, KeyspaceMetadata keyspace)
    {
        List<Blocker> blockers = new ArrayList<>();
        if (table.requiresAccordSupport() || table.params.transactionalMode != TransactionalMode.off
            || table.params.transactionalMigrationFrom != TransactionalMigrationFromMode.none)
            blockers.add(Blocker.ACCORD);
        if (table.isCounter())
            blockers.add(Blocker.COUNTER);
        if (!table.indexes.isEmpty())
            blockers.add(Blocker.INDEX);
        if (table.isView() || (keyspace != null && keyspace.views.forTable(table.id).iterator().hasNext()))
            blockers.add(Blocker.MATERIALIZED_VIEW);
        if (table.params.cdc)
            blockers.add(Blocker.CDC);
        if (!table.triggers.isEmpty())
            blockers.add(Blocker.TRIGGER);
        if (table.hasStaticColumns())
            blockers.add(Blocker.STATIC_COLUMN);
        if (table.hasMaskedColumns())
            blockers.add(Blocker.MASKED_COLUMN);
        if (table.isCompactTable())
            blockers.add(Blocker.COMPACT_STORAGE);
        if (table.isVirtual())
            blockers.add(Blocker.VIRTUAL_TABLE);
        if (SchemaConstants.isSystemKeyspace(table.keyspace))
            blockers.add(Blocker.SYSTEM_KEYSPACE);
        if (table.clusteringColumns().isEmpty())
            blockers.add(Blocker.NO_CLUSTERING);
        if (keyspace != null && keyspace.replicationStrategy != null && keyspace.replicationStrategy.getReplicationFactor().hasTransientReplicas())
            blockers.add(Blocker.TRANSIENT_REPLICATION);
        for (ColumnMetadata column : table.columns())
            if (column.name.toString().equals(TableLayout.BUCKET_COLUMN))
            {
                blockers.add(Blocker.RESERVED_COLUMN);
                break;
            }
        if (table.params.gcGraceSeconds < minGcGraceSeconds)
            blockers.add(Blocker.GC_GRACE_TOO_SMALL);
        return blockers;
    }

    /**
     * The runtime part (design section 6, 7.4), decided on the proposing node and by the same call for an operator's
     * {@code layout split} and for the policy: LWT activity. Pure in its arguments.
     *
     * @param casOperations Paxos operations on the table on this node since it started
     *                      ({@code casPrepare}, {@code casPropose} and {@code casCommit} counts)
     * @param profiledCas   {@code cas} calls on the key in its shape profile; 0 without a profile
     * @return why the key must not be split, or null
     */
    public static String runtime(long casOperations, long profiledCas)
    {
        if (casOperations > 0 || profiledCas > 0)
            return "lightweight transactions were used on the table (" + casOperations + " Paxos operations on this node since it started"
                   + (profiledCas > 0 ? ", " + profiledCas + " on the key in its profile" : "") + "); they are rejected on an adapted partition";
        return null;
    }

    /** The blockers as one sentence, or null if there are none. */
    public static String describe(List<Blocker> blockers)
    {
        if (blockers.isEmpty())
            return null;
        StringBuilder sb = new StringBuilder();
        for (Blocker blocker : blockers)
            sb.append(sb.length() == 0 ? "" : "; ").append(blocker).append(": ").append(blocker.description);
        return sb.toString();
    }
}
