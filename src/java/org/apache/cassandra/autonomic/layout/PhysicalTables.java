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
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * The overflow table of a layout (design 2.1): its name, the CQL that creates it from the logical table, and the check
 * that a table has the derived shape. The CQL is plain DDL because a schema transformation travels through the
 * metadata log as its CQL text (design F1). Pure.
 */
public final class PhysicalTables
{
    /** Longest prefix of the logical table name kept in the name of the overflow table. */
    public static final int MAX_PREFIX = 32;
    static final String SEPARATOR = "_al_";
    static final int ID_HEX_DIGITS = 8;

    private PhysicalTables()
    {
    }

    /** {@code <logical name truncated to 32 characters>_al_<first 8 hex digits of the layout id>}, at most 44 characters. */
    public static String physicalName(String logicalName, UUID layoutId)
    {
        String prefix = logicalName.length() > MAX_PREFIX ? logicalName.substring(0, MAX_PREFIX) : logicalName;
        return prefix + SEPARATOR + layoutId.toString().substring(0, ID_HEX_DIGITS);
    }

    /**
     * The {@code CREATE TABLE} statement of the overflow table: the columns of the logical table with
     * {@link TableLayout#BUCKET_COLUMN} (an int) appended to the partition key, the same clustering columns and order
     * and regular columns, {@code default_time_to_live = 0} (the logical default is applied before the router sees a
     * mutation), the same {@code gc_grace_seconds}, compaction, compression and caching, and the given table id.
     * Static columns are not derived: a table that has them is not eligible.
     */
    public static String derive(TableMetadata logical, UUID layoutId, TableId physicalId)
    {
        StringBuilder cql = new StringBuilder("CREATE TABLE ");
        cql.append(ColumnIdentifier.maybeQuote(logical.keyspace)).append('.')
           .append(ColumnIdentifier.maybeQuote(physicalName(logical.name, layoutId))).append(" (");

        List<String> pk = new ArrayList<>();
        for (ColumnMetadata column : logical.partitionKeyColumns())
        {
            cql.append(definition(column)).append(", ");
            pk.add(column.name.toCQLString());
        }
        String bucket = ColumnIdentifier.maybeQuote(TableLayout.BUCKET_COLUMN);
        cql.append(bucket).append(' ').append(Int32Type.instance.asCQL3Type()).append(", ");
        pk.add(bucket);
        List<String> ck = new ArrayList<>();
        List<String> order = new ArrayList<>();
        for (ColumnMetadata column : logical.clusteringColumns())
        {
            cql.append(definition(column)).append(", ");
            ck.add(column.name.toCQLString());
            order.add(column.name.toCQLString() + ' ' + column.clusteringOrder());
        }
        for (ColumnMetadata column : logical.regularColumns())
            cql.append(definition(column)).append(", ");

        cql.append("PRIMARY KEY ((").append(String.join(", ", pk)).append(')');
        for (String column : ck)
            cql.append(", ").append(column);
        cql.append(")) WITH ID = ").append(physicalId.toLongString());
        if (!order.isEmpty())
            cql.append(" AND CLUSTERING ORDER BY (").append(String.join(", ", order)).append(')');
        cql.append(" AND comment = ").append(quote("Managed by autonomic layout of " + logical.keyspace + '.' + logical.name + ". Do not access directly."));
        cql.append(" AND default_time_to_live = 0");
        cql.append(" AND gc_grace_seconds = ").append(logical.params.gcGraceSeconds);
        cql.append(" AND compaction = ").append(map(logical.params.compaction.asMap()));
        cql.append(" AND compression = ").append(map(logical.params.compression.asMap()));
        cql.append(" AND caching = ").append(map(logical.params.caching.asMap()));
        return cql.toString();
    }

    private static String definition(ColumnMetadata column)
    {
        return column.name.toCQLString() + ' ' + column.type.asCQL3Type();
    }

    private static String quote(String value)
    {
        return '\'' + value.replace("'", "''") + '\'';
    }

    private static String map(Map<String, String> options)
    {
        StringBuilder sb = new StringBuilder("{");
        String sep = "";
        for (Map.Entry<String, String> e : new TreeMap<>(options).entrySet())
        {
            sb.append(sep).append(quote(e.getKey())).append(": ").append(quote(e.getValue()));
            sep = ", ";
        }
        return sb.append('}').toString();
    }

    /** True if {@code physical} has the derived shape of {@code logical}; see {@link #mismatch}. */
    public static boolean matches(TableMetadata logical, TableMetadata physical)
    {
        return mismatch(logical, physical) == null;
    }

    /**
     * Null if {@code physical} has the shape {@link #derive} gives: an ordinary table of the same keyspace whose
     * partition key is the logical one plus a trailing int {@link TableLayout#BUCKET_COLUMN}, with identical clustering
     * (names, types, order), static and regular columns, no default TTL, and the logical {@code gc_grace_seconds}.
     * Otherwise why not. Compaction, compression and caching may differ (design 4.8: other parameters change on the base only).
     */
    public static String mismatch(TableMetadata logical, TableMetadata physical)
    {
        if (physical.isCounter() || physical.isView() || physical.isVirtual())
            return "the physical table is a counter, view or virtual table";
        if (!logical.keyspace.equals(physical.keyspace))
            return "the tables are in different keyspaces";
        List<ColumnMetadata> lpk = logical.partitionKeyColumns();
        List<ColumnMetadata> ppk = physical.partitionKeyColumns();
        if (ppk.size() != lpk.size() + 1)
            return "partition key has " + ppk.size() + " columns, expected " + (lpk.size() + 1);
        for (int i = 0; i < lpk.size(); i++)
            if (!same(lpk.get(i), ppk.get(i)))
                return "partition key column " + i + " differs";
        ColumnMetadata bucket = ppk.get(lpk.size());
        if (!bucket.name.toString().equals(TableLayout.BUCKET_COLUMN) || !bucket.type.equals(Int32Type.instance))
            return "last partition key column is not an int named " + TableLayout.BUCKET_COLUMN;
        if (!sameColumns(logical.clusteringColumns(), physical.clusteringColumns()))
            return "clustering columns differ";
        if (!sameColumns(new ArrayList<>(logical.staticColumns()), new ArrayList<>(physical.staticColumns())))
            return "static columns differ";
        if (!sameColumns(new ArrayList<>(logical.regularColumns()), new ArrayList<>(physical.regularColumns())))
            return "regular columns differ";
        if (physical.params.defaultTimeToLive != 0)
            return "the physical table has a default time to live";
        if (physical.params.gcGraceSeconds != logical.params.gcGraceSeconds)
            return "gc_grace_seconds is " + physical.params.gcGraceSeconds + ", the logical table has " + logical.params.gcGraceSeconds;
        return null;
    }

    private static boolean same(ColumnMetadata a, ColumnMetadata b)
    {
        return a.name.equals(b.name) && a.type.equals(b.type) && a.kind == b.kind;
    }

    private static boolean sameColumns(List<ColumnMetadata> a, List<ColumnMetadata> b)
    {
        if (a.size() != b.size())
            return false;
        for (int i = 0; i < a.size(); i++)
            if (!same(a.get(i), b.get(i)))
                return false;
        return true;
    }
}
