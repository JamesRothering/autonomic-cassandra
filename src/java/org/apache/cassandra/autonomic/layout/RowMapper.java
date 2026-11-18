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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Maps updates between a logical table and its overflow table (design 3.3, {@code split}). Rows keep their clustering
 * and have their cells re-labelled with {@link Cell#withUpdatedColumn}, so values, cell paths, timestamps, TTLs, local
 * deletion times, row liveness, row deletions and complex deletions are copied bit for bit. A row goes to the bucket
 * of its clustering ({@link BucketFunction}); a range tombstone and a partition deletion go to every bucket. The
 * partition key gains the bucket column, {@code incarnation * 256 + bucket}.
 *
 * <p>Static rows cannot occur: tables with static columns are not eligible ({@link Eligibility}).
 */
public final class RowMapper
{
    private RowMapper()
    {
    }

    /**
     * The updates of the overflow table for a logical update, by bucket. A bucket is present if a row hashes to it or
     * the update deletes the partition or a range; then every bucket is present.
     */
    public static Map<Integer, PartitionUpdate> toPhysical(PartitionUpdate logical, TableMetadata physical, TableLayout layout, int incarnation)
    {
        if (!logical.staticRow().isEmpty() || !logical.columns().statics.isEmpty())
            throw new IllegalArgumentException("static columns cannot be split");
        List<ByteBuffer> keyComponents = components(logical.metadata(), logical.partitionKey().getKey());
        DeletionInfoView deletions = new DeletionInfoView(logical);
        Map<Integer, PartitionUpdate.Builder> builders = new TreeMap<>();
        RegularAndStaticColumns columns = new RegularAndStaticColumns(Columns.NONE, relabel(logical.columns().regulars, physical));
        int estimate = Math.max(1, logical.rowCount());

        if (deletions.partitionLevel || !deletions.ranges.isEmpty())
            for (int bucket = 0; bucket < layout.buckets; bucket++)
                builders.put(bucket, builder(physical, keyComponents, incarnation, bucket, columns, estimate));
        for (Map.Entry<Integer, PartitionUpdate.Builder> e : builders.entrySet())
        {
            if (deletions.partitionLevel)
                e.getValue().addPartitionDeletion(logical.partitionLevelDeletion());
            for (RangeTombstone range : deletions.ranges)
                e.getValue().add(range);
        }
        for (Row row : logical)
        {
            int bucket = BucketFunction.bucket(row.clustering(), layout.hashedClusteringColumns, layout.buckets);
            builders.computeIfAbsent(bucket, b -> builder(physical, keyComponents, incarnation, b, columns, estimate))
                    .add(relabelRow(row, physical));
        }
        Map<Integer, PartitionUpdate> result = new TreeMap<>();
        builders.forEach((bucket, builder) -> result.put(bucket, builder.build()));
        return result;
    }

    /** The logical update of one update of the overflow table: the bucket is dropped from the key, everything else is kept. */
    public static PartitionUpdate toLogical(PartitionUpdate physical, TableMetadata logical)
    {
        if (!physical.staticRow().isEmpty())
            throw new IllegalArgumentException("static columns cannot be split");
        List<ByteBuffer> components = components(physical.metadata(), physical.partitionKey().getKey());
        List<ByteBuffer> logicalComponents = components.subList(0, components.size() - 1);
        DecoratedKey key = logical.partitioner.decorateKey(compose(logical, logicalComponents));
        RegularAndStaticColumns columns = new RegularAndStaticColumns(Columns.NONE, relabel(physical.columns().regulars, logical));
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(logical, key, columns, Math.max(1, physical.rowCount()));
        DeletionInfoView deletions = new DeletionInfoView(physical);
        if (deletions.partitionLevel)
            builder.addPartitionDeletion(physical.partitionLevelDeletion());
        for (RangeTombstone range : deletions.ranges)
            builder.add(range);
        for (Row row : physical)
            builder.add(relabelRow(row, logical));
        return builder.build();
    }

    /** The value of the bucket column of a partition key of the overflow table. */
    public static int bucketValue(TableMetadata physical, DecoratedKey key)
    {
        List<ByteBuffer> components = components(physical, key.getKey());
        return Int32Type.instance.compose(components.get(components.size() - 1));
    }

    /** The partition key of the overflow table that holds a bucket of a logical partition (design 2.2). */
    public static DecoratedKey physicalKey(TableMetadata logical, TableMetadata physical, ByteBuffer logicalKey, int incarnation, int bucket)
    {
        return physicalKey(physical, components(logical, logicalKey), incarnation, bucket);
    }

    /** The row with every cell and complex deletion re-labelled to the column of the same name of {@code to}; nothing else changes. */
    public static Row relabel(Row row, TableMetadata to)
    {
        return relabelRow(row, to);
    }

    // ---- internals ----

    private static final class DeletionInfoView
    {
        final boolean partitionLevel;
        final List<RangeTombstone> ranges = new ArrayList<>();

        DeletionInfoView(PartitionUpdate update)
        {
            partitionLevel = !update.partitionLevelDeletion().isLive();
            for (Iterator<RangeTombstone> it = update.deletionInfo().rangeIterator(false); it.hasNext(); )
                ranges.add(it.next());
        }
    }

    private static PartitionUpdate.Builder builder(TableMetadata physical, List<ByteBuffer> keyComponents, int incarnation, int bucket,
                                                   RegularAndStaticColumns columns, int estimate)
    {
        return new PartitionUpdate.Builder(physical, physicalKey(physical, keyComponents, incarnation, bucket), columns, estimate);
    }

    private static DecoratedKey physicalKey(TableMetadata physical, List<ByteBuffer> keyComponents, int incarnation, int bucket)
    {
        List<ByteBuffer> all = new ArrayList<>(keyComponents);
        all.add(Int32Type.instance.decompose(BucketFunction.physicalValue(incarnation, bucket)));
        return physical.partitioner.decorateKey(compose(physical, all));
    }

    private static List<ByteBuffer> components(TableMetadata table, ByteBuffer key)
    {
        if (table.partitionKeyColumns().size() == 1)
            return List.of(key);
        return CompositeType.splitName(key, ByteBufferAccessor.instance);
    }

    private static ByteBuffer compose(TableMetadata table, List<ByteBuffer> components)
    {
        if (components.size() == 1)
            return components.get(0);
        return CompositeType.build(ByteBufferAccessor.instance, components.toArray(new ByteBuffer[0]));
    }

    private static Columns relabel(Columns columns, TableMetadata to)
    {
        List<ColumnMetadata> mapped = new ArrayList<>(columns.size());
        for (ColumnMetadata column : columns)
        {
            ColumnMetadata target = column(column, to);
            if (target != null)
                mapped.add(target);
        }
        return Columns.from(mapped);
    }

    /**
     * The column of {@code to} that {@code column} maps to, or null if the column was dropped from {@code to}: a table
     * whose metadata is one epoch ahead of the data being mapped has the column recorded as dropped, and its cells are
     * invisible to every read of {@code to} anyway, so they are skipped (ADR-034). A column that is neither present nor
     * dropped (an addition {@code to} has not seen yet) is an error, never silently skipped, so the caller retries.
     */
    private static ColumnMetadata column(ColumnMetadata column, TableMetadata to)
    {
        ColumnMetadata mapped = to.getColumn(column.name);
        if (mapped == null && to.droppedColumns.containsKey(column.name.bytes))
            return null;
        if (mapped == null || !mapped.type.equals(column.type))
            throw new IllegalArgumentException("column " + column.name + " is missing or differs in " + to);
        return mapped;
    }

    private static Row relabelRow(Row row, TableMetadata to)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(row.clustering());
        builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
        builder.addRowDeletion(row.deletion());
        for (ColumnData data : row)
        {
            ColumnMetadata column = column(data.column(), to);
            if (column == null)
                continue;
            if (data.column().isComplex())
            {
                ComplexColumnData complex = (ComplexColumnData) data;
                builder.addComplexDeletion(column, complex.complexDeletion());
                for (Cell<?> cell : complex)
                    builder.addCell(cell.withUpdatedColumn(column));
            }
            else
            {
                builder.addCell(((Cell<?>) data).withUpdatedColumn(column));
            }
        }
        return builder.build();
    }
}
