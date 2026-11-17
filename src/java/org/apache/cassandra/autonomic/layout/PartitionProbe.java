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
import java.util.List;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.Clock;

/**
 * The row-count probe of design 7.2 steps 2 and 6: coordinator reads of one partition at {@code ONE}, selecting only
 * all columns (a row that exists only through a regular cell is not seen by a selection of the clustering columns
 * alone), in pages and with a pause between pages. It stops at {@code maxRows}, which bounds its cost whatever the
 * size of the partition; the first rows also give the mean encoded row size.
 */
public final class PartitionProbe
{
    public static final int PAGE_ROWS = 1000;
    public static final int SAMPLE_ROWS = 50;
    public static final long PAUSE_MILLIS = 10;

    /** What the probe saw. {@code rows} is exact below {@code maxRows}; at it the partition is larger. */
    public static final class Result
    {
        public final long rows;
        public final boolean hitMax;
        public final double meanRowBytes;

        public Result(long rows, boolean hitMax, double meanRowBytes)
        {
            this.rows = rows;
            this.hitMax = hitMax;
            this.meanRowBytes = meanRowBytes;
        }

        public long estimatedBytes()
        {
            return (long) (rows * meanRowBytes);
        }
    }

    private PartitionProbe()
    {
    }

    public static Result run(TableMetadata table, ByteBuffer key, long maxRows) throws InterruptedException
    {
        DecoratedKey decorated = table.partitioner.decorateKey(key);
        Clustering<?> after = null;
        long rows = 0;
        while (rows < maxRows)
        {
            List<Row> page = read(table, decorated, after, PAGE_ROWS, ColumnFilter.all(table));
            rows += page.size();
            if (page.size() < PAGE_ROWS)
                return new Result(rows, false, sample(table, decorated));
            after = page.get(page.size() - 1).clustering();
            Thread.sleep(PAUSE_MILLIS);
        }
        return new Result(rows, true, sample(table, decorated));
    }

    private static double sample(TableMetadata table, DecoratedKey key)
    {
        List<Row> rows = read(table, key, null, SAMPLE_ROWS, ColumnFilter.all(table));
        if (rows.isEmpty())
            return 0;
        long bytes = 0;
        for (Row row : rows)
            bytes += row.dataSize();
        return (double) bytes / rows.size();
    }

    private static List<Row> read(TableMetadata table, DecoratedKey key, Clustering<?> after, int limit, ColumnFilter columns)
    {
        Slices slices = after == null ? Slices.ALL
                                      : Slices.with(table.comparator, Slice.make(ClusteringBound.exclusiveStartOf(after), ClusteringBound.TOP));
        long nowInSec = Clock.Global.currentTimeMillis() / 1000;
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(table, (int) nowInSec, columns, RowFilter.none(), DataLimits.cqlLimits(limit), key,
                                                                                new ClusteringIndexSliceFilter(slices, false));
        List<Row> rows = new java.util.ArrayList<>();
        try (PartitionIterator partitions = StorageProxy.read(SinglePartitionReadCommand.Group.one(command), ConsistencyLevel.ONE,
                                                              Dispatcher.RequestTime.forImmediateExecution()))
        {
            while (partitions.hasNext())
                try (RowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        rows.add(partition.next());
                }
        }
        return rows;
    }
}
