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

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

/**
 * The rows of one logical partition, merged from the rows of its buckets (design 3.4, steps 5 and 6): a k-way merge
 * on the clustering comparator, reversed if the query is. A clustering lives in exactly one bucket, so there are no
 * ties to resolve. Rows are re-labelled to the columns of the logical table as they are returned; the partition key
 * and the metadata are the logical ones.
 *
 * <p>A {@link Source} holds the rows of one bucket in query order. An eager source has them all; a lazy one fetches
 * the next chunk when the previous one is consumed, so the iterator may read, and fail, while it is iterated.
 *
 * <p>With a single source and no re-labelling this is also the form in which the rows of a partition that was read
 * from the base table are held until the read is validated.
 */
final class FanOutIterator extends AbstractIterator<Row> implements RowIterator
{
    /** The rows of one bucket in query order. */
    static final class Source
    {
        private List<Row> buffer;
        private int index;
        /** The next chunk, or null if there is none; null for an eager source. */
        private Supplier<List<Row>> more;

        private Source(List<Row> rows, Supplier<List<Row>> more)
        {
            this.buffer = rows;
            this.more = more;
        }

        static Source eager(List<Row> rows)
        {
            return new Source(rows, null);
        }

        /** {@code more} returns the next chunk of rows, or null or an empty list when the bucket is exhausted. */
        static Source lazy(Supplier<List<Row>> more)
        {
            return new Source(List.of(), more);
        }

        /** The next row, not consumed; null if the bucket is exhausted. Fetches the next chunk of a lazy source. */
        Row peek()
        {
            while (index >= buffer.size())
            {
                if (more == null)
                    return null;
                List<Row> next = more.get();
                if (next == null || next.isEmpty())
                {
                    more = null;
                    return null;
                }
                buffer = next;
                index = 0;
            }
            return buffer.get(index);
        }

        void advance()
        {
            index++;
        }
    }

    private final TableMetadata metadata;
    private final DecoratedKey partitionKey;
    private final boolean reversed;
    private final RegularAndStaticColumns columns;
    private final Row staticRow;
    private final List<Source> sources;
    private final Comparator<Clusterable> order;
    /** The table whose columns the rows are re-labelled to, or null if the sources already hold rows of {@link #metadata}. */
    private final TableMetadata relabelTo;

    FanOutIterator(TableMetadata metadata, DecoratedKey partitionKey, boolean reversed, RegularAndStaticColumns columns, Row staticRow,
                   List<Source> sources, boolean relabel)
    {
        this.metadata = metadata;
        this.partitionKey = partitionKey;
        this.reversed = reversed;
        this.columns = columns;
        this.staticRow = staticRow;
        this.sources = sources;
        this.order = reversed ? metadata.comparator.reversed() : metadata.comparator;
        this.relabelTo = relabel ? metadata : null;
    }

    @Override
    protected Row computeNext()
    {
        Source first = null;
        Row firstRow = null;
        for (Source source : sources)
        {
            Row row = source.peek();
            if (row != null && (firstRow == null || order.compare(row, firstRow) < 0))
            {
                first = source;
                firstRow = row;
            }
        }
        if (first == null)
            return endOfData();
        first.advance();
        return relabelTo == null ? firstRow : RowMapper.relabel(firstRow, relabelTo);
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public boolean isReverseOrder()
    {
        return reversed;
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        return columns;
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    @Override
    public Row staticRow()
    {
        return staticRow;
    }

    @Override
    public void close()
    {
    }
}
