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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Writes partitions in the record format the Rust memtable reads and returns ({@code codec.rs}). Keys and
 * clusterings are written in their byte-comparable form ({@code ByteComparable.Version.OSS50}), so Rust orders
 * them by byte comparison; a cell's column is its index in the table's regular columns, which sorts like the
 * columns themselves.
 * <p>
 * Supported: regular simple-column cells, cell tombstones, expiring cells, row liveness, row deletions and partition
 * deletions. Static rows, range tombstones, complex (collection) columns and counters throw
 * {@link UnsupportedOperationException}.
 */
public final class PartitionRecords
{
    static final ByteComparable.Version VERSION = ByteComparable.Version.OSS50;

    private PartitionRecords()
    {
    }

    /**
     * Writes an update as one record, with its rows as stored. This deliberately does not use
     * {@code update.unfilteredIterator()}, which would filter the rows by the update's own partition deletion: the
     * memtable receives the raw rows, as Cassandra's memtable does in {@code put}.
     */
    public static void write(DataOutputBuffer out, PartitionUpdate update) throws IOException
    {
        write(out, update.metadata(), update.partitionKey(), update.partitionLevelDeletion(), update.staticRow(), update.iterator());
    }

    /** Writes one partition record as a read sees it. The iterator is consumed, not closed. */
    public static void write(DataOutputBuffer out, TableMetadata metadata, UnfilteredRowIterator partition) throws IOException
    {
        write(out, metadata, partition.partitionKey(), partition.partitionLevelDeletion(), partition.staticRow(), partition);
    }

    private static void write(DataOutputBuffer out, TableMetadata metadata, DecoratedKey key, DeletionTime partitionDeletion,
                              Row staticRow, Iterator<? extends Unfiltered> rows) throws IOException
    {
        if (!staticRow.isEmpty())
            throw new UnsupportedOperationException("static rows are not supported by the Rust memtable spike");
        writeKey(out, key);
        writeDeletion(out, partitionDeletion);
        int countAt = out.getLength();
        out.writeInt(0);
        int count = 0;
        while (rows.hasNext())
        {
            Unfiltered unfiltered = rows.next();
            if (!unfiltered.isRow())
                throw new UnsupportedOperationException("range tombstones are not supported by the Rust memtable spike");
            writeRow(out, metadata, (Row) unfiltered);
            count++;
        }
        patchInt(out, countAt, count);
    }

    public static void writeKey(DataOutputBuffer out, DecoratedKey key) throws IOException
    {
        writeComparable(out, key.asComparableBytes(VERSION));
    }

    /** Writes {@code len:u32} then the bytes of {@code source}. */
    static void writeComparable(DataOutputBuffer out, ByteSource source) throws IOException
    {
        int lengthAt = out.getLength();
        out.writeInt(0);
        int b;
        while ((b = source.next()) != ByteSource.END_OF_STREAM)
            out.writeByte(b);
        patchInt(out, lengthAt, out.getLength() - lengthAt - 4);
    }

    private static void writeRow(DataOutputBuffer out, TableMetadata metadata, Row row) throws IOException
    {
        writeComparable(out, metadata.comparator.asByteComparable(row.clustering()).asComparableBytes(VERSION));
        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        out.writeLong(liveness.timestamp());
        out.writeInt(liveness.ttl());
        out.writeLong(liveness.localExpirationTime());
        writeDeletion(out, row.deletion().time());
        out.writeByte(row.deletion().isShadowable() ? 1 : 0);
        int countAt = out.getLength();
        out.writeInt(0);
        int count = 0;
        for (ColumnData data : row)
        {
            if (data.column().isComplex())
                throw new UnsupportedOperationException("complex columns are not supported by the Rust memtable spike");
            Cell<?> cell = (Cell<?>) data;
            if (cell.isCounterCell())
                throw new UnsupportedOperationException("counters are not supported by the Rust memtable spike");
            out.writeInt(metadata.regularColumns().simpleIdx(cell.column()));
            out.writeLong(cell.timestamp());
            out.writeInt(cell.ttl());
            out.writeLong(cell.localDeletionTime());
            ByteBuffer value = cell.buffer();
            out.writeInt(value.remaining());
            out.write(value.duplicate());
            count++;
        }
        patchInt(out, countAt, count);
    }

    private static void writeDeletion(DataOutputBuffer out, DeletionTime deletion) throws IOException
    {
        out.writeLong(deletion.markedForDeleteAt());
        out.writeLong(deletion.localDeletionTime());
    }

    private static void patchInt(DataOutputBuffer out, int at, int value)
    {
        byte[] data = out.getData();
        data[at] = (byte) (value >>> 24);
        data[at + 1] = (byte) (value >>> 16);
        data[at + 2] = (byte) (value >>> 8);
        data[at + 3] = (byte) value;
    }
}
