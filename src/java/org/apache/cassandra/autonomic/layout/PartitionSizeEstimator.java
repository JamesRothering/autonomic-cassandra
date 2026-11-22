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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * The cheap on-disk size of one partition on this node (ADR-043, docs/spikes/partition-size-estimate.md): for each live
 * sstable that holds the key, the data file position of the next key minus the position of this key, both from the
 * partition index and with no read of the data file; plus {@link Row#dataSize()} of the key's rows in the live
 * memtables, which has no per-partition counter and is walked, at most {@link #MEMTABLE_WALK_MAX_ROWS} rows.
 *
 * <p>A sum over fragments, so an upper bound of the live data until compaction (overwrites and deletes count), and
 * in serialized bytes, which is up to 2.5x less than {@link PartitionProbe.Result#estimatedBytes()} for narrow rows.
 * It is a trigger for the probe, never a verdict.
 */
public final class PartitionSizeEstimator
{
    /** Rows of the key's memtable part that are walked; a partition with more in the memtable is counted for these only. */
    public static final int MEMTABLE_WALK_MAX_ROWS = 50_000;

    private PartitionSizeEstimator()
    {
    }

    /** Bytes of the key's sstable fragments plus its memtable rows; 0 if the node holds nothing of the key. */
    public static long estimate(ColumnFamilyStore cfs, DecoratedKey key)
    {
        long bytes = 0;
        try (ColumnFamilyStore.RefViewFragment view = cfs.selectAndReference(View.select(SSTableSet.LIVE, key)))
        {
            for (SSTableReader sstable : view.sstables)
            {
                long start = sstable.getPosition(key, SSTableReader.Operator.EQ, false);
                if (start < 0)
                    continue;
                long next = sstable.getPosition(key, SSTableReader.Operator.GT, false);
                long end = next < 0 ? sstable.uncompressedLength() : next;
                bytes += Math.max(0, end - start);
            }
            int walked = 0;
            for (Memtable memtable : view.memtables)
            {
                try (UnfilteredRowIterator rows = memtable.rowIterator(key))
                {
                    if (rows == null)
                        continue;
                    while (walked < MEMTABLE_WALK_MAX_ROWS && rows.hasNext())
                    {
                        Unfiltered u = rows.next();
                        if (u.isRow())
                        {
                            walked++;
                            bytes += ((Row) u).dataSize();
                        }
                    }
                }
            }
        }
        return bytes;
    }
}
