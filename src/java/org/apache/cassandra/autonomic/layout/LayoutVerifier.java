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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.LongSupplier;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Equivalence verification of one partition in phase VERIFY (design 4.6): the rows of the base table and the rows of
 * the overflow table, read in pages in clustering order, are compared row by row; a row that differs, is missing or is
 * extra is re-read on its own after a delay, up to {@link #recheckAttempts} times in all, to let in-flight dual writes
 * settle; what still differs after that is a mismatch.
 *
 * <p>The three kinds of mismatch are reported apart (see {@link Kind}); this class does not decide what any of them
 * means for the migration.
 *
 * <p>Verification is <em>steppable</em>: {@link #step} does a bounded amount of work, one page of each table or one
 * batch of re-reads, never sleeps, and keeps all of its progress in this object, so an actuator can run it in steps
 * under a step timeout and lose nothing but the in-memory state if it is abandoned. {@link #nextDueMillis} tells when
 * the next step has something to do if the previous one returned {@link Status#RUNNING} without work left.
 *
 * <p>Both tables are read with the same {@code nowInSec} while paging, so that a cell that expires during a long
 * verification expires on both sides at once; a re-read uses the time of the re-read, again on both sides.
 *
 * <p>Tombstones are not compared: reads are filtered, so verification cannot see them (design 4.6).
 *
 * <p>Not thread safe; one thread calls {@link #step}.
 */
public final class LayoutVerifier
{
    public static final int DEFAULT_PAGE_ROWS = 1000;
    public static final int DEFAULT_RECHECK_ATTEMPTS = 3;
    public static final long DEFAULT_RECHECK_DELAY_MILLIS = 2000;
    /** How many mismatches are kept for the report; the counts are exact. */
    public static final int SAMPLE_LIMIT = 20;
    /** Rows that differ at first sight and wait for a re-read; beyond this the verification fails without waiting. */
    public static final int MAX_SUSPECTS = 1000;

    /** One of the two tables as the verifier reads it: logical rows, in clustering order, filtered. */
    public interface Reader
    {
        /** Up to {@code limit} rows with a clustering after {@code after} (all rows if null), as of {@code nowInSec}. */
        List<Row> page(Clustering<?> after, int limit, long nowInSec);

        /** The row with this clustering as of {@code nowInSec}, or null if there is none. */
        Row row(Clustering<?> clustering, long nowInSec);
    }

    public enum Kind
    {
        /** The row is in both tables and its content differs. */
        CONTENT_DIFFERS,
        /** The row is in the base table and not in the overflow table. */
        MISSING_FROM_OVERFLOW,
        /**
         * The row is in the overflow table and not in the base table. Arises legitimately: a write of class C1 whose
         * overflow copy succeeded and whose base copy did not was never acknowledged.
         */
        MISSING_FROM_BASE
    }

    public enum Status
    {
        RUNNING,
        /** Every row was compared and none is a mismatch. */
        PASSED,
        /** Done, with at least one mismatch (or too many rows to wait for). */
        FAILED
    }

    public static final class Mismatch
    {
        public final Kind kind;
        public final Clustering<?> clustering;
        /** What differed, as of the last look. */
        public final String detail;

        Mismatch(Kind kind, Clustering<?> clustering, String detail)
        {
            this.kind = kind;
            this.clustering = clustering;
            this.detail = detail;
        }

        @Override
        public String toString()
        {
            return kind + " " + clustering + ": " + detail;
        }
    }

    /** A row that differed at first sight. */
    private static final class Suspect
    {
        final Clustering<?> clustering;
        int attempts = 1;
        long dueMillis;
        Kind kind;
        String detail;

        Suspect(Clustering<?> clustering, Kind kind, String detail, long dueMillis)
        {
            this.clustering = clustering;
            this.kind = kind;
            this.detail = detail;
            this.dueMillis = dueMillis;
        }
    }

    private final ClusteringComparator comparator;
    private final Reader base;
    private final Reader overflow;
    private final int pageRows;
    private final int recheckAttempts;
    private final long recheckDelayMillis;
    private final LongSupplier clockMillis;
    private final long scanNowInSec;

    private Clustering<?> cursor;
    private boolean scanned;
    private final Map<Clustering<?>, Suspect> suspects;
    private final List<Mismatch> samples = new ArrayList<>();
    private final long[] counts = new long[Kind.values().length];
    private long rowsCompared;
    private long rechecked;
    private long settled;
    private boolean overflowed;
    private Status status = Status.RUNNING;

    public LayoutVerifier(ClusteringComparator comparator, Reader base, Reader overflow, int pageRows, int recheckAttempts, long recheckDelayMillis,
                          LongSupplier clockMillis)
    {
        if (pageRows < 1 || recheckAttempts < 1)
            throw new IllegalArgumentException("page rows and re-check attempts must be positive");
        this.comparator = comparator;
        this.base = base;
        this.overflow = overflow;
        this.pageRows = pageRows;
        this.recheckAttempts = recheckAttempts;
        this.recheckDelayMillis = recheckDelayMillis;
        this.clockMillis = clockMillis;
        this.scanNowInSec = clockMillis.getAsLong() / 1000;
        this.suspects = new LinkedHashMap<>();
    }

    public Status status()
    {
        return status;
    }

    /**
     * Does one bounded unit of work and returns the status. Re-reads that are due go first, then the next page of both
     * tables; once the scan is complete, only the re-reads that are not yet due are left, and the step does nothing
     * until {@link #nextDueMillis}.
     */
    public Status step()
    {
        if (status != Status.RUNNING)
            return status;
        long now = clockMillis.getAsLong();
        if (recheckDue(now))
            return status;
        if (!scanned && !overflowed)
        {
            scanPage(now);
            return finishIfDone();
        }
        return finishIfDone();
    }

    /** The time at which a step has work again if the last one had none; 0 if it has work now or the verification is over. */
    public long nextDueMillis()
    {
        if (status != Status.RUNNING || !scanned || suspects.isEmpty())
            return 0;
        long due = Long.MAX_VALUE;
        for (Suspect s : suspects.values())
            due = Math.min(due, s.dueMillis);
        return due;
    }

    private Status finishIfDone()
    {
        if ((scanned || overflowed) && suspects.isEmpty())
            status = total() == 0 ? Status.PASSED : Status.FAILED;
        else if (overflowed)
            status = Status.FAILED;
        return status;
    }

    private long total()
    {
        long total = 0;
        for (long c : counts)
            total += c;
        return total + (overflowed ? 1 : 0);
    }

    // ---- the scan ----

    private void scanPage(long now)
    {
        List<Row> b = base.page(cursor, pageRows, scanNowInSec);
        List<Row> o = overflow.page(cursor, pageRows, scanNowInSec);
        // a table whose page is full may have more rows after its last one; what lies beyond the last row of a full page
        // of one table is not comparable yet, so this step compares up to the smaller of the two, inclusive
        Clustering<?> end = null;
        if (b.size() >= pageRows)
            end = b.get(b.size() - 1).clustering();
        if (o.size() >= pageRows)
        {
            Clustering<?> last = o.get(o.size() - 1).clustering();
            if (end == null || comparator.compare(last, end) < 0)
                end = last;
        }

        int i = 0;
        int j = 0;
        while (i < b.size() || j < o.size())
        {
            Row br = i < b.size() ? b.get(i) : null;
            Row or = j < o.size() ? o.get(j) : null;
            int c = br == null ? 1 : or == null ? -1 : comparator.compare(br.clustering(), or.clustering());
            Clustering<?> at = (c <= 0 ? br : or).clustering();
            if (end != null && comparator.compare(at, end) > 0)
                break;
            rowsCompared++;
            if (c < 0)
            {
                i++;
                suspect(at, Kind.MISSING_FROM_OVERFLOW, "present in the base table only", now);
            }
            else if (c > 0)
            {
                j++;
                suspect(at, Kind.MISSING_FROM_BASE, "present in the overflow table only", now);
            }
            else
            {
                i++;
                j++;
                String diff = diff(br, or);
                if (diff != null)
                    suspect(at, Kind.CONTENT_DIFFERS, diff, now);
            }
            if (overflowed)
                return;
        }
        if (end == null)
            scanned = true;
        else
            cursor = end;
    }

    private void suspect(Clustering<?> clustering, Kind kind, String detail, long now)
    {
        if (suspects.size() >= MAX_SUSPECTS)
        {
            overflowed = true;
            return;
        }
        suspects.put(clustering, new Suspect(clustering, kind, detail, now + recheckDelayMillis));
    }

    // ---- the re-reads ----

    /** Re-reads the suspects that are due, at most one page of them. True if it did. */
    private boolean recheckDue(long now)
    {
        List<Suspect> due = new ArrayList<>();
        for (Suspect s : suspects.values())
        {
            if (s.dueMillis <= now)
            {
                due.add(s);
                if (due.size() >= pageRows)
                    break;
            }
        }
        if (due.isEmpty())
            return false;
        long nowInSec = now / 1000;
        for (Suspect s : due)
        {
            rechecked++;
            Row br = base.row(s.clustering, nowInSec);
            Row or = overflow.row(s.clustering, nowInSec);
            Kind kind;
            String detail;
            if (br == null && or == null)
            {
                kind = null;
                detail = null;
            }
            else if (or == null)
            {
                kind = Kind.MISSING_FROM_OVERFLOW;
                detail = "present in the base table only";
            }
            else if (br == null)
            {
                kind = Kind.MISSING_FROM_BASE;
                detail = "present in the overflow table only";
            }
            else
            {
                detail = diff(br, or);
                kind = detail == null ? null : Kind.CONTENT_DIFFERS;
            }

            if (kind == null)
            {
                suspects.remove(s.clustering);
                settled++;
                continue;
            }
            s.kind = kind;
            s.detail = detail;
            if (++s.attempts >= recheckAttempts)
            {
                suspects.remove(s.clustering);
                counts[kind.ordinal()]++;
                if (samples.size() < SAMPLE_LIMIT)
                    samples.add(new Mismatch(kind, s.clustering, detail));
            }
            else
            {
                s.dueMillis = now + recheckDelayMillis;
            }
        }
        finishIfDone();
        return true;
    }

    // ---- the report ----

    /** The mismatches that were confirmed, at most {@link #SAMPLE_LIMIT}, in the order they were confirmed. */
    public List<Mismatch> mismatches()
    {
        return new ArrayList<>(samples);
    }

    public long count(Kind kind)
    {
        return counts[kind.ordinal()];
    }

    /** Rows that differed at first sight and did not at a re-read. */
    public long settled()
    {
        return settled;
    }

    public long rowsCompared()
    {
        return rowsCompared;
    }

    public long rechecked()
    {
        return rechecked;
    }

    /** True if there were more rows to re-read than {@link #MAX_SUSPECTS}; the verification failed without waiting for them. */
    public boolean tooManyDifferences()
    {
        return overflowed;
    }

    public String summary()
    {
        return status + ": " + rowsCompared + " rows compared, " + counts[Kind.CONTENT_DIFFERS.ordinal()] + " content mismatches, "
               + counts[Kind.MISSING_FROM_OVERFLOW.ordinal()] + " missing from the overflow table, " + counts[Kind.MISSING_FROM_BASE.ordinal()]
               + " missing from the base table, " + settled + " settled at a re-read" + (overflowed ? ", too many differences" : "");
    }

    // ---- comparing rows ----

    /** Null if the rows are the same in everything that verification sees; else the first difference. */
    static String diff(Row a, Row b)
    {
        if (!a.clustering().equals(b.clustering()))
            return "clustering";
        if (a.primaryKeyLivenessInfo().timestamp() != b.primaryKeyLivenessInfo().timestamp())
            return "row liveness timestamp " + a.primaryKeyLivenessInfo().timestamp() + " / " + b.primaryKeyLivenessInfo().timestamp();
        if (a.primaryKeyLivenessInfo().ttl() != b.primaryKeyLivenessInfo().ttl())
            return "row liveness ttl " + a.primaryKeyLivenessInfo().ttl() + " / " + b.primaryKeyLivenessInfo().ttl();
        if (a.primaryKeyLivenessInfo().localExpirationTime() != b.primaryKeyLivenessInfo().localExpirationTime())
            return "row liveness expiry " + a.primaryKeyLivenessInfo().localExpirationTime() + " / " + b.primaryKeyLivenessInfo().localExpirationTime();
        if (!a.deletion().equals(b.deletion()))
            return "row deletion " + a.deletion() + " / " + b.deletion();

        Iterator<ColumnData> ia = a.columnData().iterator();
        Iterator<ColumnData> ib = b.columnData().iterator();
        while (ia.hasNext() || ib.hasNext())
        {
            if (!ia.hasNext() || !ib.hasNext())
                return "columns " + (ia.hasNext() ? ia.next().column().name : ib.next().column().name) + " in one row only";
            ColumnData ca = ia.next();
            ColumnData cb = ib.next();
            if (!ca.column().name.equals(cb.column().name))
                return "columns " + ca.column().name + " / " + cb.column().name;
            String name = ca.column().name.toString();
            if (ca instanceof ComplexColumnData != cb instanceof ComplexColumnData)
                return "column " + name + " is complex in one row only";
            if (ca instanceof ComplexColumnData)
            {
                ComplexColumnData xa = (ComplexColumnData) ca;
                ComplexColumnData xb = (ComplexColumnData) cb;
                if (!xa.complexDeletion().equals(xb.complexDeletion()))
                    return "column " + name + " complex deletion " + xa.complexDeletion() + " / " + xb.complexDeletion();
                Iterator<Cell<?>> sa = xa.iterator();
                Iterator<Cell<?>> sb = xb.iterator();
                while (sa.hasNext() || sb.hasNext())
                {
                    if (!sa.hasNext() || !sb.hasNext())
                        return "column " + name + " has more cells in one row";
                    String d = diff(name, sa.next(), sb.next());
                    if (d != null)
                        return d;
                }
            }
            else
            {
                String d = diff(name, (Cell<?>) ca, (Cell<?>) cb);
                if (d != null)
                    return d;
            }
        }
        return null;
    }

    private static String diff(String column, Cell<?> a, Cell<?> b)
    {
        String where = "cell " + column + (a.path() == null || a.path().size() == 0 ? "" : "[" + a.path() + "]");
        if (a.path() == null != (b.path() == null) || (a.path() != null && !a.path().equals(b.path())))
            return where + " path";
        if (a.timestamp() != b.timestamp())
            return where + " timestamp " + a.timestamp() + " / " + b.timestamp();
        if (a.ttl() != b.ttl())
            return where + " ttl " + a.ttl() + " / " + b.ttl();
        if (a.localDeletionTime() != b.localDeletionTime())
            return where + " local deletion time " + a.localDeletionTime() + " / " + b.localDeletionTime();
        if (a.isTombstone() != b.isTombstone())
            return where + " tombstone";
        if (!a.buffer().equals(b.buffer()))
            return where + " value " + ByteBufferUtil.bytesToHex(a.buffer()) + " / " + ByteBufferUtil.bytesToHex(b.buffer());
        return null;
    }

    // ---- the tables ----

    /**
     * A verifier over the partition {@code key} of {@code logical}, which has a layout in {@code cm}, reading both
     * tables at {@code consistencyLevel}: the base table through the stock read (the phase is VERIFY, class C1), the
     * overflow table through the fan-out of design 3.4 forced to it.
     */
    public static LayoutVerifier forPartition(ClusterMetadata cm, TableId logical, ByteBuffer key, ConsistencyLevel consistencyLevel, int pageRows,
                                              int recheckAttempts, long recheckDelayMillis, LongSupplier clockMillis)
    {
        LayoutRegistry registry = LayoutRegistry.of(cm);
        RoutedKey routed = RoutedKey.of(cm, registry, logical, key);
        if (routed == null)
            throw new IllegalArgumentException("partition 0x" + ByteBufferUtil.bytesToHex(key) + " of " + logical + " has no layout");
        TableMetadata table = RoutedKey.logical(cm, routed.layout);
        DecoratedKey decorated = table.partitioner.decorateKey(key);
        return new LayoutVerifier(table.comparator, new TableReader(table, decorated, null, consistencyLevel),
                                  new TableReader(table, decorated, new Overflow(cm, routed), consistencyLevel),
                                  pageRows, recheckAttempts, recheckDelayMillis, clockMillis);
    }

    private static final class Overflow
    {
        final ClusterMetadata cm;
        final RoutedKey routed;

        Overflow(ClusterMetadata cm, RoutedKey routed)
        {
            this.cm = cm;
            this.routed = routed;
        }
    }

    private static final class TableReader implements Reader
    {
        private final TableMetadata table;
        private final DecoratedKey key;
        private final Overflow overflow;
        private final ConsistencyLevel consistencyLevel;

        TableReader(TableMetadata table, DecoratedKey key, Overflow overflow, ConsistencyLevel consistencyLevel)
        {
            this.table = table;
            this.key = key;
            this.overflow = overflow;
            this.consistencyLevel = consistencyLevel;
        }

        @Override
        public List<Row> page(Clustering<?> after, int limit, long nowInSec)
        {
            Slices slices = after == null ? Slices.ALL
                                          : Slices.with(table.comparator, Slice.make(ClusteringBound.exclusiveStartOf(after), ClusteringBound.TOP));
            return read(SinglePartitionReadCommand.create(table, nowInSec, ColumnFilter.all(table), RowFilter.none(), DataLimits.cqlLimits(limit), key,
                                                          new ClusteringIndexSliceFilter(slices, false)));
        }

        @Override
        public Row row(Clustering<?> clustering, long nowInSec)
        {
            List<Row> rows = read(SinglePartitionReadCommand.create(table, nowInSec, ColumnFilter.all(table), RowFilter.none(), DataLimits.NONE, key,
                                                                    new ClusteringIndexNamesFilter(names(clustering), false)));
            return rows.isEmpty() ? null : rows.get(0);
        }

        private NavigableSet<Clustering<?>> names(Clustering<?> clustering)
        {
            NavigableSet<Clustering<?>> names = new TreeSet<>(table.comparator);
            names.add(clustering);
            return names;
        }

        private List<Row> read(SinglePartitionReadCommand command)
        {
            Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
            List<Row> rows = new ArrayList<>();
            try (PartitionIterator partitions = overflow == null
                                                ? StorageProxy.read(SinglePartitionReadCommand.Group.one(command), consistencyLevel, requestTime)
                                                : LayoutReads.fanOut(command, overflow.routed, overflow.cm, consistencyLevel, requestTime, () -> {}))
            {
                while (partitions.hasNext())
                {
                    try (RowIterator partition = partitions.next())
                    {
                        while (partition.hasNext())
                            rows.add(partition.next());
                    }
                }
            }
            return rows;
        }
    }
}
