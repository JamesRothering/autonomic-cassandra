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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.layout.LayoutVerifier.Kind;
import org.apache.cassandra.autonomic.layout.LayoutVerifier.Status;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** T6.8: the verifier against two fake tables, with a clock that the test moves. */
public class LayoutVerifierTest
{
    private static TableMetadata table;
    private static ColumnMetadata v;
    private static ColumnMetadata w;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
        table = TableMetadata.builder("ks", "t").addPartitionKeyColumn("p", Int32Type.instance)
                             .addClusteringColumn("c", Int32Type.instance)
                             .addRegularColumn("v", UTF8Type.instance)
                             .addRegularColumn("w", UTF8Type.instance).build();
        v = table.getColumn(ByteBufferUtil.bytes("v"));
        w = table.getColumn(ByteBufferUtil.bytes("w"));
    }

    private static Clustering<?> c(int i)
    {
        return Clustering.make(ByteBufferUtil.bytes(i));
    }

    /** A row with the liveness timestamp ts, and v and w written at ts (v with a ttl if ttl > 0). */
    private static Row mk(int c, long ts, String value, int ttl)
    {
        Row.Builder b = BTreeRow.sortedBuilder();
        b.newRow(c(c));
        b.addPrimaryKeyLivenessInfo(ttl > 0 ? LivenessInfo.expiring(ts, ttl, 1_700_000_000L) : LivenessInfo.create(ts));
        b.addCell(ttl > 0 ? BufferCell.expiring(v, ts, ttl, 1_700_000_000L, ByteBufferUtil.bytes(value))
                          : BufferCell.live(v, ts, ByteBufferUtil.bytes(value)));
        b.addCell(BufferCell.live(w, ts, ByteBufferUtil.bytes("w" + c)));
        return b.build();
    }

    private static Row mk(int c)
    {
        return mk(c, 100 + c, "v" + c, 0);
    }

    /** A table held in memory, with a hook that runs before every read to change it between the looks. */
    private static final class Fake implements LayoutVerifier.Reader
    {
        final TreeMap<Integer, Row> rows = new TreeMap<>();
        int pages;
        int rowReads;
        Consumer<Fake> beforeRead = f -> {};

        Fake(int... clusterings)
        {
            for (int i : clusterings)
                rows.put(i, mk(i));
        }

        @Override
        public List<Row> page(Clustering<?> after, int limit, long nowInSec)
        {
            pages++;
            beforeRead.accept(this);
            List<Row> page = new ArrayList<>();
            for (Map.Entry<Integer, Row> e : rows.entrySet())
            {
                if (after != null && table.comparator.compare(c(e.getKey()), after) <= 0)
                    continue;
                page.add(e.getValue());
                if (page.size() == limit)
                    break;
            }
            return page;
        }

        @Override
        public Row row(Clustering<?> clustering, long nowInSec)
        {
            rowReads++;
            beforeRead.accept(this);
            return rows.get(ByteBufferUtil.toInt((ByteBuffer) clustering.bufferAt(0)));
        }
    }

    private static final int[] TEN = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    private final AtomicLong clock = new AtomicLong(1_700_000_000_000L);

    private LayoutVerifier verifier(Fake base, Fake overflow, int pageRows)
    {
        return new LayoutVerifier(table.comparator, base, overflow, pageRows, 3, 2000, clock::get);
    }

    /** Steps to the end, advancing the clock to the next time a step has work. */
    private int run(LayoutVerifier verifier)
    {
        int steps = 0;
        while (verifier.step() == Status.RUNNING)
        {
            steps++;
            assertTrue("runs away", steps < 10_000);
            long due = verifier.nextDueMillis();
            if (due > clock.get())
                clock.set(due);
        }
        return steps;
    }

    @Test
    public void equalTablesPass()
    {
        for (int pageRows : new int[]{ 1, 3, 10, 11, 1000 })
        {
            LayoutVerifier verifier = verifier(new Fake(TEN), new Fake(TEN), pageRows);
            run(verifier);
            assertEquals("page rows " + pageRows, Status.PASSED, verifier.status());
            assertEquals(10, verifier.rowsCompared());
            assertEquals(0, verifier.rechecked());
            assertTrue(verifier.mismatches().isEmpty());
        }
        LayoutVerifier empty = verifier(new Fake(), new Fake(), 5);
        run(empty);
        assertEquals(Status.PASSED, empty.status());
    }

    @Test
    public void aStepIsBoundedAndProgressIsKept()
    {
        Fake base = new Fake(TEN);
        Fake overflow = new Fake(TEN);
        LayoutVerifier verifier = verifier(base, overflow, 3);
        assertEquals(Status.RUNNING, verifier.step());
        // one step reads one page of each table
        assertEquals(1, base.pages);
        assertEquals(1, overflow.pages);
        assertEquals(3, verifier.rowsCompared());
        // the next page starts after the last row that was compared
        assertEquals(Status.RUNNING, verifier.step());
        assertEquals(6, verifier.rowsCompared());
        run(verifier);
        assertEquals(Status.PASSED, verifier.status());
        // a finished verifier does nothing
        int pages = base.pages;
        assertEquals(Status.PASSED, verifier.step());
        assertEquals(pages, base.pages);
    }

    @Test
    public void everyKindOfDifferenceIsReportedWithItsClustering()
    {
        Fake base = new Fake(TEN);
        Fake overflow = new Fake(TEN);
        overflow.rows.put(2, mk(2, 102, "other", 0));      // a differing value
        overflow.rows.put(3, mk(3, 1, "v3", 0));           // a differing timestamp, older in the target (a newer one is NEWER_IN_TARGET)
        overflow.rows.put(4, mk(4, 104, "v4", 3600));      // a differing ttl
        overflow.rows.remove(6);                            // a missing row
        overflow.rows.put(11, mk(11));                     // an extra row
        LayoutVerifier verifier = verifier(base, overflow, 4);
        run(verifier);

        assertEquals(Status.FAILED, verifier.status());
        assertEquals(3, verifier.count(Kind.CONTENT_DIFFERS));
        assertEquals(1, verifier.count(Kind.MISSING_FROM_OVERFLOW));
        assertEquals(1, verifier.count(Kind.MISSING_FROM_BASE));
        Map<Integer, LayoutVerifier.Mismatch> byRow = new HashMap<>();
        for (LayoutVerifier.Mismatch m : verifier.mismatches())
            byRow.put(ByteBufferUtil.toInt((ByteBuffer) m.clustering.bufferAt(0)), m);
        assertEquals(5, byRow.size());
        assertEquals(Kind.CONTENT_DIFFERS, byRow.get(2).kind);
        assertTrue(byRow.get(2).detail, byRow.get(2).detail.contains("value"));
        assertEquals(Kind.CONTENT_DIFFERS, byRow.get(3).kind);
        assertTrue(byRow.get(3).detail, byRow.get(3).detail.contains("timestamp"));
        assertEquals(Kind.CONTENT_DIFFERS, byRow.get(4).kind);
        assertTrue(byRow.get(4).detail, byRow.get(4).detail.contains("ttl"));
        assertEquals(Kind.MISSING_FROM_OVERFLOW, byRow.get(6).kind);
        assertEquals(Kind.MISSING_FROM_BASE, byRow.get(11).kind);
        // each is looked at the attempts it is allowed: the first look and two re-reads
        assertEquals(5 * 2, verifier.rechecked());
        assertEquals(0, verifier.settled());
    }

    @Test
    public void aRowThatChangedBetweenTheFirstReadAndTheRecheckPasses()
    {
        Fake base = new Fake(TEN);
        Fake overflow = new Fake(TEN);
        overflow.rows.put(5, mk(5, 105, "stale", 0));
        overflow.rows.remove(8);
        base.rows.put(12, mk(12)); // the base-only row of a write in flight
        // the dual write lands before the re-read
        overflow.beforeRead = f -> {
            if (f.rowReads > 0)
            {
                f.rows.put(5, mk(5));
                f.rows.put(8, mk(8));
                f.rows.put(12, mk(12));
            }
        };
        LayoutVerifier verifier = verifier(base, overflow, 100);
        run(verifier);
        assertEquals(verifier.summary(), Status.PASSED, verifier.status());
        assertEquals(3, verifier.settled());
        assertEquals(3, verifier.rechecked());
        assertTrue(verifier.mismatches().isEmpty());
    }

    @Test
    public void aRowThatSettlesAtTheSecondRecheckPasses()
    {
        Fake base = new Fake(TEN);
        Fake overflow = new Fake(TEN);
        overflow.rows.put(5, mk(5, 105, "stale", 0));
        overflow.beforeRead = f -> {
            if (f.rowReads > 1)
                f.rows.put(5, mk(5));
        };
        LayoutVerifier verifier = verifier(base, overflow, 100);
        run(verifier);
        assertEquals(Status.PASSED, verifier.status());
        assertEquals(2, verifier.rechecked());
    }

    @Test
    public void reReadsWaitForTheirDelay()
    {
        Fake base = new Fake(TEN);
        Fake overflow = new Fake(TEN);
        overflow.rows.remove(3);
        LayoutVerifier verifier = verifier(base, overflow, 100);
        assertEquals(Status.RUNNING, verifier.step()); // the scan
        long first = verifier.nextDueMillis();
        assertEquals(clock.get() + 2000, first);
        // nothing to do before it is due, and nothing is read
        int reads = overflow.rowReads;
        clock.addAndGet(1999);
        assertEquals(Status.RUNNING, verifier.step());
        assertEquals(reads, overflow.rowReads);
        clock.addAndGet(1);
        assertEquals(Status.RUNNING, verifier.step());
        assertEquals(reads + 1, overflow.rowReads);
        // the next attempt is a delay later
        assertEquals(clock.get() + 2000, verifier.nextDueMillis());
        run(verifier);
        assertEquals(Status.FAILED, verifier.status());
        assertEquals(1, verifier.count(Kind.MISSING_FROM_OVERFLOW));
    }

    @Test
    public void aRowThatWasMissingAndIsExtraAtTheRecheckIsReportedAsWhatItIsThen()
    {
        Fake base = new Fake(TEN);
        Fake overflow = new Fake(TEN);
        overflow.rows.remove(3);
        // the base copy goes away while it waits: now only the overflow table has it? no: both lack it, so it settles
        base.beforeRead = f -> {
            if (f.rowReads > 0)
                f.rows.remove(3);
        };
        LayoutVerifier verifier = verifier(base, overflow, 100);
        run(verifier);
        assertEquals(Status.PASSED, verifier.status());
        assertEquals(1, verifier.settled());
    }

    @Test
    public void pagesOfDifferentFillAreComparedOnlyUpToTheSmallerOne()
    {
        // the overflow table has rows in the gaps of the base table, so the pages end at different clusterings
        Fake base = new Fake(1, 3, 5, 7, 9, 11, 13);
        Fake overflow = new Fake(1, 2, 3, 5, 7, 9, 11, 13);
        LayoutVerifier verifier = verifier(base, overflow, 3);
        run(verifier);
        assertEquals(Status.FAILED, verifier.status());
        assertEquals(1, verifier.count(Kind.MISSING_FROM_BASE));
        assertEquals(0, verifier.count(Kind.MISSING_FROM_OVERFLOW));
        assertEquals(0, verifier.count(Kind.CONTENT_DIFFERS));
        assertEquals(2, ByteBufferUtil.toInt((ByteBuffer) verifier.mismatches().get(0).clustering.bufferAt(0)));
        assertEquals(8, verifier.rowsCompared());
    }

    @Test
    public void tooManyDifferencesFailWithoutWaiting()
    {
        Fake base = new Fake();
        for (int i = 0; i < LayoutVerifier.MAX_SUSPECTS + 50; i++)
            base.rows.put(i, mk(i));
        LayoutVerifier verifier = verifier(base, new Fake(), 5000);
        int steps = 0;
        while (verifier.step() == Status.RUNNING && steps++ < 10)
            ;
        assertEquals(Status.FAILED, verifier.status());
        assertTrue(verifier.tooManyDifferences());
        assertEquals("no time passed", 1_700_000_000_000L, clock.get());
    }

    @Test
    public void samplesAreCappedButCountsAreExact()
    {
        Fake base = new Fake();
        Fake overflow = new Fake();
        for (int i = 0; i < 60; i++)
        {
            base.rows.put(i, mk(i));
            overflow.rows.put(i, mk(i, 1, "x", 0));
        }
        LayoutVerifier verifier = verifier(base, overflow, 1000);
        run(verifier);
        assertEquals(60, verifier.count(Kind.CONTENT_DIFFERS));
        assertEquals(LayoutVerifier.SAMPLE_LIMIT, verifier.mismatches().size());
    }

    // ---- roles, tolerance and NEWER_IN_TARGET (T6.17e) ----

    private LayoutVerifier verifier(Fake base, Fake overflow, LayoutVerifier.Direction direction, java.util.Set<Kind> tolerated)
    {
        return new LayoutVerifier(table.comparator, base, overflow, direction, tolerated, 4, 3, 2000, clock::get);
    }

    private LayoutVerifier verifier(Fake base, Fake overflow, LayoutVerifier.Direction direction)
    {
        return verifier(base, overflow, direction, LayoutVerifier.defaultTolerated(direction));
    }

    /** The five differences, with the newer cell in the base table (row 2) and in the overflow table (row 3). */
    private static Fake[] differing()
    {
        Fake base = new Fake(TEN);
        Fake overflow = new Fake(TEN);
        overflow.rows.put(2, mk(2, 102, "other", 0));      // CONTENT_DIFFERS: the same timestamps, another value
        base.rows.put(3, mk(3, 500, "v3", 0));             // newer in the base table
        overflow.rows.put(4, mk(4, 900, "v4", 0));         // newer in the overflow table
        overflow.rows.remove(6);                            // only in the base table
        overflow.rows.put(11, mk(11));                     // only in the overflow table
        return new Fake[]{ base, overflow };
    }

    @Test
    public void everyKindIsCountedInTheForwardDirectionAndTheTargetIsTheOverflowTable()
    {
        Fake[] t = differing();
        LayoutVerifier verifier = verifier(t[0], t[1], LayoutVerifier.Direction.FORWARD);
        run(verifier);
        assertEquals(Status.FAILED, verifier.status());
        // row 3 is newer in the authoritative table: a difference; row 4 is newer in the target: tolerated
        assertEquals(2, verifier.count(Kind.CONTENT_DIFFERS));
        assertEquals(1, verifier.count(Kind.NEWER_IN_TARGET));
        assertEquals(1, verifier.count(Kind.MISSING_FROM_OVERFLOW));
        assertEquals(1, verifier.count(Kind.MISSING_FROM_BASE));
        assertEquals("only NEWER_IN_TARGET is tolerated by default", 1, verifier.toleratedCount());
        assertTrue(verifier.isTolerated(Kind.NEWER_IN_TARGET));
        assertFalse(verifier.isTolerated(Kind.MISSING_FROM_OVERFLOW));
        Map<Integer, Kind> kinds = new HashMap<>();
        for (LayoutVerifier.Mismatch m : verifier.mismatches())
            kinds.put(ByteBufferUtil.toInt((ByteBuffer) m.clustering.bufferAt(0)), m.kind);
        assertEquals(Kind.CONTENT_DIFFERS, kinds.get(2));
        assertEquals(Kind.CONTENT_DIFFERS, kinds.get(3));
        assertEquals(Kind.NEWER_IN_TARGET, kinds.get(4));
        assertEquals(Kind.MISSING_FROM_OVERFLOW, kinds.get(6));
        assertEquals(Kind.MISSING_FROM_BASE, kinds.get(11));
        assertTrue(verifier.summary(), verifier.summary().contains("1 newer in the target"));
    }

    @Test
    public void everyKindIsCountedInTheReverseDirectionAndTheTargetIsTheBaseTable()
    {
        Fake[] t = differing();
        LayoutVerifier verifier = verifier(t[0], t[1], LayoutVerifier.Direction.REVERSE);
        run(verifier);
        assertEquals(Status.FAILED, verifier.status());
        // now row 3, newer in the base table, is the tolerated one, and row 4 a difference
        assertEquals(2, verifier.count(Kind.CONTENT_DIFFERS));
        assertEquals(1, verifier.count(Kind.NEWER_IN_TARGET));
        assertEquals(1, verifier.count(Kind.MISSING_FROM_OVERFLOW));
        assertEquals(1, verifier.count(Kind.MISSING_FROM_BASE));
        assertEquals("a base-only row and a newer base cell are tolerated", 2, verifier.toleratedCount());
        Map<Integer, Kind> kinds = new HashMap<>();
        for (LayoutVerifier.Mismatch m : verifier.mismatches())
            kinds.put(ByteBufferUtil.toInt((ByteBuffer) m.clustering.bufferAt(0)), m.kind);
        assertEquals(Kind.NEWER_IN_TARGET, kinds.get(3));
        assertEquals(Kind.CONTENT_DIFFERS, kinds.get(4));
    }

    @Test
    public void toleratedKindsAreCountedAndDoNotFailTheVerification()
    {
        for (LayoutVerifier.Direction direction : LayoutVerifier.Direction.values())
        {
            Fake base = new Fake(TEN);
            Fake overflow = new Fake(TEN);
            // the target's copy of row 5 is newer
            (direction == LayoutVerifier.Direction.FORWARD ? overflow : base).rows.put(5, mk(5, 700, "v5", 0));
            LayoutVerifier verifier = verifier(base, overflow, direction);
            run(verifier);
            assertEquals(direction.name(), Status.PASSED, verifier.status());
            assertEquals(1, verifier.count(Kind.NEWER_IN_TARGET));
            assertEquals(1, verifier.toleratedCount());
            assertTrue(verifier.mismatches().get(0).detail, verifier.mismatches().get(0).detail.contains("timestamp"));

            // the same difference with an empty tolerance set fails
            Fake base2 = new Fake(TEN);
            Fake overflow2 = new Fake(TEN);
            (direction == LayoutVerifier.Direction.FORWARD ? overflow2 : base2).rows.put(5, mk(5, 700, "v5", 0));
            LayoutVerifier strict = verifier(base2, overflow2, direction, java.util.Set.of());
            run(strict);
            assertEquals(direction.name(), Status.FAILED, strict.status());
            assertEquals(1, strict.count(Kind.NEWER_IN_TARGET));
            assertEquals(0, strict.toleratedCount());
        }
        // a one-sided row of the tolerated kind passes too, in the direction it belongs to
        Fake base = new Fake(TEN);
        Fake overflow = new Fake(TEN);
        overflow.rows.remove(7);
        LayoutVerifier reverse = verifier(base, overflow, LayoutVerifier.Direction.REVERSE);
        run(reverse);
        assertEquals(Status.PASSED, reverse.status());
        assertEquals(1, reverse.count(Kind.MISSING_FROM_OVERFLOW));
        LayoutVerifier forward = verifier(base, overflow, LayoutVerifier.Direction.FORWARD);
        run(forward);
        assertEquals(Status.FAILED, forward.status());
    }

    @Test
    public void aCellThatIsNewerInTheTargetIsOnlyTolerableIfNothingElseDiffers()
    {
        Row old = mk(1);
        // v newer in the target, w the same: tolerable
        assertTrue(LayoutVerifier.newerInTarget(old, mk(1, 101, "v1", 0)) == false);
        Row.Builder b = BTreeRow.sortedBuilder();
        b.newRow(c(1));
        b.addPrimaryKeyLivenessInfo(LivenessInfo.create(101));
        b.addCell(BufferCell.live(v, 500, ByteBufferUtil.bytes("new")));
        b.addCell(BufferCell.live(w, 101, ByteBufferUtil.bytes("w1")));
        Row newer = b.build();
        assertTrue(LayoutVerifier.newerInTarget(old, newer));
        assertFalse("the other way round the authoritative row is the newer one", LayoutVerifier.newerInTarget(newer, old));
        // v newer, w older in the target: a real difference
        Row.Builder m = BTreeRow.sortedBuilder();
        m.newRow(c(1));
        m.addPrimaryKeyLivenessInfo(LivenessInfo.create(101));
        m.addCell(BufferCell.live(v, 500, ByteBufferUtil.bytes("new")));
        m.addCell(BufferCell.live(w, 5, ByteBufferUtil.bytes("old")));
        assertFalse(LayoutVerifier.newerInTarget(old, m.build()));
        // a column that only the target has
        Row.Builder only = BTreeRow.sortedBuilder();
        only.newRow(c(1));
        only.addPrimaryKeyLivenessInfo(LivenessInfo.create(101));
        only.addCell(BufferCell.live(v, 101, ByteBufferUtil.bytes("v1")));
        Row without = only.build();
        assertTrue(LayoutVerifier.newerInTarget(without, old));
        assertFalse("a column only the authoritative row has is a difference", LayoutVerifier.newerInTarget(old, without));
        // equal rows differ in nothing
        assertFalse(LayoutVerifier.newerInTarget(old, mk(1)));
    }

    // ---- row comparison ----

    @Test
    public void rowsAreComparedInEverythingVerificationSees()
    {
        assertNull(LayoutVerifier.diff(mk(1), mk(1)));
        assertNotNull(LayoutVerifier.diff(mk(1), mk(1, 102, "v1", 0)));
        assertNotNull(LayoutVerifier.diff(mk(1), mk(1, 101, "x", 0)));
        assertTrue(LayoutVerifier.diff(mk(1, 101, "v1", 100), mk(1, 101, "v1", 200)).contains("ttl"));

        // a cell that one row has and the other lacks
        Row.Builder only = BTreeRow.sortedBuilder();
        only.newRow(c(1));
        only.addPrimaryKeyLivenessInfo(LivenessInfo.create(101));
        only.addCell(BufferCell.live(v, 101, ByteBufferUtil.bytes("v1")));
        assertNotNull(LayoutVerifier.diff(mk(1), only.build()));

        // an expiry that differs while the ttl is the same
        Row.Builder expiring = BTreeRow.sortedBuilder();
        expiring.newRow(c(1));
        expiring.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(101, 100, 1_700_000_001L));
        expiring.addCell(BufferCell.expiring(v, 101, 100, 1_700_000_001L, ByteBufferUtil.bytes("v1")));
        expiring.addCell(BufferCell.live(w, 101, ByteBufferUtil.bytes("w1")));
        String diff = LayoutVerifier.diff(mk(1, 101, "v1", 100), expiring.build());
        assertNotNull(diff);
        assertTrue(diff, diff.contains("expiry") || diff.contains("local deletion"));
    }
}
