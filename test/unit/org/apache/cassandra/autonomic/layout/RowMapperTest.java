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
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** {@link RowMapper}: the mapping of design 3.3 for rows, range tombstones, partition and complex deletions. */
public class RowMapperTest extends CQLTester
{
    private static final UUID LAYOUT_ID = UUID.fromString("1a2b3c4d-0000-4000-8000-000000000002");
    private static final int BUCKETS = 4;
    private static final int INCARNATION = 3;

    private TableMetadata logical;
    private TableMetadata physical;
    private TableLayout layout;

    private void setUp(String definition) throws Throwable
    {
        String t = createTable("CREATE TABLE %s (" + definition + ")");
        logical = Schema.instance.getTableMetadata(keyspace(), t);
        TableId id = TableId.generate();
        schemaChange(PhysicalTables.derive(logical, LAYOUT_ID, id));
        physical = Schema.instance.getTableMetadata(keyspace(), PhysicalTables.physicalName(t, LAYOUT_ID));
        LayoutHeader header = new LayoutHeader(LAYOUT_ID, logical.id, physical.id, keyspace(), t, physical.name,
                                               TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, BUCKETS,
                                               Math.min(2, logical.clusteringColumns().size()));
        layout = new TableLayout(header, INCARNATION + 1, Map.of(), 1);
    }

    private void setUpComposite() throws Throwable
    {
        setUp("p int, q text, c1 int, c2 text, v int, v2 text, l list<int>, s set<int>, m map<text, int>, PRIMARY KEY ((p, q), c1, c2)");
    }

    private PartitionUpdate.SimpleBuilder builder(long timestamp, int ttl)
    {
        return PartitionUpdate.simpleBuilder(logical, 7, "seven").timestamp(timestamp).ttl(ttl);
    }

    private PartitionUpdate singleRow()
    {
        PartitionUpdate.SimpleBuilder b = builder(5, 0);
        b.row(1, "a").add("v", 1);
        return b.build();
    }

    // ---- comparison, cell for cell ----

    /** Everything a PartitionUpdate says, as text: deletions with their times, and every cell with its metadata. */
    private static String describe(PartitionUpdate update)
    {
        TableMetadata metadata = update.metadata();
        StringBuilder sb = new StringBuilder();
        sb.append("partitionDeletion=").append(update.partitionLevelDeletion()).append('\n');
        for (Iterator<RangeTombstone> it = update.deletionInfo().rangeIterator(false); it.hasNext(); )
        {
            RangeTombstone rt = it.next();
            sb.append("range ").append(rt.deletedSlice().toString(metadata.comparator)).append(' ').append(rt.deletionTime()).append('\n');
        }
        for (Row row : update)
        {
            sb.append("row ").append(row.clustering().toString(metadata)).append(" liveness=").append(row.primaryKeyLivenessInfo())
              .append(" deletion=").append(row.deletion()).append('\n');
            for (ColumnData data : row)
            {
                sb.append("  ").append(data.column().name);
                if (data instanceof ComplexColumnData)
                {
                    ComplexColumnData complex = (ComplexColumnData) data;
                    sb.append(" complexDeletion=").append(complex.complexDeletion());
                    for (Cell<?> cell : complex)
                        sb.append(' ').append(cell(cell));
                }
                else
                {
                    sb.append(' ').append(cell((Cell<?>) data));
                }
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    private static String cell(Cell<?> cell)
    {
        return "[path=" + (cell.path() == null ? "" : ByteBufferUtil.bytesToHex(cell.path().get(0))) + " value=" + ByteBufferUtil.bytesToHex(cell.buffer())
               + " ts=" + cell.timestamp() + " ttl=" + cell.ttl() + " ldt=" + cell.localDeletionTime() + " tombstone=" + cell.isTombstone() + ']';
    }

    private PartitionUpdate roundTrip(Map<Integer, PartitionUpdate> physicalUpdates)
    {
        List<PartitionUpdate> back = new ArrayList<>();
        for (PartitionUpdate update : physicalUpdates.values())
            back.add(RowMapper.toLogical(update, logical));
        return PartitionUpdate.merge(back);
    }

    private void assertRoundTrip(PartitionUpdate original, Map<Integer, PartitionUpdate> mapped)
    {
        PartitionUpdate back = roundTrip(mapped);
        assertEquals(original.partitionKey(), back.partitionKey());
        assertEquals(describe(original), describe(back));
    }

    // ---- rows ----

    @Test
    public void rowsGoToTheBucketOfTheirClusteringWithEveryCellCopied() throws Throwable
    {
        setUpComposite();
        List<PartitionUpdate> parts = new ArrayList<>();
        PartitionUpdate.SimpleBuilder plain = builder(1000, 0);
        for (int i = 0; i < 40; i++)
            plain.row(i, "c" + i).add("v", i).add("v2", "text" + i).add("l", List.of(i, i + 1)).add("s", java.util.Set.of(i)).add("m", Map.of("k" + i, i));
        parts.add(plain.build());
        // other timestamps and TTLs, a collection overwrite (a complex deletion), cell and row deletions, an update without liveness
        PartitionUpdate.SimpleBuilder ttl = builder(2000, 100000);
        for (int i = 0; i < 10; i++)
            ttl.row(i, "c" + i).add("v", i * 10).add("m", Map.of("n" + i, 1));
        parts.add(ttl.build());
        PartitionUpdate.SimpleBuilder deletes = builder(3000, 0);
        deletes.row(11, "c11").delete("v2");
        deletes.row(12, "c12").delete();
        deletes.row(13, "c13").delete("l");
        deletes.row(14, "c14").delete("m");
        parts.add(deletes.build());
        PartitionUpdate original = PartitionUpdate.merge(parts);

        Map<Integer, PartitionUpdate> mapped = RowMapper.toPhysical(original, physical, layout, INCARNATION);
        assertEquals("all buckets are in use", BUCKETS, mapped.size());

        int rows = 0;
        for (Map.Entry<Integer, PartitionUpdate> e : mapped.entrySet())
        {
            int bucket = e.getKey();
            PartitionUpdate update = e.getValue();
            // the partition key: the logical key components, then incarnation * 256 + bucket
            assertEquals(physical.id, update.metadata().id);
            DecoratedKey key = update.partitionKey();
            ByteBuffer expected = CompositeType.build(ByteBufferAccessor.instance, org.apache.cassandra.db.marshal.Int32Type.instance.decompose(7),
                                                      org.apache.cassandra.db.marshal.UTF8Type.instance.decompose("seven"),
                                                      org.apache.cassandra.db.marshal.Int32Type.instance.decompose(INCARNATION * 256 + bucket));
            assertEquals(physical.partitioner.decorateKey(expected), key);
            assertEquals(INCARNATION * 256 + bucket, RowMapper.bucketValue(physical, key));
            for (Row row : update)
            {
                rows++;
                assertEquals(row.clustering().toString(logical), bucket, BucketFunction.bucket(row.clustering(), 2, BUCKETS));
                for (ColumnData data : row)
                    assertSame("cells are labelled with the physical column", physical.getColumn(data.column().name), data.column());
            }
        }
        assertEquals(original.rowCount(), rows);

        // toLogical(toPhysical(u)) is u, cell for cell: timestamps, TTLs, local deletion times, deletions
        assertRoundTrip(original, mapped);
        // the description really sees what it should: a TTL cell, a complex deletion, a row deletion
        String text = describe(original);
        assertTrue(text, text.contains("ttl=100000") && text.contains("complexDeletion=deletedAt=3000") && text.contains("deletion=deletedAt=3000"));
    }

    @Test
    public void oneRowTouchesOneBucket() throws Throwable
    {
        setUpComposite();
        PartitionUpdate original = singleRow();
        Map<Integer, PartitionUpdate> mapped = RowMapper.toPhysical(original, physical, layout, INCARNATION);
        assertEquals(1, mapped.size());
        assertEquals(BucketFunction.bucket(original.iterator().next().clustering(), 2, BUCKETS), (int) mapped.keySet().iterator().next());
        assertRoundTrip(original, mapped);
    }

    @Test
    public void incarnationIsPartOfTheKey() throws Throwable
    {
        setUpComposite();
        PartitionUpdate original = singleRow();
        int bucket = BucketFunction.bucket(original.iterator().next().clustering(), 2, BUCKETS);
        for (int incarnation : new int[]{ 1, 2, 77, LayoutRegistry.MAX_INCARNATION - 1 })
        {
            PartitionUpdate update = RowMapper.toPhysical(original, physical, layout, incarnation).get(bucket);
            assertEquals(incarnation * 256 + bucket, RowMapper.bucketValue(physical, update.partitionKey()));
        }
    }

    // ---- deletions ----

    @Test
    public void rangeTombstoneAppearsInEveryBucket() throws Throwable
    {
        setUpComposite();
        PartitionUpdate.SimpleBuilder b = builder(4000, 0);
        b.addRangeTombstone().start(5).inclStart().end(20, "c30").exclEnd();
        b.addRangeTombstone().start(100).exclStart().end(200).inclEnd();
        b.row(1, "r").add("v", 1);
        PartitionUpdate original = b.build();
        assertEquals(2, rangeCount(original));

        Map<Integer, PartitionUpdate> mapped = RowMapper.toPhysical(original, physical, layout, INCARNATION);
        assertEquals(BUCKETS, mapped.size());
        for (int bucket = 0; bucket < BUCKETS; bucket++)
        {
            PartitionUpdate update = mapped.get(bucket);
            assertEquals("bucket " + bucket, 2, rangeCount(update));
            assertEquals(describeRanges(original), describeRanges(update));
        }
        // the row is in one bucket only
        int rowBuckets = 0;
        for (PartitionUpdate update : mapped.values())
            if (update.rowCount() > 0)
                rowBuckets++;
        assertEquals(1, rowBuckets);
        assertRoundTrip(original, mapped);
    }

    private static int rangeCount(PartitionUpdate update)
    {
        int n = 0;
        for (Iterator<RangeTombstone> it = update.deletionInfo().rangeIterator(false); it.hasNext(); it.next())
            n++;
        return n;
    }

    private static String describeRanges(PartitionUpdate update)
    {
        StringBuilder sb = new StringBuilder();
        for (Iterator<RangeTombstone> it = update.deletionInfo().rangeIterator(false); it.hasNext(); )
        {
            RangeTombstone rt = it.next();
            sb.append(rt.deletedSlice().toString(update.metadata().comparator)).append(rt.deletionTime());
        }
        return sb.toString();
    }

    @Test
    public void partitionDeletionAppearsInEveryBucket() throws Throwable
    {
        setUpComposite();
        PartitionUpdate original = builder(4500, 0).delete().build();
        Map<Integer, PartitionUpdate> mapped = RowMapper.toPhysical(original, physical, layout, INCARNATION);
        assertEquals(BUCKETS, mapped.size());
        for (PartitionUpdate update : mapped.values())
        {
            assertEquals(original.partitionLevelDeletion(), update.partitionLevelDeletion());
            assertEquals(0, update.rowCount());
        }
        assertRoundTrip(original, mapped);

        // a partition deletion with rows newer than it
        PartitionUpdate.SimpleBuilder b = builder(4600, 0);
        b.delete();
        PartitionUpdate.SimpleBuilder newer = builder(4700, 0);
        newer.row(2, "x").add("v", 9);
        newer.row(3, "y").add("v", 8);
        PartitionUpdate withRows = PartitionUpdate.merge(List.of(b.build(), newer.build()));
        Map<Integer, PartitionUpdate> mappedWithRows = RowMapper.toPhysical(withRows, physical, layout, INCARNATION);
        assertEquals(BUCKETS, mappedWithRows.size());
        assertRoundTrip(withRows, mappedWithRows);
    }

    @Test
    public void emptyUpdateMapsToNothing() throws Throwable
    {
        setUpComposite();
        PartitionUpdate empty = PartitionUpdate.emptyUpdate(logical, logical.partitioner.decorateKey(
            CompositeType.build(ByteBufferAccessor.instance, ByteBufferUtil.bytes(7), ByteBufferUtil.bytes("seven"))));
        assertEquals(0, RowMapper.toPhysical(empty, physical, layout, INCARNATION).size());
    }

    // ---- other shapes ----

    @Test
    public void singleColumnPartitionKeyAndReversedClustering() throws Throwable
    {
        setUp("k int, c1 int, c2 int, v text, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC");
        PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(logical, 99).timestamp(10);
        for (int i = 0; i < 20; i++)
            b.row(i, i % 3).add("v", "x" + i);
        b.addRangeTombstone().start(40).end(30); // c1 is clustered in descending order
        PartitionUpdate original = b.build();
        Map<Integer, PartitionUpdate> mapped = RowMapper.toPhysical(original, physical, layout, INCARNATION);
        assertEquals(BUCKETS, mapped.size());
        for (Map.Entry<Integer, PartitionUpdate> e : mapped.entrySet())
            assertEquals(INCARNATION * 256 + e.getKey(), RowMapper.bucketValue(physical, e.getValue().partitionKey()));
        assertRoundTrip(original, mapped);
    }

    @Test
    public void udtAndFrozenCollectionCells() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (x int, y text)");
        setUp("p int, c int, nu " + udt + ", fu frozen<" + udt + ">, fl frozen<list<int>>, PRIMARY KEY (p, c)");
        PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(logical, 1).timestamp(77).ttl(1000);
        for (int i = 0; i < 12; i++)
            b.row(i).add("fu", udtValue(i, "z")).add("fl", List.of(i));
        PartitionUpdate.SimpleBuilder d = PartitionUpdate.simpleBuilder(logical, 1).timestamp(88);
        d.row(3).delete("fu");

        // a non-frozen UDT is a complex column: cells with field paths, a complex deletion, an expiring cell, a cell tombstone
        ColumnMetadata nu = logical.getColumn(ByteBufferUtil.bytes("nu"));
        assertTrue(nu.isComplex());
        PartitionUpdate.Builder complex = new PartitionUpdate.Builder(logical, logical.partitioner.decorateKey(ByteBufferUtil.bytes(1)),
                                                                     RegularAndStaticColumns.of(nu), 12);
        for (int i = 0; i < 12; i++)
        {
            Row.Builder row = BTreeRow.sortedBuilder();
            row.newRow(Clustering.make(ByteBufferUtil.bytes(i)));
            row.addComplexDeletion(nu, DeletionTime.build(60, 1000));
            row.addCell(BufferCell.live(nu, 61, ByteBufferUtil.bytes(i), CellPath.create(ByteBufferUtil.bytes((short) 0))));
            row.addCell(BufferCell.expiring(nu, 62, 5000, 1_700_000_000L, ByteBufferUtil.bytes("y" + i), CellPath.create(ByteBufferUtil.bytes((short) 1))));
            complex.add(row.build());
        }
        Row.Builder gone = BTreeRow.sortedBuilder();
        gone.newRow(Clustering.make(ByteBufferUtil.bytes(40)));
        gone.addCell(BufferCell.tombstone(nu, 63, 1_700_000_000L, CellPath.create(ByteBufferUtil.bytes((short) 1))));
        complex.add(gone.build());

        PartitionUpdate original = PartitionUpdate.merge(List.of(b.build(), d.build(), complex.build()));
        assertTrue(describe(original), describe(original).contains("complexDeletion=deletedAt=60"));
        Map<Integer, PartitionUpdate> mapped = RowMapper.toPhysical(original, physical, layout, INCARNATION);
        assertEquals(BUCKETS, mapped.size());
        assertRoundTrip(original, mapped);
    }

    private ByteBuffer udtValue(int x, String y)
    {
        TupleType type = (TupleType) logical.getColumn(ByteBufferUtil.bytes("fu")).type;
        return type.pack(ByteBufferUtil.bytes(x), ByteBufferUtil.bytes(y));
    }

    @Test
    public void staticColumnsAreRefused() throws Throwable
    {
        String t = createTable("CREATE TABLE %s (p int, c int, s int static, v int, PRIMARY KEY (p, c))");
        TableMetadata withStatic = Schema.instance.getTableMetadata(keyspace(), t);
        PartitionUpdate.SimpleBuilder sb = PartitionUpdate.simpleBuilder(withStatic, 1).timestamp(1);
        sb.row().add("s", 1);
        PartitionUpdate update = sb.build();
        setUpComposite();
        try
        {
            RowMapper.toPhysical(update, physical, layout, INCARNATION);
            fail();
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("static"));
        }
    }

    @Test
    public void aColumnThatDiffersIsRefused() throws Throwable
    {
        setUpComposite();
        String t = createTable("CREATE TABLE %s (p int, q text, c1 int, c2 text, v text, PRIMARY KEY ((p, q), c1, c2))");
        TableMetadata other = Schema.instance.getTableMetadata(keyspace(), t);
        PartitionUpdate.SimpleBuilder ob = PartitionUpdate.simpleBuilder(other, 7, "seven").timestamp(1);
        ob.row(1, "a").add("v", "text");
        PartitionUpdate update = ob.build();
        try
        {
            RowMapper.toPhysical(update, physical, layout, INCARNATION);
            fail();
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("differs"));
        }
    }

    // ---- one epoch apart (T6.14, ADR-034) ----

    @Test
    public void cellsOfAColumnDroppedFromTheTargetAreSkippedAndUnseenColumnsStillFail() throws Throwable
    {
        setUp("p int, c int, v int, w text, PRIMARY KEY (p, c)");
        PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(logical, 7).timestamp(5);
        b.row(1).add("v", 1).add("w", "x");
        PartitionUpdate withBoth = b.build();

        // the target is one epoch ahead: w was dropped from it
        schemaChange("ALTER TABLE " + keyspace() + '.' + physical.name + " DROP w");
        TableMetadata ahead = Schema.instance.getTableMetadata(keyspace(), physical.name);
        Map<Integer, PartitionUpdate> mapped = RowMapper.toPhysical(withBoth, ahead, layout, INCARNATION);
        assertEquals(1, mapped.size());
        PartitionUpdate update = mapped.values().iterator().next();
        assertEquals(1, update.columns().regulars.size());
        Row row = update.iterator().next();
        assertEquals(1, row.columnCount());
        assertEquals("v", row.columns().iterator().next().name.toString());
        assertEquals(1, RowMapper.relabel(withBoth.iterator().next(), ahead).columnCount());

        // the target has not seen a column the data has: refused, never dropped
        TableMetadata behind = ahead;
        schemaChange("ALTER TABLE " + keyspace() + '.' + logical.name + " ADD x text");
        TableMetadata logicalAhead = Schema.instance.getTableMetadata(keyspace(), logical.name);
        PartitionUpdate.SimpleBuilder b2 = PartitionUpdate.simpleBuilder(logicalAhead, 7).timestamp(5);
        b2.row(1).add("x", "new");
        try
        {
            RowMapper.toPhysical(b2.build(), behind, layout, INCARNATION);
            fail("a cell of a column the target has not seen must not be dropped");
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("missing or differs"));
        }
    }
}
