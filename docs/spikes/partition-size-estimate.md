# Spike: estimating one partition's size cheaply on a node

Status: spike result, 2026-11-21. No production code changed. Decision left to the owner.

## The question

`PartitionSplitPolicy` measures a hot partition with `PartitionProbe`, which reads the whole partition through the
coordinator, 1000 rows a page with a 10 ms pause between pages, up to 5M rows. The result is cached for as long as
the key stays hot, so a partition that was too small when first probed is never measured again. Re-probing is
expensive, so re-probe triggers were being designed. The question for this spike: can a node find out how big one
key's partition is **without reading its rows**?

## Short answer

Yes, for the sstables, and it is cheap: well under a millisecond per key once the index pages are cached, with no
read of the data file in the cases that matter. In every sstable the data file is written in key order, so
**the size of a key's fragment is the data file position of the next key minus the position of this key**. Both
positions come from the partition index. Cassandra already measures partition size this way: the `partition_size`
guardrail in `SortedTableWriter` uses `endPosition - partitionStartPosition`, and the `MaxPartitionSize` gauge is
built from the same numbers.

The memtable has no per-partition byte counter in either memtable implementation. Its part has to be found by
walking the key's rows in memory. There is no I/O, but the cost grows with the number of the key's rows that are in
the memtable (about 3.5 ms for 20k rows in the test).

What the cheap estimate does **not** give: an exact row count, and a figure in the same units as today's
`estimatedBytes`. Details below.

## 1. Mechanism per sstable format

Defaults in this fork: `sstable.selected_format` is `big` (`Config.SSTableConfig.selected_format = BigFormat.NAME`;
`conf/cassandra.yaml` leaves it commented as `big`). `cassandra_latest.yaml` selects `bti`. The default memtable is
`skiplist` (`memtable.configurations.default inherits skiplist`); `trie` is available.

The prototype uses only public, format-independent calls, for each live sstable that can hold the key
(`cfs.selectAndReference(View.select(SSTableSet.LIVE, key))`, which uses the interval tree of first/last keys):

```
start = sstable.getPosition(key, Operator.EQ, false)    // -1: key not in this sstable
next  = sstable.getPosition(key, Operator.GT, false)    // -1: key is the last one
end   = next < 0 ? sstable.uncompressedLength() : next
fragment bytes = end - start
```

Passing `updateStats=false` keeps the lookups out of the key cache and read metrics.

**BIG** (`BigTableReader.getRowIndexEntry`):

- EQ: min/max key check, bloom filter (memory), key cache (memory). On a key cache miss: binary search in the
  index summary (memory), then a sequential scan of `Index.db` from the sampled position, at most one index interval
  (128 entries by default). The matching `RowIndexEntry` is deserialized; for a wide partition it is a
  `ShallowIndexedEntry`, which does not load the row index blocks.
- GT: the same, but no bloom filter and no key cache. The scan passes over the key's own entry by seeking over its
  row index, so a wide partition's large row index is skipped, not read.
- No read of `Data.db`. Cost per sstable that holds the key: two short reads of `Index.db` (usually in the page
  cache, a few KiB), plus memory work.

**BTI** (`BtiTableReader`):

- EQ (`getExactPosition`): min/max key, bloom filter, then a walk of the partition trie in `Partitions.db`
  (memory-mapped). For a partition with a row index (anything larger than one row index block, 16 KiB by default, so
  every partition we care about), the key is checked and the entry read in `Rows.db`. Only for a small partition
  without a row index is the key checked by reading its header in `Data.db`.
- GT (`getRowIndexEntry` with `reader.ceiling`): a trie walk; the key's own position is rejected, and the next one is
  taken without checking its key (`assumeNoMatch`), from the trie payload or from one `Rows.db` entry.
- No read of `Data.db` when the hot partition is wide. Cost: a few page reads of `Partitions.db` and `Rows.db`.

Both formats: `onDiskSizeForPartitionPositions` turns the range into compressed bytes from the compression chunk
offsets (memory, chunk granularity).

A production version would want one method on `SSTableReader` that returns both positions from one index walk
instead of two lookups; that is an optimisation, not needed to decide.

## 2. Memtable contribution

Neither `SkipListMemtable` (default) nor `TrieMemtable` keeps a byte count per partition; both only keep the table's
`liveDataSize`. In both, a partition is a `BTreePartitionData` (the trie in `TrieMemtable` indexes keys, not rows),
so:

- the row count is O(1) (`BTree.size`), but `getPartition` is package-private in both; the public
  `Memtable.rowIterator(key)` is what the prototype uses;
- the bytes need a walk of the key's rows, summing `Row.dataSize()`. That is in memory, no I/O, about 0.2 µs a row
  here (20k rows: ~3.5 ms). It is bounded by the memtable size, which flushes at a fraction of the heap.

The memtable figure is `dataSize`, not serialized bytes, so it reads 15-150% higher than the same rows on disk
(see 3). For a size check this is usually a small share of the total; for a partition that is mostly written in the
last minutes it is not. If the walk is too slow for a very write-hot key, a small API addition could give the row
count in O(1) and multiply it by a per-row size.

## 3. What the number means

- **Uncompressed serialized bytes** of the partition as written on disk (row headers, vint-encoded clustering and
  cells, timestamps delta-encoded per sstable). The same unit as the guardrail and `MaxPartitionSize`. Compressed
  bytes are available too, at chunk granularity (in the test, values that compress 3-4x gave 6.3 MiB for 21.9 MiB).
- **A sum over fragments, not the live size.** Each sstable that holds the key adds its fragment. Overwritten rows
  are counted once per copy; deleted rows are counted, and so are the tombstones, until compaction merges them. So
  the sum is an upper bound of the live data, and exact after a major compaction. In the test: written 3 times, 3x;
  60% deleted, 2.6x. Fragments also differ a little from the merged result, because timestamps are encoded relative
  to each sstable's minimum (four disjoint fragments of narrow rows summed 9% *less* than the compacted partition).
- **Local to this node.** It measures this replica only. The split policy runs on the preferred proposer (the first
  live node of the directory), which may not be a replica of the key. The hot-partition samplers do run on replicas
  (`ColumnFamilyStore.apply`, `SinglePartitionReadCommand` local reads), so `HotPartitionSensor` could measure its
  top keys where it finds them.
- **Not the units of today's check.** `PartitionProbe.estimatedBytes` is `rows x mean Row.dataSize()`, an in-memory
  measure. For narrow rows it is 2.5x the serialized size (1M rows of 8 B values: 50.5 MiB by the probe, 20.5 MiB on
  disk); for 100 B rows 1.3x; for 32 KiB rows equal. `LARGE_PARTITION_BYTES = 100 MiB` would mean a different
  partition if it were compared with the cheap figure; the threshold would need restating in on-disk bytes.

Which probe outputs it can replace:

| Probe output / use | Replaceable? | Note |
|---|---|---|
| `estimatedBytes` vs `LARGE_PARTITION_BYTES` (no_spread) | Yes, with the threshold restated in serialized bytes | Overstates with overwrites and deletes, which errs toward "large" |
| `estimatedBytes` for `backfill_too_long` | Yes | Overstates: the backfill copies live rows only. Errs toward refusing |
| `estimatedBytes` for the `disk` check | Yes | On-disk bytes are the right unit here; overstating is the safe side |
| `rows` vs `min_rows_for_split` | Only approximately | See below |
| `hitMax` (more than 5M rows, too_large) | Only as a byte limit | A byte cap would replace a row cap |
| `meanRowBytes` | No | Needs rows |

**Approximate row count.** Nothing cheap stores rows per partition. The BIG row index (`IndexInfo`) and the BTI
row index store offsets and clustering bounds per block, no row counts; the block count (`blockCount()`) is only bytes
divided by the block size. Per-sstable stats have `totalRows` for the whole sstable, so
`fragment bytes x totalRows / sstable data length` gives a row estimate that is right when the table's rows are all
alike (1M narrow rows: 976k; 200k rows: 199.9k) and wrong when they are not (the 1500 rows of 32 KiB, in a table
that also had 6000 small rows: 7396, 5x) and with overwrites or deletes (3x, 3.6x). A better approximation would be
bytes divided by the serialized size of a few sampled rows, which needs a short read of the first rows (one page, not
the partition).

## 4. Measurements

Prototype: `test/unit/org/apache/cassandra/autonomic/layout/PartitionSizeEstimateSpikeTest.java` (spike code,
skipped unless `SPIKE_PARTITION_SIZE` is set). Each scenario writes one target partition plus 2000 small partitions
(3 rows of 100 B) in a single-node `CQLTester`, once with BIG + skiplist memtable and once with BTI + trie memtable.
Values are a quarter random, the rest zeros, LZ4 compression, `gc_grace_seconds = 0`, auto-compaction off. Run with
`SPIKE_PARTITION_SIZE=1 ANT_ARGS=-Dno-build-accord=true .build/run-tests.sh -s -a test -t PartitionSizeEstimateSpikeTest`
on the 4-core, 5 GB machine; data was in the page cache (no way to drop it without root), so the cold-disk cost of
the index reads is not measured.

Columns: **frag** = sstable fragments (+mem if the memtable holds rows). **live rows** = exact, by a local read.
**approx rows** = from sstable stats (plus memtable rows). **cheap** = sum of fragments + memtable `dataSize`.
**compr** = compressed chunks the fragments touch. **compacted** = the cheap estimate again after flush and major
compaction (one fragment, tombstones purged): the true serialized size. **live dataSize** = sum of `Row.dataSize()`
of the live rows. **probe est** = `PartitionProbe.estimatedBytes()`. **cheap ms** = first call / median of 20 /
sstable part only. **probe ms** includes the probe's own 10 ms pauses (**pause ms**). **dataSize/row** = the probe's
`meanRowBytes`; **disk/row** = compacted bytes per live row. Sizes in MiB.

| fmt | shape | frag | live rows | approx rows | probe rows | cheap | compr | compacted | live dataSize | probe est | cheap ms | probe ms | pause ms | dataSize/row | disk/row |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| big | narrow 1M rows x 8B | 4 | 1,000,000 | 976,195 | 1,000,000 | 18.6 | 7.8 | 20.5 | 50.5 | 50.5 | 0.42 / 0.20 / 0.18 | 16,755 | 10,000 | 53 | 21.5 |
| bti | narrow 1M rows x 8B | 4 | 1,000,000 | 976,222 | 1,000,000 | 18.6 | 7.8 | 20.5 | 50.5 | 50.5 | 0.42 / 0.19 / 0.17 | 14,725 | 10,000 | 53 | 21.5 |
| big | wide 1500 rows x 32KiB | 1 | 1,500 | 7,396 | 1,500 | 46.9 | 12.0 | 46.9 | 46.9 | 46.9 | 0.32 / 0.11 / 0.10 | 1,442 | 10 | 32,813 | 32,787 |
| bti | wide 1500 rows x 32KiB | 1 | 1,500 | 7,396 | 1,500 | 46.9 | 12.0 | 46.9 | 46.9 | 46.9 | 0.24 / 0.07 / 0.07 | 1,722 | 10 | 32,813 | 32,787 |
| big | 50k rows x 200B written 3 times | 3 | 50,000 | 152,625 | 50,000 | 30.7 | 9.0 | 10.3 | 11.7 | 11.7 | 0.42 / 0.24 / 0.58 | 1,713 | 500 | 245 | 216 |
| bti | 50k rows x 200B written 3 times | 3 | 50,000 | 152,625 | 50,000 | 30.7 | 9.0 | 10.3 | 11.7 | 11.7 | 0.91 / 0.18 / 0.17 | 1,400 | 500 | 245 | 216 |
| big | 50k rows x 200B, 60% deleted | 2 | 20,000 | 72,633 | 20,000 | 10.5 | 3.1 | 4.1 | 4.7 | 4.7 | 0.42 / 0.20 / 0.12 | 563 | 200 | 245 | 216 |
| bti | 50k rows x 200B, 60% deleted | 2 | 20,000 | 72,632 | 20,000 | 10.5 | 3.1 | 4.1 | 4.7 | 4.7 | 1.15 / 0.13 / 0.09 | 653 | 200 | 245 | 216 |
| big | 200k rows x 100B, 4 sstables + memtable (20k rows) | 4+mem | 200,000 | 199,877 | 200,000 | 21.9 | 6.3 | 21.6 | 27.7 | 27.7 | 11.75 / 3.98 / 1.69 | 5,539 | 2,000 | 145 | 113 |
| bti | 200k rows x 100B, 4 sstables + memtable (20k rows) | 4+mem | 200,000 | 199,876 | 200,000 | 21.9 | 6.3 | 21.6 | 27.7 | 27.7 | 8.46 / 3.43 / 0.29 | 3,445 | 2,000 | 145 | 113 |

What the table says:

- **Cost.** The cheap estimate takes 0.07-0.25 ms (median) when the key has no rows in the memtable, about 4 ms with
  20k rows in the memtable, and at most 12 ms on a first call. The probe takes 0.6-17 s for these partitions; about
  17 ms per 1000 rows, of which 10 ms is its pause, so a 5M-row partition (its cap) costs about 85 s of reading. The
  cheap estimate is 4-5 orders of magnitude cheaper and does not read the partition at all.
- **Accuracy against the true on-disk size** (compacted column): exact for a single fragment; -9% for four disjoint
  fragments of narrow rows (timestamp encoding); +1% with a memtable part; **+200% with three overwrites, +160% with
  60% deleted** (fragments counted until compaction).
- **Against the probe's figure:** the probe measures `dataSize`, which is 2.5x the on-disk size for narrow rows and
  1.3x for 100 B rows. The probe's byte estimate itself is good only in its own unit: it equals the live `dataSize`
  here because all rows of a partition have the same size; with mixed row sizes, its 50-row sample can be off either
  way.
- **BIG and BTI** behave the same; BTI is a little faster on repeat calls.

## 5. Recommendation

**Add a cheap pre-check and re-measure step in front of the probe; do not replace the probe yet.**

1. Measure the cheap size on the replicas, where the hot key is found: `HotPartitionSensor` already runs on each
   replica and emits its top 5 keys per sampled table per window; it can emit a `partition.local_bytes` signal for each
   (well under 1 ms per key from the index, plus the memtable walk). This makes the size a fresh signal every window,
   not a one-off probe result.
2. In the policy, use that signal (the maximum over replicas) for the size-based decisions: the `no_spread`
   large-partition check (with the 100 MiB threshold restated in on-disk bytes), `backfill_too_long` and `disk`. The
   overstatement from overwrites and deletes errs on the cautious side for backfill and disk, and toward "large"
   for `no_spread`.
3. Keep the probe for what only it can give, the row count against `min_rows_for_split` and the `hitMax` cap, but run
   it only when the cheap size makes the answer plausible: skip it while `local_bytes < min_rows_for_split x a low
   row size` (the partition cannot have enough rows), and refuse `too_large` from bytes before probing. Alternatively
   restate `min_rows_for_split` as a byte minimum and drop the probe; that is a design change for the owner, not
   something the spike can settle.

Effect on the re-probe trigger design:

| Trigger | With the cheap size |
|---|---|
| Growth estimate from sampled write counts | **Unnecessary.** The size itself is re-measured every window; growth is the difference between windows |
| Table `MaxPartitionSize` gauge as a gate | **Unnecessary.** It is the same measure per sstable, coarser (histogram buckets, whole table, does not add fragments) |
| `partition_size` guardrail event (`SortedTableWriter`) | **Unnecessary.** Same measure, but only per written fragment, so it never fires for a partition spread over many small sstables until compaction merges them; and it is off by default |
| Periodic backstop | **Keep, much simpler**: drop the cached probe result when the cheap size has grown past a factor (for example 2x) since the probe, or after a long period, and re-probe only then. The re-probe stays the expensive part, so it should stay rare |

## Open risks

- **Overcounting before compaction.** Heavy overwrite or delete workloads (queues, time-ordered upserts) can show
  several times their live size. That errs toward refusing (backfill, disk) and toward `sizeOnly` splits under
  `no_spread`, which could propose a split of a partition that is large only in dead data. A `droppable tombstone`
  view per sstable does not exist per key; the probe (or a compaction) is the only correction.
- **Unit change.** The 100 MiB threshold and the backfill throughput were chosen against `dataSize`-based estimates;
  moving to on-disk bytes changes which partitions pass, by up to 2.5x for narrow rows.
- **Replica locality.** The proposer is not necessarily a replica. The figure must travel as a signal from the
  replicas, and replicas can disagree (missed writes, compaction state); the maximum is the conservative choice.
- **Memtable walk.** O(rows of the key in the memtable). Fine for normal memtable sizes; for an extremely write-hot
  key, consider an O(1) row count (needs `getPartition` exposed) times a sampled row size.
- **Cold index pages.** Timings are with the index in the page cache. A cold `Index.db` read costs a disk read per
  fragment (BIG: from the summary point, up to 128 entries; BTI: a few trie pages). Still no data file read.
- **Edge cases not tested:** early-open sstables during compaction (their `uncompressedLength` is the boundary, so
  the last key's fragment may be short), transient replication, many hundreds of fragments (cost is linear in the
  fragments that hold the key), partitions with static rows or collections (same mechanism, not measured).
- **BTI small partitions.** For a partition without a row index (under 16 KiB), the EQ lookup reads the key from
  `Data.db`. Irrelevant for split candidates, but the cost is not zero for small keys.
