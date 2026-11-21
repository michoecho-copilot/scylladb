sstables: add Parquet as a format of the Data file

This series lets an sstable store its Data file as a Parquet file. A table
opts in with a new cluster configuration option:

    ALTER TABLE ks.tbl WITH sstable_data_format = 'parquet';

The default stays `native`, so existing tables are not affected. The new
sstables have the format `pq` and the version `aa`. Their other components
(trie index, bloom filter, `Statistics.db`, `Scylla.db`) are as in `mt`. The
index stores row ordinals in place of byte offsets.

Details are in the docs that the series adds:

- `docs/architecture/sstable/sstable-parquet.rst`: user documentation.
- `docs/dev/sstable-parquet-format.md`: the file format.
- `docs/dev/sstable-parquet-benchmark.md`: a comparison with `mt` on nine
  public datasets.

## Impact on existing code

- The `mx` writer is split into helpers that both writers share: large data
  tracking, the serialization header, statistics, row index blocks, the trie
  index writers, the bloom filter, and sealing. These commits do not change
  behaviour.
- Code no longer compares sstable versions by order. It checks features of a
  version through predicates, and the comparison operators are deleted.
- `sstables_manager` chooses the version of a new sstable per table.
  `upgradesstables` also rewrites sstables whose format differs from the
  table's choice.
- `index_cache_fraction` also covers the new Parquet metadata cache.
- Dictionary training skips Parquet Data files.
- The option can be set only when every node supports it. System tables and
  compact storage tables always use the native format.
- `scylla sstable` supports Parquet sstables.
- Arrow C++ becomes an optional build dependency of seven test binaries, as a
  reference Parquet implementation. It is not linked into `scylla`.
  `install-dependencies.sh` installs it. Without it, the seven tests are not
  built, and test.py skips them. The frozen toolchain needs an update to run them.

## Differences from `mt`

**Leaves.** A *leaf* is a column of the Parquet file. Most costs below grow
with the number of leaves. A file always has these leaves:

- one per key column;
- for each static or regular column that has a value in the file, the leaves
  that hold its values. An atomic column (a native or frozen type) and a set
  have one. A map has two: keys and values. A list has two: elements and
  their timeuuid cell keys. A non-frozen UDT has one per field. A counter has
  three: shard values, shard ids, and clocks.
- `_scylla.kind`, the kind of the row, and `_scylla.ts`, the row's default
  timestamp.

The other metadata leaves are optional. A file has one only if some row in
it needs it:

- Per row, 9 leaves: the row's default TTL and expiry, the deletion time of a
  dead row marker, and the partition, row, and shadowable tombstones.
- Per column, up to 5 leaves for an atomic column, and a few more for a
  collection: the cell's timestamp, TTL, expiry, and deletion time, and a
  marker for empty values of fixed-width types.

A cell's timestamp and TTL are stored only where they differ from the row's
defaults. For a row with a live row marker, the defaults are the marker's
timestamp and TTL. For other rows, the defaults are only a base for the
cells, and any value gives a correct file. The writer picks the timestamp
and the TTL that more than half of the row's cells share, if there is one.
So plain inserts need no optional leaf. A TTL on whole rows adds the two row
leaves. Cells with their own timestamps or TTLs, deleted cells, and empty
values add leaves per column.

For example, take this table and these writes:

```cql
CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck));
INSERT INTO ks.t (pk, ck, v) VALUES (1, 1, 10) USING TIMESTAMP 100;
INSERT INTO ks.t (pk, ck, v) VALUES (1, 2, 20) USING TIMESTAMP 100;
UPDATE ks.t USING TIMESTAMP 105 SET v = 30 WHERE pk = 1 AND ck = 3;
UPDATE ks.t USING TIMESTAMP 120 SET v = 11 WHERE pk = 1 AND ck = 1;
DELETE FROM ks.t USING TIMESTAMP 110 WHERE pk = 2;
```

An sstable with this data has these Parquet rows. The leaves under
`_scylla.` are shown without that prefix, and `d` is the deletion time of
the partition tombstone.

| `pk` | `ck` | `v` | `kind` | `ts` | `cells.v.ts` | `partition_tomb_ts` | `partition_tomb_deletion_time` |
|---|---|---|---|---|---|---|---|
| 1 | 1 | 11 | 3 | 100 | 20 | null | null |
| null | 2 | 20 | 3 | 100 | null | null | null |
| null | 3 | 30 | 2 | 105 | null | null | null |
| 2 | null | null | 0 | null | null | 110 | d |

- Each Parquet row holds one clustering row. The partition key is stored
  only on the first row of its partition.
- `kind` 3 is a clustering row with a row marker, written by `INSERT`. Its
  `ts` is the marker's timestamp. `kind` 2 is a row without a marker,
  written by `UPDATE`. Its `ts` is only a base for its cells' timestamps.
  Here it is 105, the timestamp of its only cell.
- `cells.v.ts` is the timestamp of `v` minus the row's `ts`. It is null
  where they are equal.
- Partition 2 has only a partition tombstone, so it gets one row of `kind`
  0, which holds the key and the tombstone.
- The file has 5 mandatory leaves (`pk`, `ck`, `v`, `kind`, and `ts`) and 3
  optional ones. `cells.v.ts` is there because of the update at timestamp
  120. The two partition tombstone leaves are there because of the
  `DELETE`. No row needs the other optional leaves, so the file does not
  have them.

What an optional leaf costs:

- Resident memory (see below) and write CPU, as for any leaf.
- Reads pay for it only in row groups where it has a value. There, a point
  read reads and decodes one page of it, as for any leaf. If the leaf is
  mostly null, its pages are small. So it should cost few bytes and little
  CPU, but it can add a read request. A read fetches the pages of all leaves
  at once, and reads two pages with one request when they lie in the same
  or adjacent 4 KiB blocks. Pages of two leaves are that close when their
  chunks in the row group are single pages next to each other. This cost was
  not measured.

The benchmark below used only plain inserts, so its files had no optional
leaves.

**Resident memory per sstable.** `mt` keeps the position of every compressed
chunk (4 KiB of data by default) from `CompressionInfo.db`. `aa` has no
`CompressionInfo.db`. It keeps a summary of the Parquet footer: 40 bytes per
row group, 2 bits per leaf per row group, and about 64 bytes plus the name
per leaf. With the default row groups of 8 MiB, this is about 5 KiB per GiB
of Data file, plus 32 bytes per leaf per GiB.

**Evictable cache.** The index is cached as for `mt`. In addition, a new
cache keeps two kinds of entries, both in decoded form, so a hit decodes
nothing:

- the OffsetIndexes of a row group, which give the location of every data
  page of every leaf in the row group;
- the contents of dictionary pages: the uncompressed dictionary of a leaf in
  a row group, and for variable-length values the position of each entry.

Its entries live in the LSA region of `cache_tracker` and are evicted in LRU
order with the row cache and the index caches. Data pages are not cached,
as for `mt`.

**Memory per reader and writer.** A reader holds one decoded page per leaf.
The read-ahead of a scan is 128 KiB per leaf (at least 640 KiB), and at most
one row group. Both are charged to the reader's permit. A writer buffers a
whole row group, up to 8 MiB. The writers of a shard share a budget of 5% of
its memory, so with many writers the row groups get smaller.

**I/O and CPU.** The benchmark compares Parquet with `mt` with
`ZstdWithDictsCompressor` and a trained dictionary. It uses about 400 MB of
rows from each of nine public datasets. They range from narrow numeric time
series (weather, trades) to rows with large text values (GitHub events,
product reviews) and a wide, sparse table with 171 leaves (disk drive
statistics). Section 4 of `docs/dev/sstable-parquet-benchmark.md` describes
them. The ratios of Parquet to `mt` were:

- The files take 0.32 to 0.86 of the size.
- Writes and full scans cost about the same CPU (0.66 to 1.21). A scan reads
  fewer bytes, but issues 1.4 to 4.5 times more read requests, because it
  reads each leaf separately.
- Point reads are much more expensive. `mt` reads and decompresses one
  4 KiB chunk. `aa` reads and decodes one whole page of every leaf that has
  a value in the row group, and a page holds at least 8 KiB of raw values.
  So the cost grows with the number of leaves and the size of pages. A single-row read costs 3.8 to 41 times the CPU, reads
  2.5 to 60 times the bytes, and issues 3.7 to 107 times the read requests.
  A read of a whole partition costs about the same as `mt` when partitions
  have thousands of rows, and 6 to 18 times more when they have a few rows.

So Parquet suits tables whose cost is dominated by storage, where smaller
files outweigh much more expensive reads.

No backport: this is a new feature.
