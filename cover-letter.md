## Motivation

The Data.db of a compressed sstable is a sequence of compressed chunks. Every
existing format indexes Data.db with logical positions: offsets in the
uncompressed data. To read at a logical position, a reader converts it to a
file position with the chunk-offset array from CompressionInfo.db.

Scylla must keep this array in RAM for every present sstable.
Its size is proportional (about 0.1%) to the size of Data.db.
This imposes a minimal RAM:disk ratio needed to handle sstables,
which is undesirable.

The simplest way to deal with that would probably be to make that array
pageable from disk on demand. But that would add extra disk reads in
workloads where that's used.

This series adds the sstable format `mu`, whose index stores post-compression
positions. Thus, a compressed `mu` sstable can read from Data.db without
consulting CompressionInfo, and the array doesn't have to be kept in memory.
(In fact, `mu` doesn't even store the full array on disk. CompressionInfo.db
keeps only a small random sample of it, for dictionary training).

## The format

`mu` is `mt` with five changes. The first three apply only to compressed
sstables.

1. **Framed chunks.** Each chunk in Data.db has the layout
   `[header][compressed data][crc32][footer]`. The header and the footer both
   hold the compressed length. A reader at the start of a chunk finds the end of
   the chunk from the header. A reader at the end of a chunk finds the start of
   the chunk from the footer. Reverse reads need the footer.
2. **Physical index positions.** Partitions.db and Rows.db store a position as
   three values: the file offset of the chunk, the number of 512-byte blocks
   that the chunk touches, and the offset in the decompressed chunk.
3. **Sampled chunk offsets.** CompressionInfo.db stores only a random sample of
   the chunk offsets. (Each chunk is kept with probability 1/64). The only user
   of the offsets is Data.db sampling for dictionary training, and it needs only
   a few random chunks.
4. **Index page footers.** Each page of Partitions.db and Rows.db ends with a
   footer: a table of the Data.db chunks that the page refers to, the size of
   the table, and a CRC32 of the page. A partition entry can refer to its chunk
   through a slot of the table instead of storing the chunk position inline, so
   neighboring partitions in the same chunk don't have to repeat it.
5. **IncompatibleFeatures.** Scylla.db always contains a new
   IncompatibleFeatures entry. A reader refuses to open an sstable with a bit
   it doesn't know. No such bits are defined yet. (It's there so that a future
   version can add a feature that older readers must refuse, without adding
   yet another format version).

An uncompressed `mu` sstable differs from an uncompressed `mt` sstable only in
Scylla.db and in the page footers of Partitions.db and Rows.db.

The new page `docs/architecture/sstable/sstable3/sstable-mu-format.rst`
describes the format in detail.

## Overview of the series

- Add `sstable_position`, a position type that is either logical or
  physical.
- Convert `continuous_data_consumer` (the thing that feeds an input stream
  to an sstable parser), and `sstable::data_stream()` so that they talk in terms of
  `sstable_position`.
- Move the IO "backend" of `partition_reversing_data_source` from `sstable::data_read` to
  a custom cursor interface, with a logical implementation (based on `sstable::data_read`)
  and, later in the series, a physical implementation (based on `decompressing_cursor`).
  (Because a `mu` file can't handle "read file range `[x - n, x]`", but only "read `n` bytes backwards from `x`").
- Add the enum for version `mu`.
- Add `framed_chunk_codec`, which abstracts over framed and unframed chunks.
- Write the chunk frames in the compressing output stream.
- Add `decompressing_cursor`, which is what handles the compression layer during `mu` reads.
  (Reads the right ranges from disk, decompresses chunks, navigates to neighboring chunks).
- Encode and decode physical positions in BTI index payloads.
  Switch the signatures of BTI index writers and readers to use `sstable_position`.
- Adjust sstable readers so that they use `sstable_position` instead of integers.
  Make writers feed physical positions to `mu` indexes.
- Add page footers to `mu` index files.
  (Teach the trie writer to account for the chunk table that grows at the end of each page,
  route all raw index bytes through the node sink, and verify the page CRC
  in `cached_file` before a page is cached).
- Stop storing every chunk offset in CompressionInfo.db of `mu`, and store a random sample instead.
  Data.db sampling (for dictionary training) reads its samples from physical positions,
  so CompressionInfo.db doesn't have to be kept open or in memory.
- Add a metric for the memory used by chunk-offset arrays (`sstables/compression_offsets_memory_size`).
- Add the IncompatibleFeatures entry.
- Document the format.
- Allow `sstable_format: mu` in the configuration.
  (This is at the very end of the series, so that `mu` can't be selected before its format is final).

## Changes for existing formats

Most of the series doesn't change the behavior of existing formats, but a few things do:

- Compaction and view-update progress is now computed from the sstable position,
  because the consumer's byte counter is no longer a position in the file.
- In reverse reads, if the end of the index range is exactly the start of the next row,
  the reader now skips that row. Previously it read the row, and then dropped it.
- `ms` and `mt` index files are byte-identical to what they were before.
  (There's a new test that pins their digests).

## Upgrade and compatibility

- The default `sstable_format` stays `me`.
- Writing `mu` needs the cluster feature `MU_SSTABLE_FORMAT`. Until the
  feature is enabled, a node configured for `mu` writes `mt` if `mt` is
  enabled, and `me` otherwise. It never falls back to `ms`.
- `mu` does not supersede `mt`. A node configured for `mt` keeps writing `mt`.
- `get_highest_sstable_version()` and the REST endpoint
  `highest_supported_sstable_version` now return `mu`.
- For `mu`, `scylla-sstable dump-compression-info` can only show the sample of the chunk offsets.
  It prints it as `sampled_offsets`, together with `sampling_denominator`.

## Tradeoffs

- In `mu` each compressed chunk is larger by 4-6 bytes than in `mt`.
- In `mu` each index entry on disk (and in index cache) is larger by ~2 bytes on average than in `mt`.
- In-memory trie nodes (in the index writer) become 48 bytes larger (72 -> 120 bytes), for all BTI formats.
- A `mu` table has about 1/64 as many distinct dictionary-training samples as it has chunks.
  A request for more samples than a small table has gets repeated samples.
- Extra virtual calls, tasks and allocations in sstable reads for all formats.
  (Due to more abstraction around coroutines in `continuous_data_consumer` and such).
- Code becomes more complicated.

## Testing

Haphazard. Mostly relying on including `mu` in existing tests.
The new unit tests are AI-generated and I don't vouch for their value.

Some new classes like `logical_reversing_cursor` have no unit tests at all, but I'm not sure if trying to add some would be worth the effort.
(Frankly, for things like this, an AI review seems more effective at catching bugs than unit tests, unless they are high-effort exhaustive tests).

## Backports

New feature, no backports needed.

Fixes: SCYLLADB-3397
Fixes: SCYLLADB-115
