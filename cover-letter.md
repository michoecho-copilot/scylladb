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

## The format

`mu` is `mt` with two changes. Both apply only to compressed sstables. An
uncompressed `mu` sstable is identical to an uncompressed `mt` sstable.

1. **Framed chunks.** Each chunk in Data.db has the layout
   `[header][compressed data][crc32][footer]`. The header and the footer both
   hold the compressed length. A reader at the start of a chunk finds the end of
   the chunk from the header. A reader at the end of a chunk finds the start of
   the chunk from the footer. Reverse reads need the footer.
2. **Physical index positions.** Partitions.db and Rows.db store a position as
   three values: the file offset of the chunk, the number of 512-byte blocks
   that the chunk touches, and the offset in the decompressed chunk.

CompressionInfo.db remains unchanged. (It's just not kept in memory for `mu`).

The new page `docs/architecture/sstable/sstable3/sstable-mu-format.rst`
describes the format in detail.

## Overview of the series

- Add `sstable_position`, a position type that is either logical or
  physical.
- Convert `continuous_data_consumer` (the thing that feeds an input stream
  to an sstable parser), and `sstable::data_stream()` so that they talk in terms of
  `sstable_position`.
- Add the enum for version `mu`.
- Add `framed_chunk_codec`, which abstracts over framed and unframed chunks.
- Write the chunk frames in the compressing output stream.
- Add `decompressing_cursor`, which is what handles the compression layer during `mu` reads.
  (Reads the right ranges from disk, decompresses chunks, navigates to neighboring chunks).
- Move the IO "backend" of `partition_reversing_data_source` from `sstable::data_read` to
  a custom cursor interface, with a logical implementation (based on `sstable::data_read`)
  and a physical implementation (based on `decompressing_cursor`).
  (Because a `mu` file can't handle "read file range `[x - n, x]`", but only "read `n` bytes backwards from `x`").
- Encode and decode physical positions in BTI index payloads.
  Switch the signatures of BTI index writers and readers to use `sstable_position`.
- Adjust sstable readers so that they use `sstable_position` instead of integers.
  Make writers feed physical positions to `mu` indexes.
- Allow `sstable_format: mu` in the configuration.
- Stop keeping the offset array in memory for `mu`.
  (Evict from RAM after sealing, don't load into RAM when opening for reading).
  Data.db sampling (for dictionary training) still needs to read logically-indexed ranges
  of the file, so it still needs to convert logical positions to physical positions via CompressionInfo,
  so we keep CompressionInfo open for the life of the sstable. (Which is sad, but I'm not sure if problematic).

## Upgrade and compatibility

- The default `sstable_format` stays `me`.
- Writing `mu` needs the cluster feature `MU_SSTABLE_FORMAT`. Until the
  feature is enabled, a node configured for `mu` writes `mt` if `mt` is
  enabled, and `me` otherwise. It never falls back to `ms`.
- `mu` does not supersede `mt`. A node configured for `mt` keeps writing `mt`.
- `get_highest_sstable_version()` and the REST endpoint
  `highest_supported_sstable_version` now return `mu`.

## Tradeoffs

- In `mu` each compressed chunk is larger by 4-6 bytes than in `mt`.
- In `mu` each index entry on disk (and in index cache) is larger by ~2 bytes on average than in `mt`.
- Each compressed `mu` sstable keeps CompressionInfo.db open, so it holds one more file descriptor.
- In-memory trie nodes (in the index writer) become 7 bytes larger (for both `mt` and `mu`).
- Extra virtual calls, tasks and allocations in sstable reads for all formats.
  (Due to more abstraction around coroutines in `continuous_data_consumer` and such).
- Code becomes more complicated.

## Testing

Haphazard. Mostly relying on including `mu` in existing tests.
The new unit tests are AI-generated and I don't vouch for their value.

Some new classes like `logical_reversing_cursor` have no unit tests at all, but I'm not sure if trying to add some would be worth the effort.
(Frankly, for things like this, an AI review seems more effective at catching bugs than unit tests, unless they are high-effort exhaustive tests).

## AI disclaimers

This series was originally written in Opus 4.8 days, more or less by hand.
Then some big parts were reviewed and interactively rewritten with Opus 5, and I reviewed the results in detail.
Then some big parts were reviewed and interactively rewritten with Opus 5.5, and I only gave the results of each step a shallow review.
So in the end, there are some parts which I only reviewed shallowly, though they are mostly localized rewrites of things I wrote myself or reviewed deeply.

The series went through several rounds of Opus 5.5 review, which found many issues of various kinds. I kept addressing the reviews until there were no worthwhile findings.

The new unit tests are unreviewed AI output. Nevertheless I included them in the name of "probably won't hurt". Perhaps they should be dropped.

## Backports

New feature, no backports needed.

Fixes: SCYLLADB-3397
Fixes: SCYLLADB-115
