# Parquet as the sstable Data file: plan

Status: the design discussion is complete, and no code has been written.
Much of the plan was written from memory of the code.
Only the points under "Checked in the code" were verified.
"Not verified" lists the assumptions that still need a check, and "Background, not verified" holds supporting notes.

## Goal and constraints

- The goal is better compression than `ms` with zstd and trained dictionaries.
- Parquet is mandated.
  We assume strict conformance: standard tools must be able to open the files.
  External analytics is not a design target.
- No representative data exists.
  Decisions rest on the format's fixed costs and on worst cases.
  Where the data matters, the writer decides at run time.
- The project cannot check the goal.
  Compression depends on the dataset, so the project delivers options and tunables.
  The test happens on real data, after the project.

## Decisions

### Schema mapping

- One Parquet row per mutation fragment, in sstable order, with one exception: the partition start has no row of its own.
- The partition key and the partition tombstone are stored on the partition's first row.
  They are null on its other rows.
  The first partition key column has a value, possibly an empty one, exactly on the rows that start a partition.
- The first row holds whichever fragment comes first.
  A partition that has only a partition tombstone gets one row whose fragment kind is "none".
- The static row is a row of its own, written only if it has content.
  There is no row for the partition end.
- The token is not stored.
- Each CQL column gets dedicated leaf columns for its value, timestamp, TTL, expiry, and deletion time.
- Every row has a default timestamp, TTL, and expiry, stored in row-level columns.
  If the row has a marker, the marker's values are the defaults, and the fragment kind records that the marker exists.
  Otherwise the writer picks the defaults, for example the most common cell timestamp.
  The reader does not depend on how they were picked.
  Static rows follow the same rules.
- A cell stores its timestamp as the difference from the row's default.
  Null means no difference.
  A null TTL or expiry means "same as the row".
  An explicit TTL of zero means "does not expire".
- The defaults are an encoding detail of one row in one file.
  The reader always produces absolute values.
- Range tombstones use bound markers as in `mx`, with one tombstone per row.
  A boundary is written as an end bound row followed by a start bound row.
  This keeps the Data file sufficient for reverse reads.
- Frozen values are serialized blobs.
  Non-frozen collections and counters are described under "Non-frozen collections and counters".
- An empty value is never stored as a null.
  A CQL column of a fixed-width type has a boolean marker column in its cell metadata.
  An empty value stores a placeholder in the value column and sets the marker.
  The placeholder is zero: `false` for a boolean, and zero bytes for a fixed-length type.
  The reader ignores the stored value when the marker is set.
  Types stored as byte arrays need no marker, because a zero-length value is natural there.

### Row-level columns

All of them sit in the reserved group.

| Column | Type | Non-null on |
|---|---|---|
| `kind` | int32 | every row |
| `partition_tomb_ts`, `partition_tomb_deletion_time` | int64 | the first row of a partition that has a partition tombstone |
| `ts` | int64 | every row except kind "none" |
| `ttl`, `expiry` | int32, int64 | rows with a default TTL |
| `marker_deletion_time` | int64 | rows with a dead marker |
| `tomb_ts` | int64 | rows with a row tombstone whose timestamp differs from `ts` |
| `tomb_deletion_time` | int64 | rows with a row tombstone, and every bound row |
| `shadowable_tomb_ts` | int64 | rows with a shadowable tombstone whose timestamp differs from `ts` |
| `shadowable_tomb_deletion_time` | int64 | rows with a shadowable tombstone |

- That is 11 leaf columns.
  A table with plain inserts, no TTL, and no deletions has two of them in the file: `kind` and `ts`.
- `kind` has eight values: none, static row, clustering row without a marker, clustering row with a marker, inclusive start, exclusive start, inclusive end, and exclusive end.
  The bound weight follows from the kind.
- `ts` is the row's default timestamp.
  Every other timestamp in the row, except the partition tombstone, is stored as a difference from it, and null means no difference.
  This covers cells, collection elements, collection tombstones, the row tombstone, and the shadowable tombstone.
  The writer subtracts and the reader adds with 64-bit wraparound, because the difference of two arbitrary timestamps does not fit in a signed 64-bit integer.
- A tombstone exists exactly when its deletion time is non-null.
- On a bound row, `ts` and `tomb_deletion_time` hold the range tombstone.
  An end bound holds the tombstone that ends, and a start bound holds the one that starts.
- For a dead marker, the kind is "clustering row with a marker", `ts` is the marker's timestamp, and `marker_deletion_time` is set.
  `ttl` and `expiry` then serve only as defaults for the cells.
- A clustering prefix is stored as nulls in the trailing clustering key columns.
  This is unambiguous because an empty value is never a null.
  A non-null component after a null one is corruption.

### Non-frozen collections and counters

- The values sit at the top level under the CQL name.
  Everything else sits in the reserved group.
- A `map<K,V>` is a Parquet map of K to V.
  A `set<K>` is a Parquet list of K.
  A `list<V>` is a Parquet list of V, and its `timeuuid` keys are a list in the reserved group.
- Element metadata is stored as lists in the reserved group: timestamp difference, TTL, expiry, deletion time, and the empty-value markers.
  A map has separate empty-value markers for keys and for values.
- Each metadata list is either null for the row or has one entry per element.
  Null means that every element is live and uses the row's defaults.
- A dead element stays in the value list, and its deletion time is in the metadata list.
  A dead map entry has a null value, and a dead list element is a null.
  A dead set element differs from a live one only by its deletion time.
- A non-frozen user-defined type (UDT) is a Parquet group with one field per UDT field, and no list.
  The group is always present, so it adds no definition level.
  Each field has cell metadata like an ordinary column.
  Fields are matched by position.
- A counter is a list of shard values under the CQL name.
  The shard ids and the logical clocks are lists in the reserved group.
  The cell's timestamp and deletion time use the ordinary cell metadata columns.
- Every non-frozen collection column, including a UDT, has two columns for the collection tombstone: timestamp difference and deletion time.
- Elements follow the type rule of ordinary columns.
- A collection without elements has a null value list.
  The writer never writes an empty list.
- The codec therefore supports repeated fields.

### Column names and identity

- Value columns sit at the top level of the Parquet schema, under their exact CQL names.
- One reserved group, `_scylla`, holds the row-level columns and the cell metadata of every column.
  The group is always present, so it adds no definition level.
- Row-level columns sit directly in the group, for example `_scylla.kind`.
  The cell metadata of a column sits in a subgroup: `_scylla.cells.<column>.ts`, `.ttl`, `.expiry`, `.deletion_time`, and `.empty`.
  A collection column also has `.tomb_ts`, `.tomb_deletion_time`, `.key` (list keys), and `.key_empty` (map keys).
  A counter column also has `.shard_id` and `.clock`.
- The serialization header in Statistics.db is the authority for column names and types.
  The writer and the reader both compute the Parquet names from it, so the reader never parses a name.
- The Parquet name is the CQL name, with three exceptions: a name that starts with `_scylla`, a name that is not valid UTF-8, and an empty name.
  Such a name is written as `_scylla_x` followed by the hexadecimal form of its bytes.
  Two different CQL names therefore never produce the same Parquet name.
- Key columns come first in the schema, in key order.
  The serialization header stores no key names, so the writer takes them from the schema at write time and applies the same escape rule.
- The reader matches key columns by position: the first top-level fields are the partition key columns, and the clustering key columns follow.
  It checks their types and does not compare their names with the current schema, so a rename has no effect.
  It finds a key column's empty-value marker under the name that the file's own schema gives to that key field.
- Key columns are always in the schema, even when they are null in the whole file.
  Matching by position depends on this.

### Types

- Fixed-width native CQL types map to the matching Parquet type, which enables typed encodings.
  Everything else is a byte array in CQL serialized form.
  That covers `decimal`, `varint`, `duration`, `inet`, and all frozen types.
- A boolean is assumed to be the byte 0 or 1.
  The writer stores it as a Parquet boolean, so another non-zero byte would not be preserved.
- The reader decodes each value back to the CQL serialized form of its write-time type.
  The existing type compatibility rules then apply as they do today.
- At open, the reader checks the Parquet schema against what the serialization header implies.
  A mismatch is reported as corruption.
- The reader maps the header's columns to the current schema as `mx` does: by name, with the same compatibility check and the same handling of a column that was dropped and added again.
- The reader skips a column that is absent from the current schema, without reading it.
  It therefore does not make the check that `mx` makes for such a column: that the column is recorded as dropped, and that no cell is newer than the drop.
- The writer records null counts, but no minimum or maximum, and declares no sort order.
  The difference between CQL order and Parquet order therefore has no effect.

### Unused columns

- A column that is null in the whole file is left out of the file's schema.
  Key columns are the exception.
  In common workloads this covers most cell metadata columns and all empty-value markers.
- A column gets no chunks until its first non-null value.
  If that happens late in the file, the writer adds all-null chunks for the earlier row groups at the end of the file.
- The reader treats a column that is absent from the schema as entirely null.
- This relies on standard tools accepting a row group whose chunks are not adjacent.
  Arrow C++ 23.0.1 and DuckDB 2.0.0 accept it, as tests with synthetic files showed.
  The codec phase repeats the check with files from our own writer.
- The all-null chunks carry their null count in the footer, as every chunk does.
  DuckDB then skips them without reading their pages.

### Room for other cell metadata layouts

A shared "exceptions" column for rare cell metadata may compress better on some workloads.
We keep the door open for it:

- Metadata columns live in the reserved group.
- The file records a layout identifier in its key-value metadata.
  The reader rejects unknown values.
- The reader gets cell metadata through one internal interface.
- A new layout needs a cluster feature and an sstable feature bit.

### Index

- Keep the `ms` trie index, storing row ordinals instead of byte offsets.
- The Parquet OffsetIndex maps an ordinal to a page in each column.
- The ColumnIndex (per-page minimum and maximum) is not written.
  It can be added later without a format change.
- The reader uses the index as today: at the range ends and after skips.
- The trie index writers and the trie index reader are used unchanged, because they treat positions as opaque integers.
- A partition's position is the ordinal of its first row.
  A block's position is the ordinal of its first row, relative to the partition's first row.
- The partition end that the writer passes to the row index is the ordinal one past the partition's last row.
  `mx` passes the position of the end-of-partition byte there, and Parquet has no such row.
- The index reader receives the file's total row count in place of the Data file size.
  It returns that value when a bound is at the end of the file.
- The Parquet reader reads the result of `data_file_positions()` as a half-open range of ordinals.

### Reverse reads

- The reader first reads the head of the partition forward: the first row, which holds the key and the partition tombstone, and the static row if present.
- It then walks backward by ordinal from the end of the range.
- Each column has its own cursor.
  A page decodes only forward, so the cursor decodes a whole page and walks the buffer backward.
- The reader flips the bound kind of each range tombstone marker.
  A boundary is stored as two rows, and the reader merges the pair into one change in either direction.
- The index is used as today: at the range ends and after a skip between clustering ranges.
- Read-ahead runs backward, as described under "Reading the Data file".

### Layers and interfaces

The new code has three layers.

- The codec knows Parquet.
  It knows nothing about CQL types, mutation fragments, or sstables.
- The mapping turns mutation fragments into Parquet rows and back.
  It knows nothing about pages or encodings.
- The sstable integration holds the writer and reader classes, the format selection, and the size measures.

These are the interfaces between the layers, and between the new code and the rest of the system.

- **Codec to bytes, on write.**
  The codec appends to an `sstables::file_writer`, as the trie index writers do.
  The digest writer and the encryption stay below it.
- **Codec to bytes, on read.**
  The codec reads from a `seastar::file`.
  "Reading the Data file" has the details.
- **Codec to compression.**
  The codec has its own page compressor interface.
  `sstables::compressor` cannot be reused.
  Its LZ4 output starts with a 4-byte length and its Deflate output has zlib framing, while Parquet needs a raw LZ4 block and gzip framing.
- **Codec to cache.**
  The codec gets OffsetIndexes and dictionaries through a small cache of decoded metadata.
  The cache keeps them in compact forms without pointers, so that a hit parses, checks, and decompresses nothing.
  The OffsetIndexes of a row group are a fixed-size record per leaf column and one per page, with 32-bit offsets relative to the row group.
  A dictionary is the uncompressed page body, and for byte arrays a 32-bit position per entry.
  The reader decodes and checks what it fetches, and inserts the compact form.
  The cache keeps its entries in the LSA region of `cache_tracker`, in fragments within the LSA size limits.
  Each entry is an index entry in the LRU of `cache_tracker`, like the entries of the other sstable index caches.
  Memory reclaim evicts the entries, and `index_cache_fraction` covers them.
  The cache has no limit of its own.
  A reader never pins an entry.
  It copies what it needs into its own memory and uses the copy as it is: the records of its projected columns, or a whole dictionary.
  The reader charges each copy, and the memory of decoding on a miss, to its permit with `request_memory()` before it allocates the memory.
  The charge lasts as long as the memory.
  Nothing that holds a reader permit enters the cache: an insertion copies the bytes into the LSA region.
- **Mapping to codec, on write.**
  The mapping appends a value to a leaf column and ends the row.
  Ending a row reports whether a page set ended there, which is when the writer adds a row index entry.
- **Mapping to codec, on read.**
  The mapping gets one cursor for each leaf column in the projection.
- **Writer to sstable.**
  The Parquet writer implements `writer_impl`.
  The `sstable_writer` constructor chooses the implementation by format.
  The `mx` writer holds sealing, the large data records, and the driving of the index writers in one class.
  These parts are first extracted into helpers that both writers use, in commits without functional change.
- **Reader to sstable.**
  The Parquet reader has three entry points, with the arguments of their `mx` counterparts: a reader, a full-scan reader, and `validate`.
  `sstable::make_reader`, `make_full_scan_reader`, and `validate` choose by format.
  What the reader keeps from the footer is stored in `shareable_components`, so a load on another shard reuses it.
  `column_translation` is used unchanged.
- **Index.**
  The interfaces do not change.
  The `total_data_db_file_size` argument of `make_bti_index_reader` gets a neutral name.
- **Sizes and progress.**
  `data_size()`, `estimated_keys_for_range()`, `writer_offset_tracker`, and `reader_position_tracker` keep their signatures.
  The Parquet code feeds them the measures under "Size measures".
- **Writer memory budget.**
  A writer registers with the sstables manager through a handle that it holds for its lifetime.
  It asks the handle for its row group target.

### Reading the Data file

- The decoders of pages, OffsetIndexes, and dictionaries work on bytes that are already in memory.
  They do no I/O.
- A fetcher inside the codec decides which byte ranges to read.
  Every range is known before the read.
  The footer locates each OffsetIndex, the OffsetIndex gives the offset and the size of each page, and a dictionary page spans from the start of its chunk to the first data page.
- The fetcher reads from a `seastar::file` that the sstable provides.
  Tracing and encryption wrap the file as they do today.
- The fetcher charges every read buffer to the reader permit itself, with the awaiting `request_memory()`, before it issues the read.
  It aligns each request to the file's read alignment, so the charge is the size of the buffer that the file allocates.
  The file is not tracked by the permit, so nothing is charged twice.
- A request is at most 128 KiB, so no read allocates a contiguous buffer above 128 KiB.
- The footer is the exception.
  Its size has no bound and the reader must not hold it in memory, so it is parsed from an input stream over the end of the file.
- A page holds whole values, so a large cell makes a large page.
  A page buffer is a chain of contiguous pieces of at most 128 KiB, like the writer's row group buffer.
  Page compression and decompression must therefore work on such chains.
  The writer encodes, checksums, and writes a page one contiguous piece at a time, with a preemption point between pieces.
  It holds at most two copies of a page at a time.
- No compression or decompression call allocates a contiguous buffer above 128 KiB.
  The only exceptions are compression codec contexts, which are allocated once and kept until shutdown, under a guard against the large allocation warning.
- A decompression whose input and output are at most 128 KiB runs synchronously, and so does a compression of at most 32 KiB.
  Each takes at most a few hundred microseconds at the default levels.
  A larger call works in steps of at most 64 KiB and may yield between them.
  LZ4 and Snappy cannot stop inside a block, so for such a page the codec compresses chunks with the library and joins them into one block or stream, and it decompresses with its own decoder.
- A compression codec context that a call holds across a yield is private to a shard and a scheduling group, and it has a lock.
  A call never waits for a call of another scheduling group.
  Small calls use per-shard contexts that they never hold across a yield.
  The calls do no I/O and do not wait for reader memory: the caller reserves the memory first.
- Zstd frames have a window of at most 2^17 bytes, so the decoder's workspace has a fixed size.
  The reader rejects a frame with a larger window.

Read-ahead keeps compaction, repair, and streaming bound by throughput, as the stream's read-ahead does for `mx`.

- Read-ahead is the fetcher's job, and its unit is rows, not bytes.
  A scan consumes rows, while the file is ordered by column: the first row of a row group needs the first page of every column.
- The fetcher keeps a window of upcoming row ordinals in the read direction.
  For each column with data, the pages that cover the window form one byte range.
  The fetcher merges two ranges only when their aligned extents overlap or touch, splits long ones into requests of 128 KiB, and issues them while the reader decodes the current window.
  So read-ahead reads no byte of other columns' chunks, or of pages outside the window, beyond the blocks that the device reads for the bytes that the read needs.
- The window never moves past the end of the reader's current range, as far as the index tells.
  This holds for a reader that may be fast-forwarded to another partition range too.
- Read-ahead waits for memory, but never ahead of the pages that a cursor waits for: those ask for memory first.
- An error in read-ahead reaches the cursor that asks for the data, and nothing else.
- Between two calls of its consumer, a reader may be registered as an inactive read, and an inactive permit must not ask for memory.
  The semaphore would queue it as a waiter, and resuming the read would then fail an assertion.
  So the codec asks for memory only within a call of its consumer, or while such a call waits for the codec.
  Read-ahead work that completes in the background, such as the load of an OffsetIndex block, asks for no memory then, and the next call goes on with it.
- Registering a read as inactive fails the memory requests of its permit that wait, with `std::bad_alloc`.
  Read-ahead that failed so is fetched again, once, when a cursor asks for its data.
- Closing a reader stops issuing reads, cancels the reads that wait in the I/O queue, and waits for the reads in flight.
  A read that waits for memory is not issued once it gets it, but close waits for it, because the semaphore cannot withdraw a memory request.
  When close returns, the reader holds no memory of its permit.
- The window grows while the fetched and unconsumed bytes are below a budget.
- Backward read-ahead is the same mechanism, with the window below the current row.
- The OffsetIndexes of upcoming row groups are fetched ahead in the same way.
  The OffsetIndex block of a row group is read and cached whole, with the OffsetIndexes of columns outside the projection.
  A reader copies only those of its projected columns out of the cache.
  Reading single OffsetIndexes would need their locations in memory, one per chunk, where the summary keeps a few bits per chunk.
  The sstable reader projects every column of the schema, so the block holds little else.
- A single-partition read starts with a window of one page set and doubles it while the read continues.
- The average request size is the budget divided by the number of columns with data.
  The budget is therefore 128 KiB per column with data, with a floor of 640 KiB and a cap of one row group.
  The floor is what an `mx` reader uses.
- At the cap, the ranges are whole column chunks.
  They are adjacent, so the scan reads the row group sequentially.
- With row groups of 8 MiB, a table with more than about 60 columns with data reaches the cap.
  A compaction of 32 such sstables then holds about 256 MiB of read buffers, where `mx` holds about 20 MiB.
- These numbers are estimates.
  Nothing was measured, and the performance phase tunes them.
- The fetcher is separate from the decoders from the start, so the performance phase changes only the fetch policy.

### Codec

- New, Seastar-native, in the tree.
- Do not consult parquet4seastar during the implementation.
  This is deliberate.
  A comparison against it happens only after the project is done, to see whether we missed something.
- It supports only the encodings our writer produces.
  The writer picks the encoding per column at run time.
- The column cursor supports `seek`, `next`, and `previous` over a decoded page, so that forward and reverse reads share one code path.
- The reader takes a column projection (the set of columns to read) and skips all other columns.
  This is implemented and tested at the Parquet layer only.
  Callers above it pass a full projection for now.
  The row cache always reads all columns, and integration with the rest of the database is out of scope.
- The page compressor interface knows which column it compresses.
- Thrift structures use a hand-written compact protocol codec.
  It streams the footer into our own compact structures, with preemption points.
  Generated Thrift code would materialize the whole footer in large contiguous allocations.

### Encodings

- Dictionary encoding is used, with a small limit on the size of a dictionary page.
  The limit is a configuration option, with a default of a few KiB.
- When a dictionary exceeds the limit, the remaining pages of that chunk use another encoding.
  This is Parquet's standard fallback, and each page header names its own encoding.
- Dictionary pages share the evictable cache of the OffsetIndexes.
  The reader keeps one bit per column chunk in memory: whether the chunk has a dictionary.
  It can then fetch the dictionary and the data page at the same time.
- The first version supports four encodings: plain, dictionary, delta for integers, and delta-length for byte arrays.
  Prefix compression for sorted keys and byte-stream-split for floating point can follow.
  Each addition needs a feature flag, but no layout change.
- The selection rule is a writer policy.
  The writer estimates the encoded size of each candidate and picks the smallest.

### Page compression

- The writer follows the table's compression option.

  | Table option | Parquet compression codec |
  |---|---|
  | none | `UNCOMPRESSED` |
  | LZ4, LZ4 with dictionaries | `LZ4_RAW` |
  | Snappy | `SNAPPY` |
  | Deflate | `GZIP` |
  | Zstd, Zstd with dictionaries | `ZSTD`, at the table's `compression_level` |

- The dictionary variants use the same compression codec without a dictionary.
  Trained dictionaries stay in the optional last phase.
- A table with default settings (LZ4 with dictionaries) therefore gets `LZ4_RAW` pages.
  The user must choose Zstd to get it, and the documentation must say so.
- `chunk_length_in_kb` has no effect, because pages replace chunks.
- All columns use the table's compression codec.
  Parquet records the codec per column chunk, so a per-column choice can be added later without a format change.
- The writer stores a page uncompressed when compression does not shrink it.
  Data Page V2 has a flag for this.
- The reader supports all five compression codecs, because a file states its own codec.

### Encryption at rest

- The existing file-level encryption is kept, unchanged.
  It wraps each component file below the sstable code, so the Parquet reader and writer see plaintext and need no extra code.
- For an encrypted table, the Data file on disk is not a Parquet file.
  Its plaintext is one, and the conformance assumption applies to the plaintext.
- The codec does not implement Parquet's own encryption.
- No tool writes a decrypted Data file, for now.
  If one is wanted later, the `decompress` operation of `scylla sstable` is the place: it would have to accept a Parquet sstable and copy its plaintext Data file.

### Physical layout

- Row group size, page set size, and minimum page size are configuration options.
- The writer buffers a whole row group, so writer memory bounds the row group size from above.
- Writer buffers have a per-shard budget.
  The sstables manager counts the active Parquet writers on the shard.
  Each writer's row group target is the budget divided by that count.
  The target is capped above by the configured row group size and below by one page set.
  A writer above its target cuts the row group early.
- Nothing blocks on the budget.
  One reader feeds all writers of a time-window split, so a blocked writer would stall the rest.
- The accounting is a per-shard counter and a metric.
  It is separate from dirty memory and from reader permits.
  Each writer reports the memory that it holds: its row group buffer, its footer records, and its dictionaries.
  The counter is the sum of the reports.
  The target limits only the row group buffer.
- A row group buffer is a chain of chunks of at most 128 KiB, never one contiguous allocation.
- The fixed cost of leaf columns argues for larger row groups on wide tables, but the code does not enforce a minimum.
  Memory wins: under pressure a wide table gets smaller row groups and a larger footer.
- Indicative values, to be tuned in the performance phase: a budget of 5% of shard memory and a row group size of 8 MiB.
- Row group size is a trade-off.
  Smaller row groups mean cheaper OffsetIndex reads, less writer buffering, and a lower cap on the read-ahead memory of a scan.
  Larger ones mean a smaller footer, faster opens, and less writer memory for footer metadata.
  The default is expected between 4 and 16 MiB.
- The writer holds the metadata of the whole footer in memory until the end of the file.
  Its size is about the file size divided by the row group size, times the number of leaf columns, times the size of one entry.
  This is accepted as it is, with no cap on sstable size for wide tables.
- Pages carry checksums.

### Metadata on the read path

- The reader parses the footer once, when the sstable is opened.
  It keeps two things per row group: the row count and the location of the row group's OffsetIndexes.
- The footer has a checksum, stored in Scylla.db.
  The reader verifies it while it parses the footer.
  A string in the footer is one contiguous allocation, so a string above 128 KiB is malformed.
  The writer therefore rejects a longer name, and a table with a column or field name whose Parquet name is longer keeps the `big` format.
- OffsetIndexes and page headers have no checksum, so the reader distrusts them.
  It raises the malformed sstable error when a value fails a sanity check or contradicts other information, for example an offset that points outside the row group.
  A size or a count from them, such as the entries of a page, is checked against a bound from the footer before the reader asks for memory for it.
- The writer places all OffsetIndexes of one row group next to each other, directly after the row group.
  It therefore does not hold them until the end of the file.
  The reader fetches them with one read on first touch, and keeps them decoded in an evictable cache.
- The writer records the null count of each column chunk in the footer.
  At open, the reader derives one bit per column chunk: whether the chunk is entirely null.
  For such a chunk it does no I/O and no decoding.
- A dictionary page needs no record of its own.
  It spans from the start of its chunk to the chunk's first data page, and the reader derives the start of the chunk.
- A chunk is "in place" if it lies inside its row group.
  The chunks that a late column gets for earlier row groups are not in place.
  They are at the end of the file and entirely null, so the reader never locates them.
- The writer writes the in-place chunks of a row group in schema order.
  The row group's OffsetIndex block holds their OffsetIndexes in the same order.
- The first in-place chunk of a row group starts where the OffsetIndex block of the previous row group ends.
  In the first row group it starts after the file's magic bytes.
  Every other in-place chunk starts where the previous in-place chunk ends, which is the end of that chunk's last data page.
- The reader keeps one number per column: the first row group in which the column's chunk is in place.
  It derives the number from the footer at open.
- A compact row group directory in Scylla.db could later avoid parsing the footer at open.
  It needs no change to the Data file.

### Page cutting

- The writer cuts pages in all columns at the same row boundaries.
  A "page set" is the group of pages that cover the same rows.
  Its target total size bounds the decode work and the reader memory of a point read.
- A column skips a cut while its pending page is below the minimum page size, so page boundaries nest.
  This limits the per-page overhead for sparse columns and keeps a compression window for narrow ones.
- Row index entries are added only at page set boundaries.
  A partition gets row index entries only if it spans more than one page set.
- The index has no auto-scaling: every page set boundary inside a partition gets an entry, whatever the partition's size.
  This matches `ms`.
  Indexing only every second or fourth page set of a huge partition can be added later without a format change.
- These are writer policies, not format invariants.
  The reader finds pages by ordinal through the OffsetIndex, so it handles any cutting policy.
  The sizes can be tuned without a format change.

### CPU per row

A table with many leaf columns and few non-null cells per row must not pay for every column on every row.

- In the writer, the cost of a row is proportional to its non-null cells, plus the fixed set of row-level columns.
- Null runs are filled lazily.
  Each leaf column remembers how many rows it has recorded.
  When a value arrives, the column first records the gap as one null run, in constant time.
- The page set size check uses a running total that every append updates.
  It does not scan the columns.
- A page set cut visits only the columns that received a value since the last cut.
  The writer keeps a list of them.
  An untouched column holds only a pending null run, and its page stays open.
- The end of a row group visits every column of the file, because every chunk needs a footer entry.
- A column with no non-null value in the file so far has no state.
- In the reader, a column cursor inside a null run is parked until the run ends.
  The reader keeps the parked cursors ordered by the ordinal at which they wake.
  The cost of a row is then proportional to its non-null cells as well.
- The column cursor exposes the length of the current null run.

### Scrub and validation

- Scrub behaves as it does for `mx`, with no new scrub code.
  The full-scan reader raises the malformed sstable error at the first row that it cannot produce.
  The existing scrub code then aborts, or keeps the rows before that point, depending on the mode.
- Any inconsistency that the reader detects counts as such an error.
  Examples are column chunks with different row counts, and a row with no fragment kind.
- Dropping only the rows that a corrupt page covers is left for later.
  It needs no format change, because every page has its own checksum and is found by ordinal.
  If it is added, it must drop whole rows, extend the gap to the next partition start when a key is lost, and close an open range tombstone at the gap.
- Page checksums are verified on every read, as for compressed `mx` sstables.
- The `validate` mode does three things.
  It scans the file with order validation.
  It cross-checks partition and block ordinals against the trie index.
  It checks the structure: the footer's row counts against the OffsetIndexes, and a non-null first key column exactly on the rows that start a partition.
- Checksum validation verifies the file digest and the checksum of every page.

### Size measures

Byte offsets in the Data file no longer measure anything useful, so two measures replace them.

- The logical measure is the raw size.
  The raw size of a row is the sum, over its non-null values, of the value's plain size: the width for a fixed-width type, the length for a byte array.
  It covers value columns and metadata columns.
  It depends only on the row's content, so a partition has the same size in every file.
  It is close to the `mx` uncompressed size, so the existing large-data thresholds keep their meaning.
- The raw size is used for large partition and large row detection, the partition size histogram in Statistics.db, write progress, and `data_size()`.
  Large cell detection uses the value size, as it does today.
- The file's total raw size is stored in Scylla.db, so `data_size()` does not need the footer.
- Read progress is the total raw size times the rows read, divided by the total rows.
- The maximum sstable size cut, and the split of a large partition, use an on-disk estimate.
  The estimate is the bytes already flushed, plus the pending raw size times the file's compression ratio so far.
  Its error is at most one row group.
- The key estimate for a token range uses ordinals: the ordinal range divided by the total rows, times the key count.
- Read-ahead is not a size question.
  It is described under "Reading the Data file".

### Format selection

- Parquet is a new format, `pq`, next to `big`, and a new version within it, `aa`.
  A version is a minor update within a format.
- Versions stop being a linear order.
  The order was only a cheap way to check for features.
  Every ordered comparison of versions is replaced by a predicate that names the feature it checks, such as `has_summary_and_index`.
  This refactoring is accepted as part of the work.
- The format is a function of the version, because `aa` exists only in `pq`.
  An interface that takes a version does not also take a format.
  The file name still carries both.
- Version `aa` has the trie index and the other features of `mt`.
- The opt-in is a cluster config option with table, keyspace, and cluster scopes (`docs/dev/cluster_config.md`).
  The working name is `sstable_data_format`, a text option with the values `native` (the default) and `parquet`.
  The node option `sstable_format` keeps choosing the version within the `big` format.
- The option's automatic cluster feature is the gate.
  The option is registered in the release that can read and write Parquet, so no second feature is needed.
- One central function decides the format and the version of a new sstable.
  It replaces `sstables_manager::get_preferred_sstable_version()`.
  It is the only code that reads the node option and the cluster config option.
  Every path that creates an sstable for a table calls it: flush, compaction, streaming, repair, upload, reshape, and `upgradesstables`.
- The sstable and the writer receive the format and the version, and never read configuration.
  Writer tunables such as the row group size reach the writer as plain values in `sstable_writer_config`.
- A change of the option takes effect on the next sstable that the table creates.
  Existing sstables stay until they are compacted.
- `upgradesstables` rewrites an sstable whose format differs from the table's choice, or whose version is older within the same format.
  A table can therefore be converted in either direction.
- A Parquet sstable may arrive in a table that has the option off, through file streaming or restore.
  The reader follows the file's own format.
- System tables ignore the option.
- The central function never chooses Parquet for a compact storage table.
  The `mx` reader ignores the header's column list for a dense table, so the rule that the header is the authority for column names does not hold there.
  The reader rejects a Parquet sstable in a compact storage table.

### Testing

Each layer is tested at its own interface, against an oracle that shares no code with it.

| Layer | Test | Oracle |
|---|---|---|
| Footer codec | Hand-written vectors, and truncated and bit-flipped footers | Arrow parses our footer; damage raises the malformed sstable error |
| Codec writer | Random columns, page sets of one row, late columns, dictionary fallback, all five compression codecs | Arrow C++ reads the same values |
| Codec reader | Files from our writer and from the Arrow writer | An in-memory model of the columns |
| Column cursor | Every short sequence of `seek`, `next`, and `previous` on a small file, in the style of `test/boost/bti_index_test.cc` | The same model |
| Fetcher | A file that counts reads | No read for an all-null chunk or for a column outside the projection |
| Mapping | A table of hand-written fragments and their expected Parquet cells | Arrow, against literal values |
| Mapping | Random schemas and mutations, with a writer that picks the row defaults at random | The input, and an `mt` sstable through `compare_readers` |
| Sstable | `aa` in the version loops: the mutation source suite, and the datafile, compaction, scrub, and schema change tests | The existing tests |
| Reverse reads | The mutation source suite | The in-memory reversing reader |
| Resources | Column visits per row on a wide sparse table, the largest allocation, 100 writers at once, injected allocation failures | Fixed bounds |
| Whole system | cqlpy and the `cql_test_env` suites with Parquet as the default; cluster tests for the option, `upgradesstables`, streaming, and encryption | The existing tests; Arrow opens a node's files |

- Arrow C++ is the reference reader and writer for conformance.
  It is linked into test binaries only.
- Arrow is an optional build dependency.
  `libarrow-devel` and `parquet-libs-devel` are added to `install-dependencies.sh`, with the matching packages for the other distributions.
  Without Arrow, both build systems leave out the Arrow tests, and test.py skips them.
  The toolchain image is regenerated later, so that CI runs them.
- The table of literal mapping cases pins the format: an accidental format change breaks it.
  Arrow also checks the invariants of the mapping, such as a non-null first key column exactly on the rows that start a partition.
- The format follows from the version, so a test that loops over the versions reaches `aa` through its loop.
  Each such loop either covers `aa` or states why it does not.
- A test-only switch makes Parquet the default for new sstables.
  It arrives with the `sstable_data_format` option.
  The suites run with it by hand during the project, and as a CI mode later.
  Its form is not decided.
- The resource tests assert on counters, not on time.
- The corruption tests need no I/O, because the decoders work on buffers.
  They run with the sanitizers of the debug mode.
- End-to-end conformance is validated in the operations phase: Arrow opens files written by a real node.
- Once the format is frozen, golden files go into `test/resource/sstables`, to guard compatibility with older builds.

### Other

- CompressionInfo.db and CRC.db go away.
  `Digest.crc32` stays, because it also covers the footer and the page index.
  The per-component digests in Scylla.db stay as well.

## Phases

1. **Mapping decisions.**
   They are settled, and they define the codec's scope.
   They later become the format documentation in `docs/dev`.
2. **Codec.**
   Add Arrow to `install-dependencies.sh` as an optional dependency before the first Arrow test is merged.
   Repeat early, with files from our own writer, the check that Arrow C++ accepts a row group whose chunks are not adjacent.
3. **Writer.**
   First replace the ordered comparisons of versions with feature predicates.
   Extract the shared parts of the `mx` writer into helpers, without functional change.
   Then add the format `pq` with version `aa` and its `writer_impl`, reusing the Statistics, Filter, Scylla.db, and trie index writers.
   Add the central function that decides the format and the version of a new sstable.
   Until the option exists, tests pass the format explicitly.
   The writer uses the raw size from the start: for large data detection, the partition size histogram, write progress, and the total in Scylla.db.
   It adds row index entries by the page set policy, not by Data file offset.
4. **Reader, in both directions.**
   Full scans, single-partition reads, fast-forwarding, clustering slicing, and native reverse reads.
   Full scans enable compaction, scrub, mutation-based streaming, and tools.
   Add the format to the version-parameterized tests, especially the mutation source conformance suite.
   The in-memory reversing reader serves only as a test oracle.
5. **Byte-offset audit.**
   Move the remaining code that treats Data file positions as bytes to the measures under "Size measures": the maximum sstable size cut, the split of a large partition, read progress, `data_size()`, and the key estimate.
6. **Operations.**
   Checksum validation, scrub, `scylla sstable` operations, metrics, the `sstable_data_format` option, and the `upgradesstables` change.
   The test-only switch that makes Parquet the default, and a run of the existing suites with it.
   End-to-end conformance validation with Arrow.
7. **Performance.**
   Size tuning, cache sizing, the read-ahead policy in both directions, reader memory accounting.
8. **Later, optional: trained zstd dictionaries.**
   This is an opt-in that breaks conformance.
   The dictionary is stored base64-encoded in the footer's key-value metadata, under a key prefix that we reserve now.
   Training needs rework, because column pages differ from row-format chunks.
   The compression codec label (a new codec value or the existing zstd label) is decided then.

## After the project

- Compare sizes with `ms`.
  Without representative data, a comparison cannot change any decision, so it is deliberately left until all the work is done.
- Compare the codec with parquet4seastar, to see whether we missed something.
- Optional: open the files with DuckDB, a Parquet reader that shares no code with Arrow.

## Risks

- **Small or negative gain on some shapes.**
  Blobs and tiny partitions are the likely cases.
- **Point-read cost.**
  A row with C columns touches up to C data pages plus dictionary pages.
  The page set size bounds the decode work, but not the number of reads.
- **Memory.**
  Each writer holds a row group, within the per-shard budget, plus the footer metadata.
  Each reader holds one page per column that it reads.
  A scan also holds read-ahead buffers, up to one row group on a wide table.
- **Large cells.**
  A page holds whole values, and it is compressed as one unit.
  A compression codec that needs contiguous buffers would force a large allocation for a large cell.
- **Unknown workloads.**
  Public datasets may not match real ones.

## Considered and dropped

- **A pyarrow prototype on public datasets, and a smaller check script.**
  The format is mandated and the data is not representative, so the results would not change any decision.
- **Choosing the cell metadata layout by measurement.**
  Its cost depends on write patterns, which we would have to invent.
- **A separate row for the partition start, as in `mx`.**
  For single-row partitions it doubles the row count.
  Every populated column then alternates between null and non-null, which defeats the run-length encoding of the null markers.
- **A null cell timestamp that means only "same as the marker", as in `mx`.**
  Rows without a marker would store a timestamp in every cell.
  In a columnar file the copies land in different columns, where no compressor removes them.
- **Other representations of empty values.**
  An extra definition level turns every fixed-width value column into a structure with one field.
  A single exceptions column adds cross-column logic on a rarely tested path.
  Byte arrays for the affected types lose the typed encodings.
- **Lists of structs for collections and counters.**
  Each entry would hold the key, the value, and the metadata.
  Every metadata column then costs at least one bit and one append per element, even when all elements use the row's defaults.
  It also puts metadata under the CQL name.
- **Collections and counters as blobs.**
  This keeps repeated fields out of the codec, but elements get no columnar encoding.
- **One Parquet row per collection element.**
  It breaks the rule of one row per mutation fragment, which the index ordinals depend on.
- **A non-frozen UDT stored like a map.**
  Field values would lose their typed encodings.
- **The counter's total under the CQL name.**
  It is redundant with the shards and costs one more column.
- **A separate list for dead collection elements.**
  The reader would have to merge two sorted lists to restore key order.
- **A separate boolean column for the row marker.**
  Folding the marker into the fragment kind saves a leaf column.
  It costs at most 2 bits per row more, and only where markers are irregular and the row group has many kinds.
- **Separate columns for the tombstone of a bound row.**
  Sharing the row tombstone columns saves two leaf columns.
- **Absolute tombstone timestamps.**
  Differences from the row's default follow one rule for all timestamps, and a collection tombstone's difference is usually the constant -1.
- **A reserved TTL value for a dead marker, as in `mx`.**
  It is a magic value, and the row then has no TTL default for its cells.
- **A string enumeration for the fragment kind.**
  The reader would have to map dictionary entries to kinds on every page.
- **An explicit prefix length column.**
  It is redundant with the nulls in the key columns.
- **Short names in the reserved group, such as `_` and one-letter metadata names.**
  The column path repeats in the footer once per column chunk.
  In a pyarrow test, short names cut a metadata chunk's footer entry from about 107 to about 93 bytes.
  This is deferred, not rejected: it can be introduced later under a new layout identifier.
- **Altering only a name that equals the reserved one.**
  The altered name can collide with another column unless the rule covers a whole family of names.
- **Positional names for key columns, such as `pk0`.**
  They hide the real names, and the reader does not use the names anyway.
- **An unspecified placeholder for empty values.**
  Two writers could then produce different files from the same data.
- **Always Zstd, whatever the table's compression option says.**
  It overrides a user who chose LZ4 for CPU reasons.
- **A separate compression codec setting for Parquet.**
  It is a second compression setting that users must understand.
- **Parquet modular encryption for the Data file.**
  A standard tool with the keys could open the file directly.
  It needs an encryption layer in the codec and a mapping from our key providers, and the other components would stay on file-level encryption.
- **Refusing Parquet on encrypted tables.**
  It excludes every user of encryption.
- **Parquet for compact storage tables.**
  They are deprecated, and they need special cases for column names and for clustering rows with a prefix key.
- **Reading the timestamps of a column that is absent from the current schema, to keep the check that `mx` makes.**
  It costs the I/O that a columnar reader can avoid.
- **One page set per row group.**
  A point read would need a single read, but every leaf column would need a footer entry for every page set.
  That makes the footer very large on wide tables.
- **Our own page directory in place of the OffsetIndex.**
  With bounded row groups, the standard structure is cheap enough.
- **An all-null flag in a ColumnIndex.**
  It saves a little permanent memory but adds a structure that we decided not to write.
- **Positional or compound Parquet column names.**
  Plain CQL names are more useful to people, and their only cost is a somewhat larger footer.
- **Replacing the values of a corrupt page with nulls during scrub.**
  A null in a timestamp or expiry column means "same as the row", so this would silently change the data.
- **Two tombstones per range tombstone row.**
  It costs two more leaf columns.
- **Taking the "other side" tombstone from the index during reverse reads.**
  It stores the least data, but it needs a "previous block" method on the index and a block-by-block walk in the reader.
- **The in-memory reversing reader as an intermediate step.**
  Native reverse reads are a requirement, and retrofitting direction into a forward-only reader costs more than designing for both.
- **Decoders that read from a stream.**
  Every read's position and length are known in advance, a page is verified and decompressed as a whole, and seeks and reverse reads fetch pages in any order.
- **One input stream per column, with the stream's own read-ahead.**
  Each stream carries its own read-ahead buffers, which is costly on wide tables, and a stream cannot read backward.
- **A new byte source interface for reads.**
  `seastar::file` already composes with permit tracking, tracing, and encryption, and a test can wrap it to count reads and inject faults.
- **`storage::make_data_or_index_source` for reads.**
  For any offset other than zero it returns a plain file data source over the same file, on local and on object storage.
- **Copying the shared code of the `mx` writer.**
  Two copies of sealing and of the large data records would drift apart.
- **Format and version as independent arguments.**
  Every test that loops over the versions would need a second loop or a change at each call site.
- **A second reader built on Arrow, as the oracle for the mapping.**
  It is a stronger oracle than literal cases and invariant checks, but it is a second implementation to maintain.
- **Arrow tests that are built only when Arrow is installed.**
  A test that is not always built is not always run.

## Checked in the code

- Index blocks are cut by Data file offset, 64 KiB by default (`sstables/mx/writer.cc`).
  A partition gets index entries only when it has at least two blocks (`add_pi_block`).
- Scylla does not depend on Thrift: `configure.py`, `CMakeLists.txt`, and `install-dependencies.sh` do not mention it.
- The in-memory reversing reader exists (`make_reversing_reader` in `readers/reversing.hh`).
- The `ms` reader path is selected by the absence of Index.db, and both `mx` and `ms` go through `mx::make_reader` (`sstables/sstables.cc`).
  The Parquet reader needs its own dispatch.
- Empty values are allowed in clustering key components and in components of a composite partition key.
  Views and secondary indexes can also store an empty single-column partition key.
  Almost every fixed-width type accepts an empty value (`validation.cc`, `types/types.cc`).
- Validation accepts any single byte as a boolean, and every non-zero byte reads as true.
  Ordering and the byte-comparable encoding of the trie index treat all non-zero bytes as equal, while equality and the token use the raw byte (`types/types.cc`, `types/comparable_bytes.cc`).
- A non-frozen collection is an optional collection tombstone plus a list of elements.
  Each element has a key and is a full cell, with its own timestamp, TTL, expiry, or deletion time (`sstables/mx/writer.cc`).
  The key is the map key, the set element, a `timeuuid` for a list, or the field index for a non-frozen UDT.
- Encryption at rest wraps each sstable component file, except Scylla.db and the TOC, in an encrypted file (`wrap_file` and `wrap_sink` in `ent/encryption/encryption.cc`).
  The encrypted file works in blocks of 4096 bytes, supports random access, and reports the plaintext length (`ent/encryption/encrypted_file_impl.cc`).
  The Data file of an encrypted table is always encrypted.
- `scylla sstable` can read encrypted sstables.
  It registers the encryption extension and takes the key settings from `scylla.yaml` (`tools/utils.cc`, `ent/encryption/encryption_config.cc`).
  It cannot do so with the replicated key provider, which needs database services (`test/cluster/test_encryption.py`).
  A subagent reported this, and the main session checked only the two points below.
- The `decompress` operation of `scylla sstable` writes the decompressed Data stream to a file, and decrypts it as a side effect.
  It skips an sstable that has no sstable-level compression, so it would skip every Parquet sstable (`decompress_operation` in `tools/scylla-sstable/scylla-sstable.cc`).
  No other tool in the tree writes a decrypted component file.
- A table's compression option names one of seven algorithms: LZ4, LZ4 with dictionaries, Zstd, Zstd with dictionaries, Snappy, Deflate, or none (`sstables/compressor.hh`).
  The default for user tables is LZ4 with dictionaries (`sstable_compression_user_table_options` in `db/config.cc`).
- pyarrow 23 writes the compression codecs `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `LZ4_RAW`, and `ZSTD`, and DuckDB 2.0.0 reads each of them correctly.
- Index auto-scaling never triggers for `ms`.
  It is driven by the size of the in-memory buffer that holds a partition's serialized row index for Index.db, and `ms` does not fill that buffer (`add_pi_block` and `write_pi_block` in `sstables/mx/writer.cc`).
- The reader maps each static and regular column of the serialization header to the current schema by name (`column_translation::state::build` in `sstables/sstable_mutation_reader.cc`).
  It takes the write-time type from the header and passes value bytes on unchanged.
  - A column whose current type is not value-compatible with the write-time type, or whose collection or counter nature changed, raises the malformed sstable error at the first cell that is read (`sstables/mx/reader.cc`).
  - A cell of a column that is absent from the current schema is skipped if the column is recorded as dropped and the cell's timestamp is not newer than the drop.
    Otherwise it raises the malformed sstable error.
  - A cell of a column that was dropped and added again is skipped if its timestamp is not newer than the drop.
  - For a dense table (compact storage with clustering columns), the reader ignores the header's column list and uses the schema's single regular column.
- Creating a compact storage table is deprecated and disabled by default (`enable_create_table_with_compact_storage` in `db/config.cc`).
- The serialization header stores names and types for static and regular columns, and only types for key columns (`sstables/types.hh`).
- A column name is a byte string.
  Its type is UTF-8 for CQL tables, but the schema allows another name type (`regular_column_name_type` in `schema/schema.hh`).
- A footer written by pyarrow 23 takes about 90 to 127 bytes per column chunk, for nested metadata columns with CQL names of 8 to 24 characters.
- A row marker is missing, live, live with a TTL and an expiry, or dead with a deletion time (`mutation/mutation_partition.hh`).
  `mx` writes a dead marker with a reserved TTL value (`sstables/mx/writer.cc`).
- A row can have a regular tombstone and a shadowable tombstone at once, and `mx` writes both.
- Deletion times and expiry times are 64-bit counts of seconds (`gc_clock.hh`).
- A counter is one cell with one timestamp and no TTL.
  Its value is a list of shards, and each shard has an id, a value, and a logical clock (`mutation/counters.hh`).
- In ordinary operation a shard runs about 5 to 10 sstable writers at once.
  Flush data writes are serialized per dirty memory manager (`replica/dirty_memory_manager.hh`).
  Regular compaction runs one job per weight class, where the weight is log4 of the input size, and maintenance compactions run one at a time (`compaction/compaction_manager.cc`).
  A shard receives 2 tablet streams at once by default (`tablet_streaming_write_concurrency_per_shard`).
  Repair runs repair memory / 32 MiB / 4 ranges at once (`repair/repair.cc`).
- A table with time-window compaction keeps up to 100 writers open at once, one per time window, in a single flush, compaction, stream, or repair (`compaction/time_window_compaction_strategy.cc`, `mutation_writer/timestamp_based_splitting_writer.cc`).
- Sstable writers have no memory accounting today.
  An `mx` writer holds a few buffers of 128 KiB (`sstables/mx/writer.cc`).
- The code has two size measures today.
  The uncompressed Data offset serves large partition and row detection, the partition size histogram, write and read progress, and `data_size()` (`sstables/mx/writer.cc`, `sstables/file_writer.hh`, `sstables/consumer.hh`, `sstables/sstables.cc`).
  The compressed file length serves the maximum sstable size cut and the split of a large partition (`get_data_offset()` in `sstables/mx/writer.cc`).
- Compaction strategies, the compaction backlog, and the tablet size use `data_size()` (`compaction/compaction_strategy.cc`, `replica/table.cc`).
- The `ms` key estimate for a token range divides a range of index positions by `data_size()` (`sstable::estimated_keys_for_range` in `sstables/sstables.cc`).
- The sstable version is chosen per node, from the node option `sstable_format` and the cluster features `ms_sstable` and `mt_sstable` (`sstables_manager::get_preferred_sstable_version`).
  Its callers are `table::make_sstable`, the upload and reshape paths in `replica/distributed_loader.cc`, `compaction_manager::perform_sstable_upgrade`, `api/system.cc`, and `scylla sstable`.
- A table already holds sstables of several versions at once.
- `sstable_format_types` has the single value `big`.
  It appears in the file name, the sstable descriptor, the streaming metadata, and the object storage attributes.
  About 20 files outside the tests name it.
  Not every use was read.
- Production code has 22 ordered comparisons of a version against a constant: 15 in `sstables/sstables.cc`, 3 in `sstables/sstables.hh`, 2 in `sstables/sstables_manager.cc`, and one each in `sstables/sstable_set.cc` and `sstables/index_reader.hh`.
  `test/boost/sstable_datafile_test.cc` has 21 more.
- `writer_impl` has seven `consume` methods and `data_file_position_for_tests()` (`sstables/writer_impl.hh`).
  The `sstable_writer` constructor calls `mc::make_writer` for every version (`sstables/writer.cc`).
- The `mx` writer seals the sstable through private members of `sstable`, as a friend class.
  It also holds the large data records and drives the trie index writers (`sstables/mx/writer.cc`).
- The component map is chosen by version, and there are three maps: for `ka` and `la`, for `mc` to `me`, and for `ms` and `mt` (`sstables/sstable_version.cc`).
- `shareable_components` holds the compression metadata, the filter, the summary, the statistics, the Scylla metadata, the checksum, and the digest (`sstables/shareable_components.hh`).
- `test_env::make_sstable` takes the version and the format as separate arguments (`test/lib/sstable_test_env.hh`).
- An `mx` reader reads Data through an input stream with buffers of 128 KiB, up to 4 read-ahead operations, and a history that adapts both (`sstable::data_stream` in `sstables/sstables.cc`).
- `storage::make_data_or_index_source` returns a file data source over the given file.
  The one exception is object storage at offset zero, which gets a download source (`sstables/storage.cc`).
- A file tracked by a reader permit charges the buffer of every `dma_read_bulk` call to the permit (`reader_concurrency_semaphore.cc`).
- The LZ4 compressor of `sstables::compressor` writes a 4-byte length before the block.
  The Deflate compressor uses `deflateInit`, which produces zlib framing (`sstables/compressor.cc`).
- `install-dependencies.sh` does not mention Arrow or Parquet.
  The development container has `libarrow-devel` and `parquet-libs-devel` 23.0.1 installed.
- Tests have precedents for the planned techniques: a file wrapper that counts reads (`test/boost/sstable_datafile_test.cc`), exhaustive operation sequences (`test/boost/bti_index_test.cc`), and injected allocation failures (`test/boost/trie_writer_test.cc`).
- Cluster config is implemented, supports text values, and has table-scoped options in use, for example `auto_repair_enabled` (`db/cluster_config_registry.cc`).
- The trie index does no arithmetic on Data positions beyond adding a block's offset to its partition's start (`sstables/trie/bti_index_reader.cc`, `bti_row_index_writer.cc`, `bti_partition_index_writer.cc`).
  - A Partitions.db payload is a signed integer of 1 to 8 bytes.
    A value of zero or more is a position in Rows.db.
    A negative value is the bitwise complement of a Data position.
  - A Rows.db block payload is the offset from the partition start, stored in as many bytes as it needs.
    An offset of zero is valid.
  - The Rows.db header stores the partition start as an unsigned variable-length integer.
  - The final separator of a partition stores the partition end minus the partition start.
    `last_block_offset()` assumes that this separator exists.
  - The index reader takes the Data file size as a constructor argument and uses it only as the position of the end of the file.
  - The index reader never opens the Data file.
- Outside the `mx` and `kl` readers, the users of `data_file_positions()` are `sstable::estimated_keys_for_range` and the index dump of `scylla sstable`.
- Arrow C++ 23.0.1 starts no threads when its low-level Parquet writer and reader work on in-memory buffers.
  Its default memory pool in the Fedora build is the system allocator.
  It exposes the page locations of an OffsetIndex.
  A small standalone program measured this.
- Arrow C++ 23.0.1 and DuckDB 2.0.0 read correct data from files whose column chunks are not adjacent.
  Two subagents tested this with files built in two independent ways: written byte by byte, and written by pyarrow and then patched.
  The main session only spot-checked the results.
  - The layouts covered: the chunks of a late column for early row groups placed after the last row group, the same with a hole or garbage left behind, a relocated chunk with a dictionary page, a relocated first column, and all chunks in reversed file order.
  - Both readers also accept a file with an OffsetIndex and no ColumnIndex, and an all-null chunk that has definition levels and no value bytes.
  - Both readers locate a chunk from its own page offsets and its compressed size.
    Changing or omitting `RowGroup.file_offset`, `RowGroup.total_compressed_size`, `RowGroup.ordinal`, and `ColumnChunk.file_offset` had no effect.
  - Neither reader reads the bytes between a row group and its distant chunk.
  - DuckDB does not read a chunk whose footer statistics say that it is entirely null.
    Arrow always reads the pages.
  - Not covered: Data Page V1, repeated columns, remote storage, and other readers such as parquet-java or Spark, which may split a file by `RowGroup.file_offset`.
- Scrub reads through the full-scan reader with the integrity check on, and validates the order of fragments above the reader (`scrub_compaction` in `compaction/compaction.cc`).
  On a malformed sstable error, the `skip` mode ends the stream: it keeps the rows before the error and loses the rest of the sstable.
- The `validate` mode checks the order and cross-checks the Data layout against the index (`sstables/mx/reader.hh`).
- Compressed sstables verify the chunk checksum on every read (`sstables/compress.cc`).
  Uncompressed ones verify it only when the integrity check is requested (`sstables/sstables.cc`).
- Today's reverse reads take the "other side" tombstone from the markers in the Data file.
  They consult the index only at the range ends and after skips (`sstables/mx/reader.cc`, `sstables/mx/partition_reversing_data_source.cc`).

## Not verified

These assumptions still need a check in the code.

- How much code assumes the `big` format value.
- Whether the `seastar::file` of an sstable on object storage serves many small positional reads efficiently.
- Whether any encryption code depends on the sstable format value or on the set of components in a way that the new format would break.
  The new format adds no component type.
- Whether an empty column name, a column name that is not valid UTF-8, or a clustering row with a prefix key can exist outside compact storage tables.
  The escape rule and the prefix rule handle these cases either way.
- Whether the Parquet specification guarantees that the chunks of a row group need not be adjacent.
  Only the behavior of Arrow C++ and DuckDB was tested.

## Background, not verified

These notes support the decisions.
Nobody verified them in a main session.

### Reported by subagents from the code

Reverse reads and the index:

- `mx` walks backward with a reversing data source that follows a "previous size" field in each row (`sstables/mx/partition_reversing_data_source.cc`).
- The reverse-related methods of `abstract_index_reader` are `advance_reverse(pos)`, `advance_reverse_to_next_partition()`, `last_block_offset()`, and `reverse_end_open_marker()` (`sstables/abstract_index_reader.hh`).
- `mx` stores the tombstone that is open at a block's end.
  `ms` stores the tombstone that is open at a block's start (`sstables/trie/bti_row_index_writer.cc`).
- The trie has `trie::step_back` (`sstables/trie/trie_traversal.hh`), but `abstract_index_reader` has no "previous block" method.

Partition layout in `mx`:

- The order is: key, partition tombstone (always written), static row, clustering rows and range tombstone markers, and a one-byte end marker (`sstables/mx/writer.cc`).
- The writer emits an empty static row whenever the schema has static columns.
  The in-tree document says the opposite (`docs/architecture/sstable/sstable3/sstables-3-data-file-format.rst`).
- A static row has no row timestamp, TTL, or deletion.
- Clustering blocks start after the header and the static row.
- For a partition with a row index, the reader takes the key and the partition tombstone from the index and does not read the header from Data.
- If the schema has static columns, a forward read always reads the header and the static row first, whatever the query asks for (`sstables/mx/reader.cc`).
  A reverse read always reads both.
- A trie lookup can return a false positive.
  The reader checks the key, taken from Data or from the Rows.db header (`sstables/mx/reader.cc`).

Empty values:

- `validation.cc` rejects an empty partition key only for a single-column key, and only on CQL write paths.
- Only `duration` rejects an empty value (`types/types.cc`).
- The tests are in `test/cqlpy/test_empty.py`.

### Stated by the user, not checked in the code

- The row cache always reads all columns.
- Scylla.db stores per-component digests.
- Empty values exist for compatibility, and no sane workload uses them.

### Parquet facts, from memory

- Nulls are stored as definition levels next to the values.
  A run costs a few bytes, and an irregular mix costs one bit per value.
- Data Page V2 stores the levels outside the compressed part.
- With Data Page V2, every page starts at a row boundary.
- Pages of different columns are not aligned unless the writer aligns them.
- A page decodes only forward.
- A dictionary page is the first page of its column chunk, and decoding any value needs the whole dictionary.
- Parquet has one footer per file, with a section per row group.
  It cannot be searched without parsing it from the start.
- Key-value metadata values are strings, so binary data needs base64.
- The list of compression codecs is a fixed enumeration.
- A page is compressed as one unit.
  With `LZ4_RAW` it is a single LZ4 block, and the LZ4 library decodes a block only from one contiguous buffer into another.
  Zstd and gzip have streaming interfaces.
- Delta encoding can be larger than plain for random integers, which is why the writer estimates sizes.
- Plain byte arrays carry a 4-byte length per value, so delta-length is almost always smaller.
- pyarrow cannot write a file with an OffsetIndex and no ColumnIndex, and it does not expose OffsetIndex contents.
  Tests of these need Arrow C++ or our own writer.
