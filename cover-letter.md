On a local filesystem, an atomic deletion removes the files of its
sstables at once, even while readers still hold those sstables.
Compaction, cleanup, quarantine removal and truncate all delete through
an atomic deletion. Data.db and the index files survive the removal,
because their file descriptors stay open. A component that is opened by
name later does not survive. For example, if compaction deletes an
uncompressed sstable while `validate_checksums_and_digests()` is
checking it, the validation reports that the sstable has no checksums.

This series moves the removal of the files to
`filesystem_storage::destroy()`, which runs when the last reference to
the sstable is dropped. The atomic deletion now only renames the TOC to
a temporary TOC and marks the sstable for deletion. The rename records
the intent durably, because the startup scan removes any sstable with a
temporary TOC, just as the "removing" registry status does on object
storage. The deletion stays atomic, because its log covers the batch
until the renames are synced.

Other `unlink()` callers own their sstables, and the loader and the
streaming consumer rely on the files being gone when the call returns.
Their behaviour does not change. If such a removal fails, the mark
stays, and `destroy()` tries again.

With the removal deferred, two things can no longer go behind the back
of a held sstable:

- `read_digest()` and `read_checksum()` skipped the read for any
  unlinked sstable, because its files could be gone. They now skip only
  if the sstable is unlinked and not marked for deletion.
- Drop table removed the table directory right after truncate. That
  would take the files of a referenced sstable away.

The directory of a dropped table is now removed only when nothing uses
it. A new handle, `sstables::table_storage`, is a reference to the
storage of a table, held on one shard:

- `sstables_manager::open_table_storage()` creates the directory of the
  table if needed, and returns a handle.
- The table object holds a handle from its constructor, which requires
  one, until `table::stop()` has drained its writers.
- `make_sstable()` takes a handle in place of the storage options, and
  the sstable holds it until it is destroyed. So an sstable can't be
  made in a directory that nothing keeps in place. This covers every
  sstable of the table, also the ones that the loader holds in the
  upload directory.
- Code that makes an sstable of a table gets the handle from the table.
  Once the table is stopped, the table gives none out, and the caller
  gets `no_such_column_family`.
- `storage_manager` on shard 0 counts the handles of all shards per
  table ID. Each table ID has its own lock, which is held only while its
  directories are created or removed.

Only local storage is counted. Tools, tests, and code that reads
sstables outside the storage of the table, like a restore from a backup,
make a handle that keeps nothing in place.

Drop table does the following:

- Once the table is truncated and stopped, it marks the storage as
  dropped. If the storage has no user, the directory is removed before
  drop table returns. Otherwise whoever releases the last handle removes
  it, in the background. Drop table runs while group0 applies a schema
  change, so it must not wait for references, which can be held for an
  unbounded time.
- The sstables managers wait for pending releases when they close. If
  the node crashes before the removal, the directory is left behind, as
  it already was when a crash happened during drop table.
- If the cleanup of the dropped table fails, the storage is not marked,
  and it stays behind, as before.
- `clear_snapshot()` removes the directory of a dropped table through
  the same state and lock, once its last snapshot is gone. It leaves the
  directory if the storage has a user on some shard. That covers a table
  that is dropped from the metadata but not stopped yet, which can still
  flush, and sstables that are still held after a drop whose cleanup
  failed.
- Creating a table with the ID of a dropped table, for example from the
  schema.cql of a snapshot, waits only for a removal of the old
  directory that is already running, and then creates the directory
  again. If the old directory is still in use, the new table takes it
  over, and it is not removed. Sstable generations are UUIDs, so the
  files of the dropped table's sstables, which stay until those are
  released, can't collide with the files of the new table.

The removal now covers the directory of the table in every data
directory. Before, only the one in the first data directory was removed.

The tests that look at the files of deleted sstables, or at the
directory of a dropped table, now wait until the sstables are destroyed
or the removal completes. The incremental compaction tests move to a
directory that outlives the sstables manager, because the removal of
compacted sstables can now run after the test body ends.

The first commit fixes a bug that predates the series. A table created by a schema change accepted writes as soon
as `commit()` added it, but its directory was created only in
`post_commit()`. A flush in between failed, and a failed flush aborts the
node. The schema applier now creates the directories in `update()`,
before it adds the tables. That commit stands on its own.

No backport is needed for the rest of the series, because it is an
enhancement.

The hazard of a crash between the removal of the components and of the
temporary TOC predates the series. The atomic deletion already unlinked
the components and the temporary TOC without a sync, and the
pending-deletion log replay only renames a TOC that still exists, so it
never covered components left without any TOC. The series relies on the
same property as 6f037549ac, that unlinks in one directory persist in
order, which journaling filesystems give. A directory fsync per
destroyed sstable would reintroduce the cost that commit removed, for a
case that does not occur in practice.
