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
it. The sstables manager counts, per table ID and shard, the users of
the storage of the table: the table object, from its constructor until
`table::stop()` has drained its writers, and every sstable object of the
table, until it is destroyed. The count covers every sstable with the
table's schema ID, so it also covers the sstables that the loader holds
in the upload directory. Once the table object is closed and the count
reaches zero, `make_sstable()` throws `no_such_column_family` for the
table, so the count can't go up again until a table object opens the
table. A running table always holds a unit, so it is never refused.

Drop table does the following:

- Once the table is truncated and stopped, it starts a background
  removal and returns. Drop table runs while group0 applies a schema
  change, so it must not wait for references, which can be held for an
  unbounded time.
- The removal waits until the storage has no user on any shard. Then it
  takes a lock, checks again, and removes the directory. The database
  waits for removals on shutdown, before it closes the sstables
  managers. If the node crashes before the removal, the directory is
  left behind, as it already was when a crash happened during drop
  table.
- If the cleanup of the dropped table fails, no removal is started, and
  the storage stays behind, as before.
- `clear_snapshot()` removes the directory of a dropped table through
  the same check, once its last snapshot is gone. It leaves the
  directory if the storage has a user on some shard. That covers a table
  that is dropped from the metadata but not stopped yet, which can still
  flush, and sstables that are still held after a drop whose cleanup
  failed.
- Creating a table with the ID of a dropped table, for example from the
  schema.cql of a snapshot, doesn't wait. Both tables use the same
  directory. The new table object is a user of the storage, so a
  pending removal gives up, and the directory stays with the new table.
  The lock orders the creation of the table with a removal that already
  passed its wait. The schema applier holds it from the creation of the
  directory until the table objects exist on every shard, so the new
  table waits for such a removal to finish. Sstable generations are
  UUIDs, so the files
  of the dropped table's sstables, which stay until those are released,
  can't collide with the files of the new table.

The tests that look at the files of deleted sstables, or at the
directory of a dropped table, now wait until the sstables are destroyed
or the removal completes. The incremental compaction tests move to a
directory that outlives the sstables manager, because the removal of
compacted sstables can now run after the test body ends.

The first commit fixes a bug that predates the series, and the lock
relies on it. A table created by a schema change accepted writes as soon
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
