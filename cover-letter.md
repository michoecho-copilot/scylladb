After a tablet merge, the merged compaction group holds the sstable runs
of both sibling tablets. The runs cover disjoint token ranges, and runs
of similar size are likely on both sides. Incremental compaction strategy
(ICS) puts runs of similar size into the same size bucket and compacts
them together. For disjoint runs, that just rewrites data without achieving
anything useful. With `min_threshold` set to 2, a merge can set off an avalanche
of such compactions.

This series splices such runs into one run instead. A run is a set of
disjoint sstables that share a run identifier. Splicing gives the
sstables of several disjoint runs the same run identifier, so ICS treats
them as a single run. ICS picks the runs to splice: disjoint runs which
fall into the same size bucket. Runs of different buckets are left alone,
so the size structure stays the one the tablets had before the merge.

The splice is done in two steps.

1. The runs are spliced in memory. The sstables get the new run identifier
   in memory only, and a new main sstable set is swapped in. This needs no
   I/O. It's done in the merge completion fiber, while compaction is still
   disabled on the merged group, so ICS never sees the runs unspliced.
   We don't persist anything to disk in this step because that
   can be a slow operation (especially on object storage)
   and the merge completion fiber mustn't be delayed for too long.
   (It holds the old effective replication map,
   and the topology coordinator's barrier waits for it).
2. A background task asynchronously persists the new run identifiers,
   by rewriting the Scylla component of each affected sstable, one sstable at a time.

Persisting the run identifier to disk isn't needed for correctness.
It shouldn't be needed for performance either, because we call the splicing
logic at boot time (before compaction is enabled) anyway.
But it makes the on-disk state of the sstable set eventually consistent
with the in-memory state, which hopefully reduces surprising behaviors.

The background task is modelled on automatic scrub. It stays out of the
way of other operations:
- It processes one sstable at a time and after each one checks if it should pause.
  So operations which disable compaction, like repair, tablet merge,
  split and truncate, wait for one sstable at most.
  The task then puts the view back in its queue and resumes once compaction is enabled again.
- It doesn't wait for the maintenance semaphore, which major compaction
  and cleanup hold for their whole run. Major compaction's selection waits
  for one sstable at most.
- A new per-view semaphore lets regular compaction pick a run between the
  rewrites of two of its sstables, instead of waiting for the rewrite of the whole run.
- Each original is deleted before the next sstable is rewritten. That
  bounds the extra space on object storage, where a component rewrite
  copies all the other components.

The rewrite gives each rewritten sstable a new sstable identifier. That's
required on object storage. So the next backup uploads the rewritten
sstables again, once.
Only sstables whose run identifier changed are rewritten.
When several runs are spliced together, the new run identifier
is inherited from the largest one, so the sstables of the largest run
don't need rewriting.

Tablet migration gains an additional RPC field which carries
the in-memory run identifier. This is so that the effect of splicing
isn't lost when a tablet migrates before its run identifiers are
persisted to disk.

Component rewrites done by incremental repair and automatic scrub now
preserve the run identifier an sstable has in memory.
(Before, they would preserve the one stored on disk, which now would undo a splice).

Sstables loaded in other ways, e.g. by nodetool refresh, aren't spliced on load.

Runs with sstables in more than one view (unrepaired, repairing,
repaired), runs being compacted, and runs with sstables which have no
Scylla component aren't spliced.

Once a view is scheduled, the task writes every sstable of the view whose
run identifier in memory differs from the one on disk. That includes
sstables whose run identifier the sstable set regenerated because of an
overlap, and sstables with a Scylla component which stores no run
identifier.

While the task rewrites an sstable, ICS defers cross-tier and
garbage-collection compactions in the compaction groups of the same
table on that shard, as it does for any running compaction of the table.

Fixes: SCYLLADB-3584

Optimization, no backports needed.
