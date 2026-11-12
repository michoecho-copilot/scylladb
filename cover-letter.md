After a tablet merge, the merged compaction group holds the sstable runs
of both sibling tablets. The runs cover disjoint token ranges, and runs
of similar size are likely on both sides. Incremental compaction strategy
(ICS) puts runs of similar size into the same size bucket and compacts
them together. For disjoint runs, that just rewrites data without achieving
anything useful. With `min_threshold` set to 2, a merge can set off an avalanche
of such compactions.

This series makes such a compaction a splice. A run is a set of disjoint
sstables that share a run identifier. Splicing gives the sstables of
several disjoint runs the same run identifier, so ICS treats them as a
single run from then on.

The splice is implemented as "fast path" of regular compaction.
When ICS picks the runs of a bucket to compact, it looks for runs among
them which don't overlap one another. If there are at least two, it marks
the job as a splice of those runs, and leaves the rest of the bucket for
the next job. So if each sibling tablet holds two overlapping runs of one
tier, the first job splices one run from each sibling, and the next job
splices the other two.
The regular compaction task doesn't run such a job as a compaction.
It only changes run identifiers, and then asks ICS for the next job.
The sstables of the input runs get a new run identifier, the way the output
of a compaction does.

The run identifier is changed in memory only. Nothing is read or written,
and the sstables keep their identity. So a splice costs the same on local
and on object storage, and backups, snapshots and tablet migrations see the
sstables as they were. The splice doesn't survive a restart or a tablet
migration: the sstables are loaded with the run identifiers stored on disk,
and ICS splices them again when it picks their bucket.

Fixes: SCYLLADB-3584

Optimization, no backports needed.
