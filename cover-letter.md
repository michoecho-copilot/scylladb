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
When ICS picks some runs to compact, then if the runs of the
selected bucket don't overlap one another, it returns a job of the new
compaction type `Splice` instead of a regular compaction.
Instead of performing a true merge, this job only changes run identifiers.
The run identifier of the largest input run is chosen as the run identifier
of the output, and sstables of non-largest runs have their run identifier
changed to it, by rewriting the Scylla component with `link_with_rewritten_component()`. 

On object storage, which has no links, the copy is made server-side, and
gets a new sstable identifier, since the objects are named after it. So
a splice costs an S3 copy of the smaller runs, which is still far cheaper
than reading and writing them through the node.

Fixes: SCYLLADB-3584

Optimization, no backports needed.
