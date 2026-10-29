# Splice disjoint ICS runs after tablet merge

After a tablet merge, the merged compaction group holds the sstable runs
of both sibling tablets. The runs cover disjoint token ranges, and runs
of similar size are likely on both sides. Incremental compaction strategy
(ICS) puts runs of similar size into the same size bucket and compacts
them together. For disjoint runs, that rewrites the data without merging
anything. With `min_threshold` set to 2, a merge can set off an avalanche
of such compactions.

This series splices such runs into one run instead. A run is a set of
disjoint sstables that share a run identifier. Splicing gives the
sstables of several disjoint runs the same run identifier, so ICS treats
them as a single run.

Splicing is kept in memory only. Writing a new run identifier to disk
needs a component rewrite, which changes the identity of the sstable,
and backup deduplication relies on that identity. So splicing is redone
on boot.

Sstables loaded in other ways, for example by tablet migration or
`nodetool refresh`, are not spliced. A merged tablet that migrates
therefore arrives with its sibling runs unspliced. That is left for a
follow-up.

Fixes: SCYLLADB-3584

## Changes

- `sstables`: exposes the overlap check between two sstables as
  `completely_ordered_before()`, and adds
  `sstable_last_position_tri_compare()` and
  `sstable::set_run_identifier()`.
  `link_with_rewritten_component()` now carries the in-memory run
  identifier over to the rewritten sstable, so that component rewrites
  (for example, by incremental repair) don't undo splicing.
- `compaction`: adds `compaction_strategy::get_runs_to_splice()`. Only
  ICS implements it. It splices disjoint runs within the same size
  bucket and leaves runs of different buckets alone. Within a bucket,
  it partitions the runs into as few chains of disjoint runs as
  possible, in O(n log n) time. The result doesn't depend on the input
  order, so it is the same on every boot.
  `compaction_strategy::may_splice_runs()` lets callers skip gathering
  runs for strategies that never splice.
- `replica`: adds `compaction_group::splice_sstable_runs()`. It picks
  runs per view (unrepaired, repairing, repaired), skips runs involved
  in an ongoing compaction, and leaves alone runs with sstables in more
  than one view. It rebuilds the main sstable set under the sstable list
  permit. Failures are logged and ignored, since splicing is only an
  optimization.
- `replica`: calls it when the merge completion fiber has merged the
  groups, while compaction is still disabled, and when a table is
  populated on boot, before compaction is enabled.

## Tests

- `ics_runs_to_splice_test` and `ics_splice_sstable_runs_test` in
  `test/boost/incremental_compaction_test.cc` cover run selection and
  splicing in a compaction group.
- `ics_splice_sstable_runs_across_views_test` covers runs whose sstables
  are in different views.
- `test_link_with_rewritten_component_preserves_run_identifier` in
  `test/boost/sstable_test.cc` covers component rewrites.
- `test_ics_runs_are_spliced_after_merge` in
  `test/cluster/test_tablets_merge.py` merges two tablets with one run
  each and checks that no compactions are pending, both after the merge
  and after a restart.
