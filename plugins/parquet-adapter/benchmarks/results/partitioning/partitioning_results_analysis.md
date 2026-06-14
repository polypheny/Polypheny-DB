# Partitioning Benchmark Analysis

Source summary:

```text
plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_summary.md
```

The partitioning run completed successfully for all systems. Every query has
`5/5` successful measured runs, and all queries returned one result row.

## Main Findings

DuckDB and Spark benefit clearly from the repartitioned layout on partition
predicates. DuckDB improves from 85.8 ms to 63.4 ms for the year filter and from
74.4 ms to 29.8 ms for the month filter. Spark improves from 1978.8 ms to
705.4 ms for the year filter and from 1054.2 ms to 426.4 ms for the month
filter.

Polypheny is fastest for simple count queries in both layouts. The repartitioned
count queries Q01-Q03 run around 16.8-19.6 ms, and the unpartitioned count
queries Q06-Q08 run around 13.2-15.8 ms.

DuckDB is fastest for the filtered count queries over `green_tripdata`. This is
true for both repartitioned and unpartitioned layouts.

## Interpretation

The partitioning effect is most visible for DuckDB and Spark because their
runtime changes substantially when `year` and `month` can be used as partition
columns.

For Polypheny, simple count queries appear metadata-like and are already very
cheap, so partition pruning is not strongly visible there. The stronger signal
is the filtered count pair: repartitioned Q04 takes 279.8 ms, while
partition-constrained Q05 takes 84.6 ms.

Full-table counts are slightly faster on the unpartitioned layout for all
systems, which is expected when no partition pruning is possible and the
partitioned directory layout adds overhead.
