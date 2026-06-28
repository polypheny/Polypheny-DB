# Partitioning Benchmark Analysis

Source summary:

```text
plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_summary.md
```

The partitioning benchmark completed successfully for all evaluated systems.
All measured queries have `5/5` successful runs, and every query returned one
aggregate result row.

## Query Groups

The benchmark uses five logical query groups. Each query is executed on the
repartitioned Hive-style layout and on the equivalent unpartitioned layout:

- `RP`: repartitioned layout with `year` and `month` represented by directory partitions
- `UP`: unpartitioned layout with `year` and `month` stored as physical columns

Q1-Q3 run on `yellow_tripdata`. Q4-Q5 run on `green_tripdata`.

## Mean Runtime Overview

All values are mean measured runtimes in milliseconds.

| Query Pair | Operation | Polypheny | DuckDB | Spark |
| --- | --- | ---: | ---: | ---: |
| Q1_P / Q1_NP | Full count, repartitioned / unpartitioned | 7.2 / 9.6 | 78.8 / 59.6 | 1681.8 / 1189.6 |
| Q2_P / Q2_NP | Year predicate, repartitioned / unpartitioned | 6.6 / 9.6 | 35.2 / 88.4 | 641.8 / 2123.2 |
| Q3_P / Q3_NP | Year and month predicate, repartitioned / unpartitioned | 8.6 / 9.8 | 17.0 / 72.2 | 450.6 / 1326.2 |
| Q4_P / Q4_NP | Data-column filter, repartitioned / unpartitioned | 150.0 / 131.2 | 30.2 / 20.6 | 1693.0 / 902.6 |
| Q5_P / Q5_NP | Year plus data-column filter, repartitioned / unpartitioned | 54.6 / 61.6 | 17.4 / 13.6 | 637.8 / 1035.4 |

## Main Findings

DuckDB and Spark show the clearest benefit from Hive-style partition pruning
when the predicates match the partition layout. DuckDB improves from 78.8 ms
for the repartitioned full-count baseline to 35.2 ms with a year predicate and
17.0 ms with a year-and-month predicate. Spark improves from 1681.8 ms to
641.8 ms and then to 450.6 ms for the same query sequence.

Polypheny is very fast for the simple count queries in both layouts. The
repartitioned simple count queries Q1_P-Q3_P run between 6.6 ms and 8.6 ms,
while the corresponding unpartitioned queries Q1_NP-Q3_NP run between 9.6 ms
and 9.8 ms. The partitioning effect is therefore present but small in absolute
terms for these metadata-friendly count queries.

The clearest Polypheny partitioning signal appears in the filtered-count pair
over `green_tripdata`. Adding the year partition predicate reduces the
repartitioned query from 150.0 ms in Q4_P to 54.6 ms in Q5_P, a 2.75x
improvement. DuckDB improves from 30.2 ms to 17.4 ms, and Spark improves from
1693.0 ms to 637.8 ms for the same repartitioned query pair.

DuckDB is fastest for the data-column filtered count queries. Polypheny is
fastest for the simple count queries, where the counts are computed with very
low overhead.

## Interpretation

Partitioning helps most when the query predicate matches the directory layout.
This is visible in the paired year and year-month comparisons: Q2_P is faster
than Q2_NP for all systems, and Q3_P is faster than Q3_NP for all systems.
The effect is strongest for DuckDB and Spark and smaller for Polypheny because
Polypheny's count queries are already inexpensive.

Partitioning is not universally faster. When no partition predicate is used,
the unpartitioned layout is faster for DuckDB and Spark on the full-count
baseline, while Polypheny is slightly faster on the repartitioned layout. For
the data-column-only filter Q4, the unpartitioned layout is faster for all
systems because no Hive-style partition pruning can be applied.

For Q5, the repartitioned layout benefits from the year partition predicate in
Polypheny and Spark. DuckDB remains faster on the unpartitioned version of this
specific query, which indicates that the benefit of pruning can be outweighed by
layout and execution overhead for small or already selective workloads.
