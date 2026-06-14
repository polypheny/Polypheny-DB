# Aggregation Benchmark Analysis

Source summary:

```text
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_summary.md
```

The aggregation run completed successfully for all three systems. Every query
has `5/5` successful measured runs, and the result row counts match across
Polypheny, DuckDB, and Apache Spark.

## Main Findings

Polypheny is fastest for the simple count queries that can be answered with very
little data materialization


## Interpretation

The strongest Polypheny result is metadata-like counting. Q01, Q02, Q06, and Q07
are all faster in Polypheny than in DuckDB and much faster than Spark. This
suggests that simple count and partition-constrained count queries are currently
a good case for the Polypheny Parquet path.

The main performance gap is on row-level filtering and aggregation over data
columns. DuckDB is about 2.3x to 5.2x faster than Polypheny on Q03-Q05 and
Q08-Q10. The gap is larger on `fhvhv_tripdata` than on `yellow_tripdata`, which
is expected because the high-volume FHV table is larger and the queries touch
more data.

Spark is not competitive on the small/simple count queries because fixed job
overhead dominates. It becomes closer on the larger `fhvhv_tripdata` workloads:
Spark is faster than Polypheny on Q08 and Q10, but DuckDB remains fastest for
both.


