# Aggregation - Benchmark Analysis

## Results Plot
![aggregation_plot.png](../plots/aggregation_plot.png)

## Interpretation

### General
- The aggregation run completed successfully for all evaluated systems. 
- All 10 queries completed successfully, with 5/5 successful measured runs per query for every system. 
- The result row counts match across Polypheny relational, Polypheny document MQL, DuckDB, and Apache Spark.
- Standard deviations are generally modest; the largest relative variation occurs in the very short metadata queries and Polypheny Relational Q03, while longer data-reading workloads are more stable.

### Correctness
Correctness checks confirm consistent aggregation results across systems: all row counts match, while Polypheny relational, DuckDB, and Apache Spark also produce matching grouping keys and COUNT, SUM, MIN, and MAX results within the configured numeric tolerance. Polypheny document MQL produces the same available aggregate values and groups, but Q05, Q09, and Q10 have intentionally different result schemas because the MQL variants omit some count fields and represent grouping keys through _id. These are output-shape differences rather than conflicting aggregation results.

### Main Findings

- Polypheny relational is strongest overall on metadata-oriented count queries: Q01, Q02, Q06, and Q07. It completes these metadata-optimized queries in approximately 10-14 ms, leading Q01, Q06, and Q07 and closely matching DuckDB on Q02. 
- The document adapter also benefits from the metadata-optimized count path, although additional document-processing overhead results in higher runtimes than the relational path.

- DuckDB is fastest for most data-reading aggregation queries (filtering, grouping, data aggregation). It has the best
runtime for Q03, Q04, Q05, Q08, Q09, and Q10. The gap is especially visible for
Q10, where DuckDB evaluates the shared-request grouping much faster than the
other systems.

- The Polypheny document path is competitive with, and sometimes faster than, the
relational path for large data-reading aggregation queries. This is visible for
Q03, Q08, Q09, and Q10. These results show that pushing aggregation into the
Parquet-backed execution path also benefits document workloads when the
aggregate can be expressed over primitive Parquet fields.

- Spark performs worst overall, recording the slowest runtime in eight of ten aggregation queries. 
Its task scheduling, file discovery, code generation, and shuffle overhead are costly in this single-machine benchmark, 
especially for short metadata-oriented queries. Spark becomes more competitive on the large Q08 scan and nearly matches 
Polypheny Relational on Q10, but remains consistently slower than DuckDB.

- The benchmark demonstrates that the Parquet adapter's metadata-based count optimization is highly effective for count queries. 
For general analytical aggregation over Parquet on a single machine, DuckDB remains the strongest reference system, 
while the optimized Polypheny document execution path delivers competitive performance for several aggregation workloads.


## Related Documents

Summary:

```text
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_summary.md


```

Source correctness comparison:

```text
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_correctness_summary.md
```
