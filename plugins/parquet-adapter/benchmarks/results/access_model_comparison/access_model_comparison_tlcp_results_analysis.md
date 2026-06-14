# Access Model Comparison TLCP Analysis

Source summary:

```text
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_tlcp_summary.md
```

The access model comparison run completed successfully for all systems. Every
query has `5/5` successful measured runs, and the result row counts match across
Polypheny relational, Polypheny document MQL, DuckDB, and Apache Spark.

## Main Findings

DuckDB is fastest for all five access-pattern queries.

Polypheny relational is much faster than Polypheny document MQL on this flat TLC
table. This is expected for a relational dataset because the document path adds
document materialization overhead without a nested-data advantage.

Projection has a clear effect. In Polypheny relational, the full scan Q01 takes
60716.2 ms, while the projection Q02 takes 21946.2 ms. The same pattern appears
for filtered access: Q04 takes 7431.0 ms, while filtered projection Q05 takes
2284.0 ms.

## Interpretation

Polypheny relational behaves best when the query can avoid full row
materialization. It is especially competitive with Spark on filtered count and
filtered projection queries, but DuckDB remains the strongest baseline.

Polypheny document MQL is not a good fit for this flat-table access-model
workload. Its runtimes stay high even for filtered count and projection queries,
which suggests limited benefit from projection and filter pushdown through the
document benchmark path.
