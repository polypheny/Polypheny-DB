# Access Model Comparison TLCP Analysis

Source summary:

```text
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_tlcp_summary.md
```

The access model comparison run completed successfully for all systems. Every
query has `5/5` successful measured runs, and result row counts now match across
Polypheny relational flat, Polypheny relational normalized, Polypheny document
MQL, DuckDB, and Apache Spark.

## Main Findings

DuckDB is fastest for all five access-pattern queries.

Polypheny relational flat and normalized mode are close for root-table access.
Normalized mode is somewhat slower for full-row reads and filtered full-row
reads, but it does not show a major performance regression for these flat
queries. Filtered projection is slightly faster in normalized mode.

The MQL filter issue is resolved. Q04 and Q05 now return `283006` rows, matching
the SQL systems. MQL remains slower than Polypheny relational for document
materialization, but filtered MQL queries are now much closer to the relational
runtime than in the previous bad run.

## Interpretation

Projection has a strong effect across engines. In Polypheny relational flat,
Q01 takes `29741.8 ms`, while Q02 takes `9650.6 ms`; Q04 takes `6073.4 ms`,
while Q05 takes `2528.8 ms`.

For this flat TLC workload, normalized relational mode does not appear to damage
performance significantly when queries access only root fields. The document
path is still more expensive for large result materialization, but filter
pushdown now behaves consistently with the other systems.
