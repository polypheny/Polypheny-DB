# Access Model Comparison - Benchmark Analysis

## Results Plot
![access_model_comparison_tlcp_plot.png](../plots/access_model_comparison_tlcp_plot.png)

## Interpretation

### General
- The access model comparison run completed successfully for all systems. 
- Every query has `5/5` successful measured runs
- Result row counts match across Polypheny relational flat, Polypheny relational normalized, Polypheny document
MQL, DuckDB, and Apache Spark.
- Standard deviations remain within 12.4% of the corresponding mean runtimes, indicating generally stable repeated measurements.


### Main Findings

- DuckDB is fastest for all five access-pattern queries.
- Polypheny relational Flat and normalized mode are very close, which is logical because only root tables accessed.

- Normalized mode is slightly slower for full-record access because it adds and materializes a synthetic row identifier for every record. This produces one additional result column and requires a binding-aware row-extraction path. When the synthetic identifier is not returned, flat and normalized performance is broadly comparable.

- MQL (Polypheny Document) is slower than Polypheny relational because of document materialization.

- Projection consistently improves performance across all tested systems. In both the unfiltered case (Q02 compared with Q01) and the filtered case (Q05 compared with Q04), queries that return only selected fields execute faster than queries that return full records. This indicates that reducing the number of retrieved columns lowers the amount of data that must be read, transferred, and materialized, which has a measurable positive effect for relational, document, DuckDB, and Spark-based access paths.


## Related Documents

Summary:

```text
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_tlcp_summary.md
```

Plot:
```text
plugins/parquet-adapter/benchmarks/results/plots/access_model_comparison_tlcp_plot.pdf
plugins/parquet-adapter/benchmarks/results/plots/access_model_comparison_tlcp_plot.png
plugins/parquet-adapter/benchmarks/results/plots/access_model_comparison_tlcp_plot.svg
```
