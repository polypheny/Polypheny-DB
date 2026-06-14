# Access Model Comparison TLCP Summary

Phase summarized: `measured`.
Warmup rows are excluded. Mean and median values use successful runs only.

## Source Files

| System | CSV |
| --- | --- |
| Polypheny Relational | `plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_rf_tlcp_results.csv` |
| Polypheny Document MQL | `plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_mql_tlcp_results.csv` |
| DuckDB | `plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_duckdb_tlcp_results.csv` |
| Apache Spark | `plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_spark_tlcp_results.csv` |

## Mean Elapsed Time (ms)

| Query | Description                 | Polypheny Relational | Polypheny Document MQL | DuckDB   | Apache Spark | Row counts |
|-------|-----------------------------|----------------------|------------------------|----------|--------------|------------|
| Q01   | Full record access          | 60,716.2             | 151,038.0              | 10,546.8 | 25,591.0     | 3,711,544  |
| Q02   | Projection                  | 21,946.2             | 97,852.6               | 635.6    | 16,701.8     | 3,711,544  |
| Q03   | Filtered count              | 358.0                | 74,869.6               | 107.8    | 1,506.6      | 1          |
| Q04   | Filtered full record access | 7,431.0              | 79,011.6               | 1,433.8  | 3,244.8      | 283,006    |
| Q05   | Filtered projection         | 2,284.0              | 76,674.0               | 253.6    | 2,661.4      | 283,006    |

## Result Row Counts

| Query | Description                 | Polypheny Relational | Polypheny Document MQL | DuckDB    | Apache Spark |
|-------|-----------------------------|----------------------|------------------------|-----------|--------------|
| Q01   | Full record access          | 3,711,544            | 3,711,544              | 3,711,544 | 3,711,544    |
| Q02   | Projection                  | 3,711,544            | 3,711,544              | 3,711,544 | 3,711,544    |
| Q03   | Filtered count              | 1                    | 1                      | 1         | 1            |
| Q04   | Filtered full record access | 283,006              | 283,006                | 283,006   | 283,006      |
| Q05   | Filtered projection         | 283,006              | 283,006                | 283,006   | 283,006      |

## Detailed Summary (ms)

| System                 | Query | Description                   | Runs | Mean      | Median    | Min       | Max       | Rows      | Columns | Status |
|------------------------|-------|-------------------------------|------|-----------|-----------|-----------|-----------|-----------|---------|--------|
| Polypheny Relational   | Q01   | Full record access            | 5/5  | 60,716.2  | 62,904.0  | 40,161.0  | 77,106.0  | 3,711,544 | 22      | ok     |
| Polypheny Relational   | Q02   | Projection                    | 5/5  | 21,946.2  | 21,219.0  | 19,616.0  | 26,790.0  | 3,711,544 | 5       | ok     |
| Polypheny Relational   | Q03   | Filtered count                | 5/5  | 358.0     | 359.0     | 329.0     | 383.0     | 1         | 1       | ok     |
| Polypheny Relational   | Q04   | Filtered full record access   | 5/5  | 7,431.0   | 6,586.0   | 6,212.0   | 10,030.0  | 283,006   | 22      | ok     |
| Polypheny Relational   | Q05   | Filtered projection           | 5/5  | 2,284.0   | 2,259.0   | 2,199.0   | 2,377.0   | 283,006   | 5       | ok     |
| Polypheny Document MQL | Q01   | Full document access          | 5/5  | 151,038.0 | 129,224.0 | 128,715.0 | 238,912.0 | 3,711,544 | 20      | ok     |
| Polypheny Document MQL | Q02   | Projection                    | 5/5  | 97,852.6  | 97,767.0  | 97,476.0  | 98,171.0  | 3,711,544 | 6       | ok     |
| Polypheny Document MQL | Q03   | Filtered count                | 5/5  | 74,869.6  | 74,482.0  | 74,334.0  | 75,880.0  | 1         | 1       | ok     |
| Polypheny Document MQL | Q04   | Filtered full document access | 5/5  | 79,011.6  | 78,759.0  | 78,538.0  | 79,703.0  | 283,006   | 20      | ok     |
| Polypheny Document MQL | Q05   | Filtered projection           | 5/5  | 76,674.0  | 76,467.0  | 76,437.0  | 77,273.0  | 283,006   | 6       | ok     |
| DuckDB                 | Q01   | Full record access            | 5/5  | 10,546.8  | 10,540.0  | 8,916.0   | 12,826.0  | 3,711,544 | 22      | ok     |
| DuckDB                 | Q02   | Projection                    | 5/5  | 635.6     | 639.0     | 597.0     | 670.0     | 3,711,544 | 5       | ok     |
| DuckDB                 | Q03   | Filtered count                | 5/5  | 107.8     | 101.0     | 90.0      | 141.0     | 1         | 1       | ok     |
| DuckDB                 | Q04   | Filtered full record access   | 5/5  | 1,433.8   | 1,451.0   | 1,252.0   | 1,592.0   | 283,006   | 22      | ok     |
| DuckDB                 | Q05   | Filtered projection           | 5/5  | 253.6     | 239.0     | 217.0     | 296.0     | 283,006   | 5       | ok     |
| Apache Spark           | Q01   | Full record access            | 5/5  | 25,591.0  | 25,364.0  | 24,345.0  | 27,921.0  | 3,711,544 | 22      | ok     |
| Apache Spark           | Q02   | Projection                    | 5/5  | 16,701.8  | 16,530.0  | 15,765.0  | 17,755.0  | 3,711,544 | 5       | ok     |
| Apache Spark           | Q03   | Filtered count                | 5/5  | 1,506.6   | 1,474.0   | 1,363.0   | 1,725.0   | 1         | 1       | ok     |
| Apache Spark           | Q04   | Filtered full record access   | 5/5  | 3,244.8   | 3,169.0   | 3,041.0   | 3,700.0   | 283,006   | 22      | ok     |
| Apache Spark           | Q05   | Filtered projection           | 5/5  | 2,661.4   | 2,612.0   | 2,069.0   | 3,339.0   | 283,006   | 5       | ok     |
