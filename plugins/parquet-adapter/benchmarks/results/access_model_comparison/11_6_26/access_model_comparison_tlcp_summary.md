# Access Model Comparison TLCP Summary

Phase summarized: `measured`.
Warmup rows are excluded. Mean and median values use successful runs only.

## Source Files

| System                 | CSV                                                                                                 |
|------------------------|-----------------------------------------------------------------------------------------------------|
| Polypheny Relational   | `plugins\parquet-adapter\benchmarks\results\access_model_comparison_polypheny_rf_tlcp_results.csv`  |
| Polypheny Document MQL | `plugins\parquet-adapter\benchmarks\results\access_model_comparison_polypheny_mql_tlcp_results.csv` |
| DuckDB                 | `plugins\parquet-adapter\benchmarks\results\access_model_comparison_duckdb_tlcp_results.csv`        |
| Apache Spark           | `plugins\parquet-adapter\benchmarks\results\access_model_comparison_spark_tlcp_results.csv`         |

## Mean Elapsed Time (ms)

| Query | Description                 | Polypheny Relational | Polypheny Document MQL | DuckDB  | Apache Spark | Row counts |
|-------|-----------------------------|----------------------|------------------------|---------|--------------|------------|
| Q01   | Full record access          | 26,142.4             | 140,778.6              | 2,816.4 | 19,838.4     | 3,711,544  |
| Q02   | Projection                  | 8,800.2              | 108,119.0              | 177.4   | 12,874.4     | 3,711,544  |
| Q03   | Filtered count              | 546.2                | 81,331.0               | 39.0    | 1,059.4      | 1          |
| Q04   | Filtered full record access | 11.6                 | 85,243.8               | 14.6    | 89.2         | differs    |
| Q05   | Filtered projection         | 9.6                  | 83,328.4               | 13.0    | 98.0         | differs    |

## Result Row Counts 

| Query | Description                 | Polypheny Relational | Polypheny Document MQL | DuckDB    | Apache Spark |
|-------|-----------------------------|----------------------|------------------------|-----------|--------------|
| Q01   | Full record access          | 3,711,544            | 3,711,544              | 3,711,544 | 3,711,544    |
| Q02   | Projection                  | 3,711,544            | 3,711,544              | 3,711,544 | 3,711,544    |
| Q03   | Filtered count              | 1                    | 1                      | 1         | 1            |
| Q04   | Filtered full record access | 0                    | 283,006                | 0         | 0            |
| Q05   | Filtered projection         | 0                    | 283,006                | 0         | 0            |

## Detailed Summary (ms)

| System                 | Query | Description                   | Runs | Mean      | Median    | Min       | Max       | Rows      | Columns | Status |
|------------------------|-------|-------------------------------|------|-----------|-----------|-----------|-----------|-----------|---------|--------|
| Polypheny Relational   | Q01   | Full record access            | 5/5  | 26,142.4  | 26,238.0  | 25,808.0  | 26,432.0  | 3,711,544 | 22      | ok     |
| Polypheny Relational   | Q02   | Projection                    | 5/5  | 8,800.2   | 8,836.0   | 8,446.0   | 9,154.0   | 3,711,544 | 5       | ok     |
| Polypheny Relational   | Q03   | Filtered count                | 5/5  | 546.2     | 523.0     | 508.0     | 631.0     | 1         | 1       | ok     |
| Polypheny Relational   | Q04   | Filtered full record access   | 5/5  | 11.6      | 12.0      | 11.0      | 12.0      | 0         | 22      | ok     |
| Polypheny Relational   | Q05   | Filtered projection           | 5/5  | 9.6       | 10.0      | 8.0       | 12.0      | 0         | 5       | ok     |
| Polypheny Document MQL | Q01   | Full document access          | 5/5  | 140,778.6 | 140,681.0 | 139,777.0 | 141,914.0 | 3,711,544 | 20      | ok     |
| Polypheny Document MQL | Q02   | Projection                    | 5/5  | 108,119.0 | 107,420.0 | 106,939.0 | 111,583.0 | 3,711,544 | 6       | ok     |
| Polypheny Document MQL | Q03   | Filtered count                | 5/5  | 81,331.0  | 81,461.0  | 80,273.0  | 82,302.0  | 1         | 1       | ok     |
| Polypheny Document MQL | Q04   | Filtered full document access | 5/5  | 85,243.8  | 85,199.0  | 84,831.0  | 85,709.0  | 283,006   | 20      | ok     |
| Polypheny Document MQL | Q05   | Filtered projection           | 5/5  | 83,328.4  | 83,364.0  | 83,096.0  | 83,598.0  | 283,006   | 6       | ok     |
| DuckDB                 | Q01   | Full record access            | 5/5  | 2,816.4   | 2,786.0   | 2,558.0   | 3,269.0   | 3,711,544 | 22      | ok     |
| DuckDB                 | Q02   | Projection                    | 5/5  | 177.4     | 179.0     | 168.0     | 184.0     | 3,711,544 | 5       | ok     |
| DuckDB                 | Q03   | Filtered count                | 5/5  | 39.0      | 41.0      | 31.0      | 46.0      | 1         | 1       | ok     |
| DuckDB                 | Q04   | Filtered full record access   | 5/5  | 14.6      | 13.0      | 12.0      | 18.0      | 0         | 22      | ok     |
| DuckDB                 | Q05   | Filtered projection           | 5/5  | 13.0      | 12.0      | 12.0      | 16.0      | 0         | 5       | ok     |
| Apache Spark           | Q01   | Full record access            | 5/5  | 19,838.4  | 19,947.0  | 19,449.0  | 20,172.0  | 3,711,544 | 22      | ok     |
| Apache Spark           | Q02   | Projection                    | 5/5  | 12,874.4  | 12,901.0  | 12,161.0  | 13,294.0  | 3,711,544 | 5       | ok     |
| Apache Spark           | Q03   | Filtered count                | 5/5  | 1,059.4   | 1,025.0   | 893.0     | 1,264.0   | 1         | 1       | ok     |
| Apache Spark           | Q04   | Filtered full record access   | 5/5  | 89.2      | 90.0      | 81.0      | 94.0      | 0         | 22      | ok     |
| Apache Spark           | Q05   | Filtered projection           | 5/5  | 98.0      | 96.0      | 82.0      | 114.0     | 0         | 5       | ok     |
