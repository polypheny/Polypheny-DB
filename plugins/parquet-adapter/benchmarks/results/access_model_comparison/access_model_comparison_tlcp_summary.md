# Access Model Comparison TLCP Summary

Phase summarized: `measured`.
Warmup rows are excluded. Mean and median values use successful runs only.

## Source Files

| System                          | CSV                                                                                                                         |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| Polypheny Relational Flat       | `plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_rf_tlcp_results.csv`  |
| Polypheny Relational Normalized | `plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_rn_tlcpn_results.csv` |
| Polypheny Document MQL          | `plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_mql_tlcp_results.csv` |
| DuckDB                          | `plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_duckdb_tlcp_results.csv`        |
| Apache Spark                    | `plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_spark_tlcp_results.csv`         |

## Mean Elapsed Time (ms)

| Query | Description                 | Polypheny Relational Flat | Polypheny Relational Normalized | Polypheny Document MQL | DuckDB  | Apache Spark | Row counts |
|-------|-----------------------------|---------------------------|---------------------------------|------------------------|---------|--------------|------------|
| Q01   | Full record access          | 29,741.8                  | 36,049.6                        | 142,440.8              | 3,672.4 | 21,738.2     | 3,711,544  |
| Q02   | Projection                  | 9,650.6                   | 10,555.8                        | 106,694.4              | 218.8   | 13,667.6     | 3,711,544  |
| Q03   | Filtered count              | 157.4                     | 170.8                           | 424.8                  | 35.4    | 1,182.4      | 1          |
| Q04   | Filtered full record access | 6,073.4                   | 7,594.4                         | 14,752.6               | 480.0   | 2,666.6      | 283,006    |
| Q05   | Filtered projection         | 2,528.8                   | 2,303.0                         | 12,683.2               | 115.2   | 1,941.4      | 283,006    |

## Result Row Counts

| Query | Description                 | Polypheny Relational Flat | Polypheny Relational Normalized | Polypheny Document MQL | DuckDB    | Apache Spark |
|-------|-----------------------------|---------------------------|---------------------------------|------------------------|-----------|--------------|
| Q01   | Full record access          | 3,711,544                 | 3,711,544                       | 3,711,544              | 3,711,544 | 3,711,544    |
| Q02   | Projection                  | 3,711,544                 | 3,711,544                       | 3,711,544              | 3,711,544 | 3,711,544    |
| Q03   | Filtered count              | 1                         | 1                               | 1                      | 1         | 1            |
| Q04   | Filtered full record access | 283,006                   | 283,006                         | 283,006                | 283,006   | 283,006      |
| Q05   | Filtered projection         | 283,006                   | 283,006                         | 283,006                | 283,006   | 283,006      |

## Detailed Summary (ms)

| System                          | Query | Description                   | Runs | Mean      | Median    | Min       | Max       | Rows      | Columns | Status |
|---------------------------------|-------|-------------------------------|------|-----------|-----------|-----------|-----------|-----------|---------|--------|
| Polypheny Relational Flat       | Q01   | Full record access            | 5/5  | 29,741.8  | 29,562.0  | 28,270.0  | 32,563.0  | 3,711,544 | 22      | ok     |
| Polypheny Relational Flat       | Q02   | Projection                    | 5/5  | 9,650.6   | 9,721.0   | 9,208.0   | 10,076.0  | 3,711,544 | 5       | ok     |
| Polypheny Relational Flat       | Q03   | Filtered count                | 5/5  | 157.4     | 153.0     | 147.0     | 180.0     | 1         | 1       | ok     |
| Polypheny Relational Flat       | Q04   | Filtered full record access   | 5/5  | 6,073.4   | 6,083.0   | 5,872.0   | 6,328.0   | 283,006   | 22      | ok     |
| Polypheny Relational Flat       | Q05   | Filtered projection           | 5/5  | 2,528.8   | 2,517.0   | 2,266.0   | 2,702.0   | 283,006   | 5       | ok     |
| Polypheny Relational Normalized | Q01   | Full record access            | 5/5  | 36,049.6  | 35,881.0  | 35,800.0  | 36,433.0  | 3,711,544 | 23      | ok     |
| Polypheny Relational Normalized | Q02   | Projection                    | 5/5  | 10,555.8  | 10,207.0  | 9,367.0   | 12,355.0  | 3,711,544 | 5       | ok     |
| Polypheny Relational Normalized | Q03   | Filtered count                | 5/5  | 170.8     | 174.0     | 153.0     | 185.0     | 1         | 1       | ok     |
| Polypheny Relational Normalized | Q04   | Filtered full record access   | 5/5  | 7,594.4   | 7,509.0   | 7,161.0   | 8,067.0   | 283,006   | 23      | ok     |
| Polypheny Relational Normalized | Q05   | Filtered projection           | 5/5  | 2,303.0   | 2,401.0   | 1,811.0   | 2,543.0   | 283,006   | 5       | ok     |
| Polypheny Document MQL          | Q01   | Full document access          | 5/5  | 142,440.8 | 138,809.0 | 138,395.0 | 156,366.0 | 3,711,544 | 20      | ok     |
| Polypheny Document MQL          | Q02   | Projection                    | 5/5  | 106,694.4 | 99,480.0  | 98,725.0  | 119,637.0 | 3,711,544 | 6       | ok     |
| Polypheny Document MQL          | Q03   | Filtered count                | 5/5  | 424.8     | 421.0     | 396.0     | 462.0     | 1         | 1       | ok     |
| Polypheny Document MQL          | Q04   | Filtered full document access | 5/5  | 14,752.6  | 15,007.0  | 14,130.0  | 15,062.0  | 283,006   | 20      | ok     |
| Polypheny Document MQL          | Q05   | Filtered projection           | 5/5  | 12,683.2  | 12,592.0  | 12,438.0  | 13,119.0  | 283,006   | 6       | ok     |
| DuckDB                          | Q01   | Full record access            | 5/5  | 3,672.4   | 3,820.0   | 3,200.0   | 3,859.0   | 3,711,544 | 22      | ok     |
| DuckDB                          | Q02   | Projection                    | 5/5  | 218.8     | 222.0     | 201.0     | 235.0     | 3,711,544 | 5       | ok     |
| DuckDB                          | Q03   | Filtered count                | 5/5  | 35.4      | 35.0      | 33.0      | 39.0      | 1         | 1       | ok     |
| DuckDB                          | Q04   | Filtered full record access   | 5/5  | 480.0     | 470.0     | 429.0     | 538.0     | 283,006   | 22      | ok     |
| DuckDB                          | Q05   | Filtered projection           | 5/5  | 115.2     | 115.0     | 110.0     | 121.0     | 283,006   | 5       | ok     |
| Apache Spark                    | Q01   | Full record access            | 5/5  | 21,738.2  | 21,838.0  | 21,403.0  | 22,122.0  | 3,711,544 | 22      | ok     |
| Apache Spark                    | Q02   | Projection                    | 5/5  | 13,667.6  | 13,656.0  | 13,338.0  | 14,034.0  | 3,711,544 | 5       | ok     |
| Apache Spark                    | Q03   | Filtered count                | 5/5  | 1,182.4   | 1,204.0   | 1,001.0   | 1,346.0   | 1         | 1       | ok     |
| Apache Spark                    | Q04   | Filtered full record access   | 5/5  | 2,666.6   | 2,645.0   | 2,227.0   | 3,063.0   | 283,006   | 22      | ok     |
| Apache Spark                    | Q05   | Filtered projection           | 5/5  | 1,941.4   | 2,021.0   | 1,626.0   | 2,193.0   | 283,006   | 5       | ok     |
