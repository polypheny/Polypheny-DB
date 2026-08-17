# Partitioning Summary

Phase summarized: `measured`.
Warmup rows are excluded. Mean, median, and standard deviation values use successful runs only.

## Source Files

| System                     | CSV                                                                                                     |
|----------------------------|---------------------------------------------------------------------------------------------------------|
| Polypheny Relational       | `plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_polypheny_results.csv`            |
| DuckDB Repartitioned       | `plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_repartitioned_results.csv` |
| DuckDB Unpartitioned       | `plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_unpartitioned_results.csv` |
| Apache Spark Repartitioned | `plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_repartitioned_results.csv`  |
| Apache Spark Unpartitioned | `plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_unpartitioned_results.csv`  |

## Mean Elapsed Time (ms)

| Query | Description                                     | Polypheny Relational | DuckDB Repartitioned | DuckDB Unpartitioned | Apache Spark Repartitioned | Apache Spark Unpartitioned | Row counts |
|-------|-------------------------------------------------|----------------------|----------------------|----------------------|----------------------------|----------------------------|------------|
| Q1_P  | Repartitioned full table baseline               | 7.2                  | 78.8                 |                      | 1,681.8                    |                            | 1          |
| Q1_NP | Unpartitioned full table baseline               | 9.6                  |                      | 59.6                 |                            | 1,189.6                    | 1          |
| Q2_P  | Repartitioned partition by year                 | 6.6                  | 35.2                 |                      | 641.8                      |                            | 1          |
| Q2_NP | Unpartitioned partition by year                 | 9.6                  |                      | 88.4                 |                            | 2,123.2                    | 1          |
| Q3_P  | Repartitioned partition by month                | 8.6                  | 17.0                 |                      | 450.6                      |                            | 1          |
| Q3_NP | Unpartitioned partition by month                | 9.8                  |                      | 72.2                 |                            | 1,326.2                    | 1          |
| Q4_P  | Repartitioned filtered count, full scan         | 150.0                | 30.2                 |                      | 1,693.0                    |                            | 1          |
| Q4_NP | Unpartitioned filtered count, full scan         | 131.2                |                      | 20.6                 |                            | 902.6                      | 1          |
| Q5_P  | Repartitioned filtered count, partition by year | 54.6                 | 17.4                 |                      | 637.8                      |                            | 1          |
| Q5_NP | Unpartitioned filtered count, partition by year | 61.6                 |                      | 13.6                 |                            | 1,035.4                    | 1          |

## Result Row Counts

| Query | Description                                     | Polypheny Relational | DuckDB Repartitioned | DuckDB Unpartitioned | Apache Spark Repartitioned | Apache Spark Unpartitioned |
|-------|-------------------------------------------------|----------------------|----------------------|----------------------|----------------------------|----------------------------|
| Q1_P  | Repartitioned full table baseline               | 1                    | 1                    |                      | 1                          |                            |
| Q1_NP | Unpartitioned full table baseline               | 1                    |                      | 1                    |                            | 1                          |
| Q2_P  | Repartitioned partition by year                 | 1                    | 1                    |                      | 1                          |                            |
| Q2_NP | Unpartitioned partition by year                 | 1                    |                      | 1                    |                            | 1                          |
| Q3_P  | Repartitioned partition by month                | 1                    | 1                    |                      | 1                          |                            |
| Q3_NP | Unpartitioned partition by month                | 1                    |                      | 1                    |                            | 1                          |
| Q4_P  | Repartitioned filtered count, full scan         | 1                    | 1                    |                      | 1                          |                            |
| Q4_NP | Unpartitioned filtered count, full scan         | 1                    |                      | 1                    |                            | 1                          |
| Q5_P  | Repartitioned filtered count, partition by year | 1                    | 1                    |                      | 1                          |                            |
| Q5_NP | Unpartitioned filtered count, partition by year | 1                    |                      | 1                    |                            | 1                          |

## Detailed Summary (ms)

### Q1

| System                     | Query | Description                       | Runs | Mean    | Median  | Std Dev | Min     | Max     | Rows | Columns | Status |
|----------------------------|-------|-----------------------------------|------|---------|---------|---------|---------|---------|------|---------|--------|
| Polypheny Relational       | Q1_P  | Repartitioned full table baseline | 5/5  | 7.2     | 7.0     | 0.8     | 6.0     | 8.0     | 1    | 1       | ok     |
| Polypheny Relational       | Q1_NP | Unpartitioned full table baseline | 5/5  | 9.6     | 10.0    | 1.1     | 8.0     | 11.0    | 1    | 1       | ok     |
| DuckDB Repartitioned       | Q1_P  | Repartitioned full table baseline | 5/5  | 78.8    | 75.0    | 11.3    | 65.0    | 95.0    | 1    | 1       | ok     |
| DuckDB Unpartitioned       | Q1_NP | Unpartitioned full table baseline | 5/5  | 59.6    | 59.0    | 2.4     | 57.0    | 63.0    | 1    | 1       | ok     |
| Apache Spark Repartitioned | Q1_P  | Repartitioned full table baseline | 5/5  | 1,681.8 | 1,589.0 | 204.8   | 1,530.0 | 2,037.0 | 1    | 1       | ok     |
| Apache Spark Unpartitioned | Q1_NP | Unpartitioned full table baseline | 5/5  | 1,189.6 | 1,217.0 | 190.4   | 973.0   | 1,418.0 | 1    | 1       | ok     |

### Q2

| System                     | Query | Description                     | Runs | Mean    | Median  | Std Dev | Min     | Max     | Rows | Columns | Status |
|----------------------------|-------|---------------------------------|------|---------|---------|---------|---------|---------|------|---------|--------|
| Polypheny Relational       | Q2_P  | Repartitioned partition by year | 5/5  | 6.6     | 6.0     | 0.9     | 6.0     | 8.0     | 1    | 1       | ok     |
| Polypheny Relational       | Q2_NP | Unpartitioned partition by year | 5/5  | 9.6     | 9.0     | 1.3     | 8.0     | 11.0    | 1    | 1       | ok     |
| DuckDB Repartitioned       | Q2_P  | Repartitioned partition by year | 5/5  | 35.2    | 35.0    | 1.8     | 33.0    | 38.0    | 1    | 1       | ok     |
| DuckDB Unpartitioned       | Q2_NP | Unpartitioned partition by year | 5/5  | 88.4    | 88.0    | 5.4     | 81.0    | 95.0    | 1    | 1       | ok     |
| Apache Spark Repartitioned | Q2_P  | Repartitioned partition by year | 5/5  | 641.8   | 592.0   | 81.3    | 582.0   | 767.0   | 1    | 1       | ok     |
| Apache Spark Unpartitioned | Q2_NP | Unpartitioned partition by year | 5/5  | 2,123.2 | 2,115.0 | 94.1    | 2,015.0 | 2,273.0 | 1    | 1       | ok     |

### Q3

| System                     | Query | Description                      | Runs | Mean    | Median  | Std Dev | Min     | Max     | Rows | Columns | Status |
|----------------------------|-------|----------------------------------|------|---------|---------|---------|---------|---------|------|---------|--------|
| Polypheny Relational       | Q3_P  | Repartitioned partition by month | 5/5  | 8.6     | 9.0     | 1.1     | 7.0     | 10.0    | 1    | 1       | ok     |
| Polypheny Relational       | Q3_NP | Unpartitioned partition by month | 5/5  | 9.8     | 10.0    | 1.9     | 7.0     | 12.0    | 1    | 1       | ok     |
| DuckDB Repartitioned       | Q3_P  | Repartitioned partition by month | 5/5  | 17.0    | 16.0    | 2.0     | 15.0    | 20.0    | 1    | 1       | ok     |
| DuckDB Unpartitioned       | Q3_NP | Unpartitioned partition by month | 5/5  | 72.2    | 71.0    | 5.6     | 65.0    | 80.0    | 1    | 1       | ok     |
| Apache Spark Repartitioned | Q3_P  | Repartitioned partition by month | 5/5  | 450.6   | 443.0   | 80.2    | 355.0   | 563.0   | 1    | 1       | ok     |
| Apache Spark Unpartitioned | Q3_NP | Unpartitioned partition by month | 5/5  | 1,326.2 | 1,343.0 | 101.3   | 1,193.0 | 1,465.0 | 1    | 1       | ok     |

### Q4

| System                     | Query | Description                             | Runs | Mean    | Median  | Std Dev | Min     | Max     | Rows | Columns | Status |
|----------------------------|-------|-----------------------------------------|------|---------|---------|---------|---------|---------|------|---------|--------|
| Polypheny Relational       | Q4_P  | Repartitioned filtered count, full scan | 5/5  | 150.0   | 155.0   | 14.6    | 133.0   | 168.0   | 1    | 1       | ok     |
| Polypheny Relational       | Q4_NP | Unpartitioned filtered count, full scan | 5/5  | 131.2   | 131.0   | 14.9    | 110.0   | 152.0   | 1    | 1       | ok     |
| DuckDB Repartitioned       | Q4_P  | Repartitioned filtered count, full scan | 5/5  | 30.2    | 30.0    | 1.3     | 29.0    | 32.0    | 1    | 1       | ok     |
| DuckDB Unpartitioned       | Q4_NP | Unpartitioned filtered count, full scan | 5/5  | 20.6    | 20.0    | 1.3     | 19.0    | 22.0    | 1    | 1       | ok     |
| Apache Spark Repartitioned | Q4_P  | Repartitioned filtered count, full scan | 5/5  | 1,693.0 | 1,715.0 | 144.3   | 1,501.0 | 1,873.0 | 1    | 1       | ok     |
| Apache Spark Unpartitioned | Q4_NP | Unpartitioned filtered count, full scan | 5/5  | 902.6   | 871.0   | 145.2   | 780.0   | 1,153.0 | 1    | 1       | ok     |

### Q5

| System                     | Query | Description                                     | Runs | Mean    | Median  | Std Dev | Min   | Max     | Rows | Columns | Status |
|----------------------------|-------|-------------------------------------------------|------|---------|---------|---------|-------|---------|------|---------|--------|
| Polypheny Relational       | Q5_P  | Repartitioned filtered count, partition by year | 5/5  | 54.6    | 53.0    | 3.0     | 53.0  | 60.0    | 1    | 1       | ok     |
| Polypheny Relational       | Q5_NP | Unpartitioned filtered count, partition by year | 5/5  | 61.6    | 59.0    | 6.0     | 56.0  | 71.0    | 1    | 1       | ok     |
| DuckDB Repartitioned       | Q5_P  | Repartitioned filtered count, partition by year | 5/5  | 17.4    | 17.0    | 1.1     | 16.0  | 19.0    | 1    | 1       | ok     |
| DuckDB Unpartitioned       | Q5_NP | Unpartitioned filtered count, partition by year | 5/5  | 13.6    | 14.0    | 0.5     | 13.0  | 14.0    | 1    | 1       | ok     |
| Apache Spark Repartitioned | Q5_P  | Repartitioned filtered count, partition by year | 5/5  | 637.8   | 681.0   | 101.5   | 502.0 | 736.0   | 1    | 1       | ok     |
| Apache Spark Unpartitioned | Q5_NP | Unpartitioned filtered count, partition by year | 5/5  | 1,035.4 | 1,001.0 | 101.7   | 955.0 | 1,197.0 | 1    | 1       | ok     |

