# Partitioning Summary

Phase summarized: `measured`.
Warmup rows are excluded. Mean and median values use successful runs only.

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
| Q01   | Repartitioned full table baseline               | 19.0                 | 83.2                 |                      | 1,570.0                    |                            | 1          |
| Q02   | Repartitioned partition by year                 | 16.8                 | 63.4                 |                      | 705.4                      |                            | 1          |
| Q03   | Repartitioned partition by month                | 19.6                 | 29.8                 |                      | 426.4                      |                            | 1          |
| Q04   | Repartitioned filtered count, full scan         | 279.8                | 41.8                 |                      | 1,756.2                    |                            | 1          |
| Q05   | Repartitioned filtered count, partition by year | 84.6                 | 22.6                 |                      | 877.4                      |                            | 1          |
| Q06   | Unpartitioned full table baseline               | 13.2                 |                      | 58.0                 |                            | 1,375.2                    | 1          |
| Q07   | Unpartitioned partition by year                 | 13.8                 |                      | 85.8                 |                            | 1,978.8                    | 1          |
| Q08   | Unpartitioned partition by month                | 15.8                 |                      | 74.4                 |                            | 1,054.2                    | 1          |
| Q09   | Unpartitioned filtered count, full scan         | 260.0                |                      | 21.2                 |                            | 905.6                      | 1          |
| Q10   | Unpartitioned filtered count, partition by year | 80.8                 |                      | 17.6                 |                            | 708.0                      | 1          |

## Result Row Counts

| Query | Description                                     | Polypheny Relational | DuckDB Repartitioned | DuckDB Unpartitioned | Apache Spark Repartitioned | Apache Spark Unpartitioned |
|-------|-------------------------------------------------|----------------------|----------------------|----------------------|----------------------------|----------------------------|
| Q01   | Repartitioned full table baseline               | 1                    | 1                    |                      | 1                          |                            |
| Q02   | Repartitioned partition by year                 | 1                    | 1                    |                      | 1                          |                            |
| Q03   | Repartitioned partition by month                | 1                    | 1                    |                      | 1                          |                            |
| Q04   | Repartitioned filtered count, full scan         | 1                    | 1                    |                      | 1                          |                            |
| Q05   | Repartitioned filtered count, partition by year | 1                    | 1                    |                      | 1                          |                            |
| Q06   | Unpartitioned full table baseline               | 1                    |                      | 1                    |                            | 1                          |
| Q07   | Unpartitioned partition by year                 | 1                    |                      | 1                    |                            | 1                          |
| Q08   | Unpartitioned partition by month                | 1                    |                      | 1                    |                            | 1                          |
| Q09   | Unpartitioned filtered count, full scan         | 1                    |                      | 1                    |                            | 1                          |
| Q10   | Unpartitioned filtered count, partition by year | 1                    |                      | 1                    |                            | 1                          |

## Detailed Summary (ms)

| System                     | Query | Description                                     | Runs | Mean    | Median  | Min     | Max     | Rows | Columns | Status |
|----------------------------|-------|-------------------------------------------------|------|---------|---------|---------|---------|------|---------|--------|
| Polypheny Relational       | Q01   | Repartitioned full table baseline               | 5/5  | 19.0    | 19.0    | 15.0    | 24.0    | 1    | 1       | ok     |
| Polypheny Relational       | Q02   | Repartitioned partition by year                 | 5/5  | 16.8    | 17.0    | 15.0    | 18.0    | 1    | 1       | ok     |
| Polypheny Relational       | Q03   | Repartitioned partition by month                | 5/5  | 19.6    | 20.0    | 17.0    | 21.0    | 1    | 1       | ok     |
| Polypheny Relational       | Q04   | Repartitioned filtered count, full scan         | 5/5  | 279.8   | 284.0   | 256.0   | 308.0   | 1    | 1       | ok     |
| Polypheny Relational       | Q05   | Repartitioned filtered count, partition by year | 5/5  | 84.6    | 85.0    | 77.0    | 92.0    | 1    | 1       | ok     |
| Polypheny Relational       | Q06   | Unpartitioned full table baseline               | 5/5  | 13.2    | 13.0    | 13.0    | 14.0    | 1    | 1       | ok     |
| Polypheny Relational       | Q07   | Unpartitioned partition by year                 | 5/5  | 13.8    | 14.0    | 13.0    | 14.0    | 1    | 1       | ok     |
| Polypheny Relational       | Q08   | Unpartitioned partition by month                | 5/5  | 15.8    | 16.0    | 15.0    | 17.0    | 1    | 1       | ok     |
| Polypheny Relational       | Q09   | Unpartitioned filtered count, full scan         | 5/5  | 260.0   | 262.0   | 228.0   | 291.0   | 1    | 1       | ok     |
| Polypheny Relational       | Q10   | Unpartitioned filtered count, partition by year | 5/5  | 80.8    | 83.0    | 62.0    | 98.0    | 1    | 1       | ok     |
| DuckDB Repartitioned       | Q01   | Repartitioned full table baseline               | 5/5  | 83.2    | 81.0    | 68.0    | 95.0    | 1    | 1       | ok     |
| DuckDB Repartitioned       | Q02   | Repartitioned partition by year                 | 5/5  | 63.4    | 69.0    | 52.0    | 71.0    | 1    | 1       | ok     |
| DuckDB Repartitioned       | Q03   | Repartitioned partition by month                | 5/5  | 29.8    | 30.0    | 27.0    | 33.0    | 1    | 1       | ok     |
| DuckDB Repartitioned       | Q04   | Repartitioned filtered count, full scan         | 5/5  | 41.8    | 37.0    | 33.0    | 58.0    | 1    | 1       | ok     |
| DuckDB Repartitioned       | Q05   | Repartitioned filtered count, partition by year | 5/5  | 22.6    | 25.0    | 16.0    | 27.0    | 1    | 1       | ok     |
| DuckDB Unpartitioned       | Q06   | Unpartitioned full table baseline               | 5/5  | 58.0    | 58.0    | 56.0    | 59.0    | 1    | 1       | ok     |
| DuckDB Unpartitioned       | Q07   | Unpartitioned partition by year                 | 5/5  | 85.8    | 84.0    | 81.0    | 91.0    | 1    | 1       | ok     |
| DuckDB Unpartitioned       | Q08   | Unpartitioned partition by month                | 5/5  | 74.4    | 74.0    | 72.0    | 79.0    | 1    | 1       | ok     |
| DuckDB Unpartitioned       | Q09   | Unpartitioned filtered count, full scan         | 5/5  | 21.2    | 21.0    | 20.0    | 22.0    | 1    | 1       | ok     |
| DuckDB Unpartitioned       | Q10   | Unpartitioned filtered count, partition by year | 5/5  | 17.6    | 18.0    | 16.0    | 19.0    | 1    | 1       | ok     |
| Apache Spark Repartitioned | Q01   | Repartitioned full table baseline               | 5/5  | 1,570.0 | 1,666.0 | 1,219.0 | 1,848.0 | 1    | 1       | ok     |
| Apache Spark Repartitioned | Q02   | Repartitioned partition by year                 | 5/5  | 705.4   | 731.0   | 618.0   | 792.0   | 1    | 1       | ok     |
| Apache Spark Repartitioned | Q03   | Repartitioned partition by month                | 5/5  | 426.4   | 402.0   | 393.0   | 529.0   | 1    | 1       | ok     |
| Apache Spark Repartitioned | Q04   | Repartitioned filtered count, full scan         | 5/5  | 1,756.2 | 1,838.0 | 1,542.0 | 1,913.0 | 1    | 1       | ok     |
| Apache Spark Repartitioned | Q05   | Repartitioned filtered count, partition by year | 5/5  | 877.4   | 723.0   | 557.0   | 1,381.0 | 1    | 1       | ok     |
| Apache Spark Unpartitioned | Q06   | Unpartitioned full table baseline               | 5/5  | 1,375.2 | 1,197.0 | 1,037.0 | 1,878.0 | 1    | 1       | ok     |
| Apache Spark Unpartitioned | Q07   | Unpartitioned partition by year                 | 5/5  | 1,978.8 | 1,898.0 | 1,723.0 | 2,224.0 | 1    | 1       | ok     |
| Apache Spark Unpartitioned | Q08   | Unpartitioned partition by month                | 5/5  | 1,054.2 | 1,021.0 | 950.0   | 1,199.0 | 1    | 1       | ok     |
| Apache Spark Unpartitioned | Q09   | Unpartitioned filtered count, full scan         | 5/5  | 905.6   | 837.0   | 717.0   | 1,152.0 | 1    | 1       | ok     |
| Apache Spark Unpartitioned | Q10   | Unpartitioned filtered count, partition by year | 5/5  | 708.0   | 672.0   | 615.0   | 855.0   | 1    | 1       | ok     |
