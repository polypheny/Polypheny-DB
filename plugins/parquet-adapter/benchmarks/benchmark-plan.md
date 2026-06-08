# Benchmark Plan

## Relational Schema

We use a set of benchmark queries (query-list.md) to evaluate different functionalities of the Parquet adapter. The workload primarily consists of aggregation queries.

### 1. Flat Partitioned Dataset

The benchmark uses four years of the TLC dataset partitioned by year and month. The year and month values are not stored within the Parquet files and are instead derived from the partition structure.

All benchmark queries are executed on Polypheny, Apache Spark, and DuckDB to establish a baseline. After performance optimizations are implemented, the same queries are executed again to measure their impact.

### 2. Flat Unpartitioned Dataset

The same TLC dataset is used without partitioning. In this configuration, the year and month values are stored as columns within the Parquet files.

All benchmark queries are executed on Polypheny, Apache Spark, and DuckDB to establish a baseline. After performance optimizations are implemented, the same queries are executed again to measure their impact.

### 3. Partitioned vs. Unpartitioned Dataset

The performance of all benchmark queries is compared on Polypheny to evaluate the impact of partitioning.

