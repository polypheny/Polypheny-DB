# Benchmark Plan

## Goals

The benchmarks evaluate the Parquet adapter from following perspectives:

- correctness of the implemented access paths;
- runtime performance for representative analytical workloads;
- comparison of Polypheny access models;
- comparison with DuckDB and Apache Spark;
- impact of Parquet-specific optimizations.


## Common Execution Setup

Unless a benchmark suite states otherwise, each query is executed with:

- one warmup run;
- five measured runs;
- elapsed wall-clock time recorded in milliseconds;
- result row count and column count recorded for correctness checks;
- failures and timeouts recorded instead of runtime values.

Warmup runs are excluded from reported results. The benchmark clients consume
query results according to the result boundary described in the report
methodology. Polypheny and DuckDB use JDBC clients that drain the complete result
set. Spark is executed in local mode inside Docker and consumes rows in the
Spark runner.

Raw benchmark outputs are stored as CSV files. Summary tables are generated with:

```powershell
python plugins\parquet-adapter\benchmarks\scripts\summarize_benchmark_results.py
```

## Datasets

| Dataset                | Local input                   | Main purpose                                           |
|------------------------|-------------------------------|--------------------------------------------------------|
| NYC TLC Trip Records   | `C:\PolyData\tlc_partitioned` | Flat analytical data and Hive-style partitioning       |
| Nested Customer        | `C:\PolyData\nested_customer` | Deep nested structures and generated normalized tables |

The TLC snapshot contains data from January 2020 through January 2023. This
period is used because newer TLC files introduce schema changes that make a
single consolidated benchmark table less stable. 

## Compared Access Paths and Systems

The benchmark plan includes the following Polypheny access paths:

- Polypheny relational adapter in flat schema mode;
- Polypheny relational adapter in normalized schema mode;
- Polypheny document adapter queried through MQL.

DuckDB and Apache Spark are used as external reference systems for equivalent
Parquet workloads where the same logical operation can be expressed.

## Suite 1: Access Model Comparison

Status: implemented and executed for the TLC `green_tripdata` table.

### Purpose

This suite compares basic access patterns across Polypheny access models and
external Parquet engines. It focuses on flat data access rather than nested
schema generation or partition pruning.

### Compared Systems and Access Paths

- Polypheny relational adapter in flat schema mode;
- Polypheny document adapter queried through MQL;
- DuckDB;
- Apache Spark.

Normalized relational mode is not included in this suite because the selected
`green_tripdata` table is flat and does not produce meaningful generated child
tables.

### Dataset

Dataset description:
`plugins/parquet-adapter/benchmarks/datasets/tlc_dataset_desc.md`

Input table: `green_tripdata` from the partitioned TLC dataset.

Polypheny table and collection names used by the current query files:

- relational table: `tlcp__green_tripdata`;
- document collection: `tlcpd_document.tlcpd__green_tripdata`;
- DuckDB and Spark view: `green_tripdata`.

### Query Files

| System                 | Query file                                                                                               |
|------------------------|----------------------------------------------------------------------------------------------------------|
| Polypheny relational   | `plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_rf.sql`  |
| Polypheny document MQL | `plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_mql.mql` |
| DuckDB and Spark       | `plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_sql.sql` |

### Query Groups

Specification stored in:
```text
plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_query_specification.md
```

### Result Files

Raw and summarized results are stored in:

```text[..](..)
plugins/parquet-adapter/benchmarks/results/access_model_comparison/
```

The current summary file is:

```text
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_tlcp_summary.md
```

### Run Pipeline

Runnable commands for this suite are documented in:

```text
plugins/parquet-adapter/benchmarks/run_pipeline/run_access_model_comparison_bm_pipeline.md
```


## Suite 2: Nested Data

Status: planned.

### Purpose

This suite evaluates nested Parquet data, repeated fields, large binary fields,
and generated normalized relational tables.

### Dataset

- Nested Customer for deeply nested customer, order, and line-item structures.
  
Dataset description:
`plugins/parquet-adapter/benchmarks/datasets/nested_customer.md`

### Compared Systems and Access Paths

- Polypheny relational adapter in normalized mode;
- Polypheny document adapter queried through MQL;
- DuckDB;
- Apache Spark.

### Query Files

| System                                  | Query file                                                                                        |
|-----------------------------------------|---------------------------------------------------------------------------------------------------|
| Polypheny relational normalized adapter | `plugins/parquet-adapter/benchmarks/query_lists/nested_data/nested_data_polypheny_normalized.sql` |
| Polypheny document MQL                  | `plugins/parquet-adapter/benchmarks/query_lists/nested_data/nested_data_mql.mql`                  |
| DuckDB                                  | `plugins/parquet-adapter/benchmarks/query_lists/nested_data/nested_data_duckdb.sql`               |
| Apache Spark                            | `plugins/parquet-adapter/benchmarks/query_lists/nested_data/nested_data_spark.sql`                |


### Query Groups

Specification stored in:
```text
plugins/parquet-adapter/benchmarks/query_lists/nested_data/nested_data_query_specification.md
```

### Result Files

Raw and summarized results are stored in:

```text
plugins/parquet-adapter/benchmarks/results/nested_data/
```

Summary file:

```text
plugins/parquet-adapter/benchmarks/results/nested_data/nested_data_summary.md
```

### Run Pipeline

Runnable commands for this suite are documented in:

```text
plugins/parquet-adapter/benchmarks/run_pipeline/run_nested_data_bm_pipeline.md
```

## Suite 3: Aggregation and Optimization

Status: planned.

### Purpose

This suite evaluates analytical aggregations and the effect of optimization work
introduced during development.

### Dataset

TLC tables from `C:\PolyData\tlc_partitioned`. The aggregation suite runs on
`yellow_tripdata` for Q01-Q05 and `fhvhv_tripdata` for Q06-Q10.

### Query Files

| System                 | Query file                                                                                 |
|------------------------|--------------------------------------------------------------------------------------------|
| Polypheny relational   | `plugins/parquet-adapter/benchmarks/query_lists/arrregation/aggregation_polypheny.sql`     |
| Polypheny document MQL | `plugins/parquet-adapter/benchmarks/query_lists/arrregation/aggregation_polypheny_mql.sql` |
| DuckDB and Spark       | `plugins/parquet-adapter/benchmarks/query_lists/arrregation/aggregation_sql.sql`           |

Human-readable query source:

```text
plugins/parquet-adapter/benchmarks/query_lists/aggregation_queries.md
```

### Query Groups

Specification stored in:

```text
plugins/parquet-adapter/benchmarks/query_lists/arrregation/aggregation_query_specification.md
```

### Compared Systems and Access Paths

- Polypheny relational before and after optimization, where both versions are available;
- Polypheny document adapter queried through MQL;
- DuckDB;
- Apache Spark.

### Query Themes

- full-table counts;
- counts over one partition month;
- selective filtered counts;
- grouped monthly aggregations;
- grouped aggregations over large TLC tables.

### Run Pipeline

Runnable commands for this suite are documented in:

```text
plugins/parquet-adapter/benchmarks/run_pipeline/run_aggregation_bm_pipeline.md
```

## Suite 4: Partitioning

Status: planned.

### Purpose

This suite evaluates the effect of Hive-style partitioning and partition pruning
on Parquet workloads.

### Dataset

Partitioned and derived unpartitioned or repartitioned variants of the TLC
dataset.

### Compared Systems and Access Paths

- Polypheny over partitioned TLC data;
- Polypheny over equivalent unpartitioned TLC data;
- DuckDB and Spark where equivalent partitioned and unpartitioned layouts are
  available.

### Query Files

| System/layout                   | Query file                                                                                                          |
|---------------------------------|---------------------------------------------------------------------------------------------------------------------|
| Polypheny relational            | `plugins/parquet-adapter/benchmarks/query_lists/partitioning/partitioning_repartitioned_unpartitioned_polypheny.sql` |
| DuckDB and Spark, repartitioned | `plugins/parquet-adapter/benchmarks/query_lists/partitioning/partitioning_repartitioned_sql.sql`                    |
| DuckDB and Spark, unpartitioned | `plugins/parquet-adapter/benchmarks/query_lists/partitioning/partitioning_unpartitioned_sql.sql`                    |

### Query Groups

Specification stored in:

```text
plugins/parquet-adapter/benchmarks/query_lists/partitioning/partitioning_query_specification.md
```

### Query Themes

- filters on partition columns such as `year` and `month`;
- filters on physical Parquet columns;
- combinations of partition filters and data filters;
- aggregations grouped by partition columns.

### Run Pipeline

Runnable commands for this suite are documented in:

```text
plugins/parquet-adapter/benchmarks/run_pipeline/run_partitioning_bm_pipeline.md
```
