# Benchmark Plan

## Goals

The benchmarks evaluate the Parquet adapter from the following perspectives:

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

Warmup runs are excluded from reported results. Summary tables report mean,
median, sample standard deviation, minimum, and maximum elapsed time over
successful measured runs only. The benchmark clients consume query results
according to the result boundary described in the report methodology. Polypheny
and DuckDB use JDBC clients that drain the complete result set. Spark is
executed in local mode inside Docker and consumes rows in the Spark runner.

Raw benchmark outputs are stored as CSV files. Summary tables are generated with:

```powershell
python plugins\parquet-adapter\benchmarks\scripts\summarize_benchmark_results.py
```

## Datasets

| Dataset                     | Local input                                      | Main purpose                                           |
|-----------------------------|--------------------------------------------------|--------------------------------------------------------|
| NYC TLC partitioned         | `C:\PolyData\tlc_partitioned`                    | Access model and aggregation workloads                 |
| NYC TLC repartitioned       | `C:\PolyData\tlc_repartitioned`                  | Hive-style partitioning and partition-pruning checks   |
| NYC TLC unpartitioned       | `C:\PolyData\tlc_unpartitioned`                  | Physical `year`/`month` column comparison              |
| Nested Customer             | `C:\PolyData\nested_customer\nestedcustomer.parquet` | Deep nested structures and generated normalized tables |

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

Status: executed for the TLC `green_tripdata` table. All five evaluated
systems completed `5/5` measured runs for Q01-Q05, and result row counts matched
across systems.

### Purpose

This suite compares basic access patterns across Polypheny access models and
external Parquet engines. It focuses on flat data access rather than nested
schema generation or partition pruning.

### Compared Systems and Access Paths

- Polypheny relational adapter in flat schema mode;
- Polypheny relational adapter in normalized schema mode;
- Polypheny document adapter queried through MQL;
- DuckDB;
- Apache Spark.

### Dataset

Dataset description:
`plugins/parquet-adapter/benchmarks/datasets/tlc_dataset_desc.md`

Input table: `green_tripdata` from the partitioned TLC dataset.

Polypheny table and collection names used by the current query files:

- relational flat table: `tlcp__green_tripdata`;
- relational normalized table: `tlcpn__green_tripdata`;
- document collection: `tlcpd_document.tlcpd__green_tripdata`;
- DuckDB and Spark view: `green_tripdata`.

### Query Files

| System                           | Query file                                                                                               |
|----------------------------------|----------------------------------------------------------------------------------------------------------|
| Polypheny relational flat        | `plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_rf.sql`  |
| Polypheny relational normalized  | `plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_rn.sql`  |
| Polypheny document MQL           | `plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_mql.mql` |
| DuckDB and Spark                 | `plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_sql.sql` |

### Query Groups

Specification stored in:
```text
plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_query_specification.md
```

### Result Files

Raw and summarized results are stored in:

```text
plugins/parquet-adapter/benchmarks/results/access_model_comparison/
```

Executed raw result files:

```text
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_polypheny_rf_tlcp_results.csv
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_polypheny_rn_tlcpn_results.csv
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_polypheny_mql_tlcp_results.csv
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_duckdb_tlcp_results.csv
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_spark_tlcp_results.csv
```

The current summary and analysis files are:

```text
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_tlcp_summary.md
plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_tlcp_results_analysis.md
```

### Run Pipeline

Runnable commands for this suite are documented in:

```text
plugins/parquet-adapter/benchmarks/run_pipeline/run_access_model_comparison_bm_pipeline.md
```


## Suite 2: Nested Data

Status: executed. Polypheny relational normalized, DuckDB, and Apache Spark
completed `5/5` measured runs for Q01-Q05. Polypheny document MQL completed
Q01-Q02, while Q03-Q05 failed in all measured runs because the nested MQL path
hit runtime index/conversion errors recorded in the summary.

### Purpose

This suite evaluates nested Parquet data, repeated fields, and generated
normalized relational tables.

### Dataset

- Nested Customer for deeply nested customer, order, and line-item structures.
  
Dataset description:
`plugins/parquet-adapter/benchmarks/datasets/nested_customer.md`

Input file: `C:\PolyData\nested_customer\nestedcustomer.parquet`.

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

Executed raw result and summary files:

```text
plugins/parquet-adapter/benchmarks/results/nested_data/nested_data_polypheny_normalized_results.csv
plugins/parquet-adapter/benchmarks/results/nested_data/nested_data_polypheny_mql_results.csv
plugins/parquet-adapter/benchmarks/results/nested_data/nested_data_duckdb_results.csv
plugins/parquet-adapter/benchmarks/results/nested_data/nested_data_spark_results.csv
plugins/parquet-adapter/benchmarks/results/nested_data/nested_data_summary.md
```

### Run Pipeline

Runnable commands for this suite are documented in:

```text
plugins/parquet-adapter/benchmarks/run_pipeline/run_nested_data_bm_pipeline.md
```

## Suite 3: Aggregation and Optimization

Status: executed. Polypheny relational, Polypheny document MQL, DuckDB, and
Apache Spark completed `5/5` measured runs for Q01-Q10. Result row counts match
across systems in the timing summary; the correctness comparison records
remaining MQL grouped-result column-shape differences for Q05, Q09, and Q10.

### Purpose

This suite evaluates analytical aggregations and the effect of Parquet-specific
optimization work introduced during development.

### Dataset

TLC tables from `C:\PolyData\tlc_partitioned`. The aggregation suite runs on
`yellow_tripdata` for Q01-Q05 and `fhvhv_tripdata` for Q06-Q10.

### Query Files

| System                 | Query file                                                                                 |
|------------------------|--------------------------------------------------------------------------------------------|
| Polypheny relational   | `plugins/parquet-adapter/benchmarks/query_lists/aggregation/aggregation_polypheny.sql`     |
| Polypheny document MQL | `plugins/parquet-adapter/benchmarks/query_lists/aggregation/aggregation_polypheny_mql.sql` |
| DuckDB and Spark       | `plugins/parquet-adapter/benchmarks/query_lists/aggregation/aggregation_sql.sql`           |

Human-readable query source:

```text
plugins/parquet-adapter/benchmarks/query_lists/aggregation/aggregation_query_specification.md
```

### Query Groups

Specification stored in:

```text
plugins/parquet-adapter/benchmarks/query_lists/aggregation/aggregation_query_specification.md
```

### Compared Systems and Access Paths

- Polypheny relational adapter;
- Polypheny document adapter queried through MQL;
- DuckDB;
- Apache Spark.

### Query Themes

- full-table counts;
- counts over one partition month;
- selective filtered counts;
- grouped yearly aggregations;
- grouped aggregations over large TLC tables.

### Result Files

Raw and summarized results are stored in:

```text
plugins/parquet-adapter/benchmarks/results/aggregation/
```

Executed raw result files:

```text
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_polypheny_results.csv
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_polypheny_mql_results.csv
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_duckdb_results.csv
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_spark_results.csv
```

Captured result values used for correctness checks:

```text
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_polypheny_values.jsonl
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_polypheny_mql_values.jsonl
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_duckdb_values.jsonl
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_spark_values.jsonl
```

The current summary, correctness summary, and analysis files are:

```text
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_summary.md
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_correctness_summary.md
plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_results_analysis.md
```

### Run Pipeline

Runnable commands for this suite are documented in:

```text
plugins/parquet-adapter/benchmarks/run_pipeline/run_aggregation_bm_pipeline.md
```

## Suite 4: Partitioning

Status: executed. Polypheny relational, DuckDB repartitioned, DuckDB
unpartitioned, Apache Spark repartitioned, and Apache Spark unpartitioned all
completed `5/5` measured runs for their applicable query/layout combinations.

### Purpose

This suite evaluates the effect of Hive-style partitioning and partition pruning
on Parquet workloads.

### Dataset

Partitioned and derived unpartitioned or repartitioned variants of the TLC
dataset:

```text
C:\PolyData\tlc_repartitioned
C:\PolyData\tlc_unpartitioned
```

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

The query identifiers use paired layout suffixes:

- `Q1_P` / `Q1_NP`: full count baseline;
- `Q2_P` / `Q2_NP`: year predicate;
- `Q3_P` / `Q3_NP`: year and month predicate;
- `Q4_P` / `Q4_NP`: data-column filtered count;
- `Q5_P` / `Q5_NP`: year plus data-column filtered count.

### Result Files

Raw and summarized results are stored in:

```text
plugins/parquet-adapter/benchmarks/results/partitioning/
```

Executed raw result files:

```text
plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_polypheny_results.csv
plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_duckdb_repartitioned_results.csv
plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_duckdb_unpartitioned_results.csv
plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_spark_repartitioned_results.csv
plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_spark_unpartitioned_results.csv
```

The current summary and analysis files are:

```text
plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_summary.md
plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_results_analysis.md
```

### Query Themes

- filters on partition columns such as `year` and `month`;
- filters on physical Parquet columns;
- combinations of partition filters and data filters;
- count aggregation over equivalent partitioned and unpartitioned layouts.

### Run Pipeline

Runnable commands for this suite are documented in:

```text
plugins/parquet-adapter/benchmarks/run_pipeline/run_partitioning_bm_pipeline.md
```
