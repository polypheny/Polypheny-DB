# Partitioning Query Specification

| Query | Layout        | Operation                         | Predicate                                                            | Returned data   | Purpose                                                                    |
|-------|---------------|-----------------------------------|----------------------------------------------------------------------|-----------------|----------------------------------------------------------------------------|
| Q01   | Repartitioned | Full table baseline               | None                                                                 | Aggregate count | Measures baseline count over repartitioned `yellow_tripdata`               |
| Q02   | Repartitioned | Partition by year                 | `year = '2022'`                                                      | Aggregate count | Measures year-level partition pruning                                      |
| Q03   | Repartitioned | Partition by month                | `year = '2022'` and `month = '10'`                                   | Aggregate count | Measures month-level partition pruning                                     |
| Q04   | Repartitioned | Filtered count, full scan         | `trip_distance >= 10.0` and `total_amount >= 40.0`                   | Aggregate count | Measures data-column filtering without partition restriction               |
| Q05   | Repartitioned | Filtered count, partition by year | `year = '2022'`, `trip_distance >= 10.0`, and `total_amount >= 40.0` | Aggregate count | Measures combined partition pruning and data-column filtering              |
| Q06   | Unpartitioned | Full table baseline               | None                                                                 | Aggregate count | Measures baseline count over unpartitioned `yellow_tripdata`               |
| Q07   | Unpartitioned | Partition-column filter by year   | `year = '2022'`                                                      | Aggregate count | Measures filtering on physical partition columns in unpartitioned data     |
| Q08   | Unpartitioned | Partition-column filter by month  | `year = '2022'` and `month = '10'`                                   | Aggregate count | Measures filtering on physical year/month columns in unpartitioned data    |
| Q09   | Unpartitioned | Filtered count, full scan         | `trip_distance >= 10.0` and `total_amount >= 40.0`                   | Aggregate count | Measures data-column filtering on unpartitioned data                       |
| Q10   | Unpartitioned | Filtered count with year filter   | `year = '2022'`, `trip_distance >= 10.0`, and `total_amount >= 40.0` | Aggregate count | Measures combined physical year-column filtering and data-column filtering |

Q01-Q03 and Q06-Q08 run on `yellow_tripdata`. Q04-Q05 and Q09-Q10 run on
`green_tripdata`.

Polypheny uses one combined query file because it can reference both adapters in
one SQL file. DuckDB and Spark use separate query files because their runners
bind one `DataDir` per execution.

Runnable query files:

```text
plugins/parquet-adapter/benchmarks/query_lists/partitioning/partitioning_repartitioned_unpartitioned_polypheny.sql
plugins/parquet-adapter/benchmarks/query_lists/partitioning/partitioning_repartitioned_sql.sql
plugins/parquet-adapter/benchmarks/query_lists/partitioning/partitioning_unpartitioned_sql.sql
```
