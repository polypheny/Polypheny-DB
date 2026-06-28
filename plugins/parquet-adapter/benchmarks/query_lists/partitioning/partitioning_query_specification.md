# Partitioning Query Specification

The partitioning benchmark uses five logical count queries. Each query is run
once on the repartitioned layout and once on the unpartitioned layout. The query
identifier suffix indicates the layout:

- `P`: repartitioned, Hive-style partitioned layout
- `NP`: unpartitioned layout with `year` and `month` stored as physical columns

| Query | Layout        | Operation                                      | Predicate                                                            | Returned data   | Purpose                                                               |
|-------|---------------|------------------------------------------------|----------------------------------------------------------------------|-----------------|-----------------------------------------------------------------------|
| Q1_P | Repartitioned | Full table baseline                            | None                                                                 | Aggregate count | Measures baseline count over repartitioned `yellow_tripdata`          |
| Q1_NP | Unpartitioned | Full table baseline                            | None                                                                 | Aggregate count | Measures baseline count over unpartitioned `yellow_tripdata`          |
| Q2_P | Repartitioned | Filter by year                                 | `year = '2022'`                                                      | Aggregate count | Measures year-level partition pruning                                 |
| Q2_NP | Unpartitioned | Physical year-column filter                    | `year = '2022'`                                                      | Aggregate count | Measures filtering on the physical year column                        |
| Q3_P | Repartitioned | Filter by year and month                       | `year = '2022'` and `month = '10'`                                   | Aggregate count | Measures pruning with both partition levels                           |
| Q3_NP | Unpartitioned | Physical year/month-column filter              | `year = '2022'` and `month = '10'`                                   | Aggregate count | Measures filtering on physical year and month columns                 |
| Q4_P | Repartitioned | Filtered count, full scan                      | `trip_distance >= 10.0` and `total_amount >= 40.0`                   | Aggregate count | Measures data-column filtering without partition restriction          |
| Q4_NP | Unpartitioned | Filtered count, full scan                      | `trip_distance >= 10.0` and `total_amount >= 40.0`                   | Aggregate count | Measures data-column filtering on unpartitioned data                  |
| Q5_P | Repartitioned | Filtered count with year partition             | `year = '2022'`, `trip_distance >= 10.0`, and `total_amount >= 40.0` | Aggregate count | Measures combined partition pruning and data-column filtering         |
| Q5_NP | Unpartitioned | Filtered count with physical year column       | `year = '2022'`, `trip_distance >= 10.0`, and `total_amount >= 40.0` | Aggregate count | Measures combined physical-column filtering and data-column filtering |

Q1_P, Q1_NP, Q2_P, Q2_NP, Q3_P, and Q3_NP run on `yellow_tripdata`.
Q4_P, Q4_NP, Q5_P, and Q5_NP run on `green_tripdata`.

Polypheny uses one combined query file because it can reference both adapters in
one SQL file. DuckDB and Spark use separate query files because their runners
bind one `DataDir` per execution.

Runnable query files:

```text
plugins/parquet-adapter/benchmarks/query_lists/partitioning/partitioning_repartitioned_unpartitioned_polypheny.sql
plugins/parquet-adapter/benchmarks/query_lists/partitioning/partitioning_repartitioned_sql.sql
plugins/parquet-adapter/benchmarks/query_lists/partitioning/partitioning_unpartitioned_sql.sql
```

## Q1_P - Repartitioned full table baseline

Text:

```text
Count all Yellow Taxi trip records in the repartitioned dataset.
```

SQL:

```text
SELECT count(*) AS row_count
FROM tlcr__yellow_tripdata;
```

## Q1_NP - Unpartitioned full table baseline

Text:

```text
Count all Yellow Taxi trip records in the unpartitioned dataset.
```

SQL:

```text
SELECT count(*) AS row_count
FROM tlcu__yellow_tripdata;
```

## Q2_P - Repartitioned filter by year

Text:

```text
Count Yellow Taxi trip records in the repartitioned dataset for partition year
2022.
```

SQL:

```text
SELECT count(*) AS row_count
FROM tlcr__yellow_tripdata
WHERE "year" = '2022';
```

## Q2_NP - Unpartitioned physical year-column filter

Text:

```text
Count Yellow Taxi trip records in the unpartitioned dataset where the physical
year column is 2022.
```

SQL:

```text
SELECT count(*) AS row_count
FROM tlcu__yellow_tripdata
WHERE "year" = '2022';
```

## Q3_P - Repartitioned filter by year and month

Text:

```text
Count Yellow Taxi trip records in the repartitioned dataset for partition year
2022 and partition month 10.
```

SQL:

```text
SELECT count(*) AS row_count
FROM tlcr__yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';
```

## Q3_NP - Unpartitioned physical year/month-column filter

Text:

```text
Count Yellow Taxi trip records in the unpartitioned dataset where the physical
year column is 2022 and the physical month column is 10.
```

SQL:

```text
SELECT count(*) AS row_count
FROM tlcu__yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';
```

## Q4_P - Repartitioned filtered count, full scan

Text:

```text
Count Green Taxi trip records in the repartitioned dataset where trip distance
is at least 10.0 and total amount is at least 40.0.
```

SQL:

```text
SELECT count(*) AS row_count
FROM tlcr__green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;
```

## Q4_NP - Unpartitioned filtered count, full scan

Text:

```text
Count Green Taxi trip records in the unpartitioned dataset where trip distance
is at least 10.0 and total amount is at least 40.0.
```

SQL:

```text
SELECT count(*) AS row_count
FROM tlcu__green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;
```

## Q5_P - Repartitioned filtered count with year partition

Text:

```text
Count Green Taxi trip records in the repartitioned dataset for partition year
2022 where trip distance is at least 10.0 and total amount is at least 40.0.
```

SQL:

```text
SELECT count(*) AS row_count
FROM tlcr__green_tripdata
WHERE "year" = '2022'
  AND trip_distance >= 10.0
  AND total_amount >= 40.0;
```

## Q5_NP - Unpartitioned filtered count with physical year column

Text:

```text
Count Green Taxi trip records in the unpartitioned dataset where the physical
year column is 2022, trip distance is at least 10.0, and total amount is at
least 40.0.
```

SQL:

```text
SELECT count(*) AS row_count
FROM tlcu__green_tripdata
WHERE "year" = '2022'
  AND trip_distance >= 10.0
  AND total_amount >= 40.0;
```
