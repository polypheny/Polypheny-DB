# Partition Test Query List

Currently Polypheny does not support hierarchical partitioning

## Test data setup

These tests use the TLC Parquet data under `C:\PolyData\tlc`. The full partition test setup expects this directory layout:

```text
C:\PolyData\tlc\
  yellow_tripdata\
    year=2025\
      month=01\
        yellow_tripdata_2025-01.parquet
  green_tripdata\
    year=2025\
      month=01\
        green_tripdata_2025-01.parquet
  fhv_tripdata\
    year=2025\
      month=01\
        fhv_tripdata_2025-01.parquet
  fhvhv_tripdata\
    year=2025\
      month=01\
        fhvhv_tripdata_2025-01.parquet
```

The SQL examples below assume that the Parquet adapter was loaded with the adapter name `prn`, so the expected tables in the `public` schema are:

- `prn__yellow_tripdata`
- `prn__green_tripdata`
- `prn__fhv_tripdata`
- `prn__fhvhv_tripdata`

If the adapter is created with another name, replace the `prn__` prefix in the queries with that adapter prefix.

### The adapter source location can be provided in four supported forms:

1. One Parquet file, for example `C:\PolyData\tlc\green_tripdata\year=2025\month=01\green_tripdata_2025-01.parquet`. 
This creates one table backed by one file.


2. A folder with several Parquet files and no `key=value` folders. 
This creates one logical table backed by multiple files.


3. A partitioned table folder, for example `C:\PolyData\tlc\green_tripdata`. 
This creates one logical table with partition columns from folders such as `year=2025` and `month=01`.


4. A dataset root with several table folders, for example `C:\PolyData\tlc`. 
This creates one logical table per child folder, such as `yellow_tripdata`, `green_tripdata`, `fhv_tripdata`, and `fhvhv_tripdata`.


For the full test plan, use option 4 (`C:\PolyData\tlc`) so all TLC partitioned tables are available together. 
The partition columns `year` and `month` should be visible as normal table columns; 
only the first partition column, `year`, is used for **Polypheny physical partitioning**.

## 1. Simple select by year and month, check that these columns were created

```sql
SELECT *
FROM "prn__green_tripdata"
WHERE "year" = '2025'
AND "month" = '01';
```

## 2. Single partition scan on `prn__green_tripdata`, group by year

```sql
SELECT
"year",
COUNT(*) AS trip_count
FROM "prn__green_tripdata"
WHERE "year" = '2025'
GROUP BY "year";
```

## 3. Simple select by year and month - Polypheny currently does not support logical operators

```sql
SELECT * from prn__yellow_tripdata WHERE "year"='2025' AND "month"='01' LIMIT 5
SELECT * from prn__green_tripdata WHERE "year"='2025' AND "month"='01' LIMIT 5
SELECT * from prn__fhv_tripdata WHERE "year"='2025' AND "month"='01' LIMIT 5
SELECT * from prn__fhvhv_tripdata WHERE "year"='2025' AND "month"='01' LIMIT 5
```

## 4. Single partition scan on `prn__green_tripdata` - NOT WORKING

Purpose: verify that Hive-style partition columns are visible and that a filter on both partition columns can prune to one `year=2025/month=01` Parquet file.

```sql
SELECT
    "year",
    "month",
    COUNT(*) AS trip_count
FROM "prn__green_tripdata"
WHERE "year" = '2025'
GROUP BY "year", "month";
```
