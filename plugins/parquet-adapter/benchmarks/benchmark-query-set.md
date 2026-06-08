# Parquet Benchmark Query Set

This file selects 10 queries from the original Q01-Q20 benchmark set for the
current Parquet adapter feature scope. The set covers the supported aggregate
functions: `COUNT`, `SUM`, `MIN`, and `MAX`.

The selected queries avoid functionality that is no longer part of the adapter
benchmark target:

- no average aggregate
- no `WITH` / CTE queries, because these are not supported by the Polypheny JDBC benchmark path
- no aggregate grouping by calculated expressions
- no aggregate arguments produced by a Calc expression

SQL uses the DuckDB table names from the existing benchmark files. The Polypheny
benchmark client maps these names to adapter tables such as
`tlc__yellow_tripdata`, `tlc__green_tripdata`, `tlc__fhvhv_tripdata`, and
`tlc__fhv_tripdata`.

The selected original queries are Q01, Q02, Q04, Q05, Q06, Q09, Q10, Q12, Q13,
and Q15. The Q06 and Q13 versions below keep the original monthly aggregate
shape but replace average distance metrics with minimum and maximum distance
metrics.

## S01 - Original Q01: Full Yellow Taxi Row Count

**Description:** Find how many yellow taxi trips are in the dataset.

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata;
```

## S02 - Original Q02: Yellow Taxi Count For One Partition Month

**Description:** Find how many yellow taxi trips were in October 2022.

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';
```

## S03 - Original Q04: Yellow Taxi Trips On One Day

**Description:** Find how many yellow taxi trips started on October 15, 2022.

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE tpep_pickup_datetime >= TIMESTAMP '2022-10-15 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP '2022-10-16 00:00:00';
```

## S04 - Original Q05: Long And Expensive Yellow Taxi Trips

**Description:** Find how many yellow taxi trips were at least 10 miles long and
cost at least 40 dollars.

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;
```

## S05 - Original Q06 Adjusted: Monthly Yellow Taxi Amount And Distance Summary

**Description:** Find, for each year and month, how many yellow taxi trips there
were, the total amount charged, and the shortest and longest trip distances.

```sql
SELECT
  "year",
  "month",
  count(*) AS trips,
  sum(total_amount) AS gross_amount,
  min(trip_distance) AS min_distance,
  max(trip_distance) AS max_distance
FROM yellow_tripdata
GROUP BY "year", "month"
ORDER BY "year", "month";
```

## S06 - Original Q09: Full High-Volume FHV Row Count

**Description:** Find how many high-volume FHV trips are in the dataset.

```sql
SELECT count(*) AS row_count
FROM fhvhv_tripdata;
```

## S07 - Original Q10: High-Volume FHV Count For One Partition Month

**Description:** Find how many high-volume FHV trips were in October 2022.

```sql
SELECT count(*) AS row_count
FROM fhvhv_tripdata
WHERE "year" = '2022'
  AND "month" = '10';
```

## S08 - Original Q12: Long And Expensive High-Volume FHV Trips

**Description:** Find how many high-volume FHV trips were at least 10 miles long
and had a base passenger fare of at least 40 dollars.

```sql
SELECT count(*) AS row_count
FROM fhvhv_tripdata
WHERE trip_miles >= 10.0
  AND base_passenger_fare >= 40.0;
```

## S09 - Original Q13 Adjusted: Monthly High-Volume FHV Fare And Distance Summary

**Description:** Find, for each year and month, how many high-volume FHV trips
there were, the total passenger fare, the total driver pay, and the shortest and
longest trip distances.

```sql
SELECT
  "year",
  "month",
  count(*) AS trips,
  sum(base_passenger_fare) AS passenger_fare,
  sum(driver_pay) AS driver_pay,
  min(trip_miles) AS min_miles,
  max(trip_miles) AS max_miles
FROM fhvhv_tripdata
GROUP BY "year", "month"
ORDER BY "year", "month";
```

## S10 - Original Q15: High-Volume FHV Shared-Ride Flag Distribution

**Description:** Find how many 2022 high-volume FHV trips fall into each
combination of shared-request and shared-match flags.

```sql
SELECT
  shared_request_flag,
  shared_match_flag,
  count(*) AS trips
FROM fhvhv_tripdata
WHERE "year" = '2022'
GROUP BY shared_request_flag, shared_match_flag
ORDER BY trips DESC;
```
