# Aggregation Query Specification

| Query | Operation                                         | Predicate                                                                                                              | Returned data                                                                   | Purpose                                             |
|-------|---------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|-----------------------------------------------------|
| Q01   | Full Yellow Taxi row count                        | None                                                                                                                   | Aggregate count                                                                 | Measures metadata-like count over `yellow_tripdata` |
| Q02   | Yellow Taxi one-month partition count             | `year = '2022'` and `month = '10'`                                                                                     | Aggregate count                                                                 | Measures count over one partition month             |
| Q03   | Yellow Taxi one-day timestamp count               | `tpep_pickup_datetime >= TIMESTAMP '2022-10-15 00:00:00'` and `tpep_pickup_datetime < TIMESTAMP '2022-10-16 00:00:00'` | Aggregate count                                                                 | Measures count with a physical timestamp predicate  |
| Q04   | Yellow Taxi filtered count                        | `trip_distance >= 10.0` and `total_amount >= 40.0`                                                                     | Aggregate count                                                                 | Measures selective filtering over data columns      |
| Q05   | Yellow Taxi monthly amount and distance summary   | None                                                                                                                   | Monthly count, total amount, minimum distance, maximum distance                 | Measures grouped aggregation over `yellow_tripdata` |
| Q06   | Full High-Volume FHV row count                    | None                                                                                                                   | Aggregate count                                                                 | Measures metadata-like count over `fhvhv_tripdata`  |
| Q07   | High-Volume FHV one-month partition count         | `year = '2022'` and `month = '10'`                                                                                     | Aggregate count                                                                 | Measures count over one partition month             |
| Q08   | High-Volume FHV filtered count                    | `trip_miles >= 10.0` and `base_passenger_fare >= 40.0`                                                                 | Aggregate count                                                                 | Measures selective filtering over data columns      |
| Q09   | High-Volume FHV monthly fare and distance summary | None                                                                                                                   | Monthly count, passenger fare sum, driver pay sum, minimum miles, maximum miles | Measures grouped aggregation over `fhvhv_tripdata`  |
| Q10   | High-Volume FHV shared-ride flag distribution     | `year = '2022'`                                                                                                        | Count by shared-request and shared-match flags                                  | Measures grouped aggregation over categorical flags |

Q01-Q05 run on `yellow_tripdata`. Q06-Q10 run on `fhvhv_tripdata`.

Runnable query files:

```text
plugins/parquet-adapter/benchmarks/query_lists/arrregation/aggregation_polypheny.sql
plugins/parquet-adapter/benchmarks/query_lists/arrregation/aggregation_polypheny_mql.sql
plugins/parquet-adapter/benchmarks/query_lists/arrregation/aggregation_sql.sql
```

## Q1 - Full Yellow Taxi row count

Text:

```text
Count all Yellow Taxi trip records.
```

SQL:

```text
SELECT count(*) AS row_count
FROM yellow_tripdata;
```

## Q2 - Yellow Taxi one-month partition count

Text:

```text
Count Yellow Taxi trip records in partition year 2022 and partition month 10.
```

SQL:

```text
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';
```

## Q3 - Yellow Taxi one-day timestamp count

Text:

```text
Count Yellow Taxi trip records with pickup timestamps on October 15, 2022.
```

SQL:

```text
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE tpep_pickup_datetime >= TIMESTAMP '2022-10-15 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP '2022-10-16 00:00:00';
```

## Q4 - Yellow Taxi filtered count

Text:

```text
Count Yellow Taxi trip records where trip distance is at least 10.0 and total
amount is at least 40.0.
```

SQL:

```text
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;
```

## Q5 - Yellow Taxi monthly amount and distance summary

Text:

```text
For each Yellow Taxi partition year and month, calculate trip count, total gross
amount, minimum trip distance, and maximum trip distance.
```

SQL:

```text
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

## Q6 - Full High-Volume FHV row count

Text:

```text
Count all High-Volume FHV trip records.
```

SQL:

```text
SELECT count(*) AS row_count
FROM fhvhv_tripdata;
```

## Q7 - High-Volume FHV one-month partition count

Text:

```text
Count High-Volume FHV trip records in partition year 2022 and partition month
10.
```

SQL:

```text
SELECT count(*) AS row_count
FROM fhvhv_tripdata
WHERE "year" = '2022'
  AND "month" = '10';
```

## Q8 - High-Volume FHV filtered count

Text:

```text
Count High-Volume FHV trip records where trip miles are at least 10.0 and base
passenger fare is at least 40.0.
```

SQL:

```text
SELECT count(*) AS row_count
FROM fhvhv_tripdata
WHERE trip_miles >= 10.0
  AND base_passenger_fare >= 40.0;
```

## Q9 - High-Volume FHV monthly fare and distance summary

Text:

```text
For each High-Volume FHV partition year and month, calculate trip count, total
passenger fare, total driver pay, minimum trip miles, and maximum trip miles.
```

SQL:

```text
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

## Q10 - High-Volume FHV shared-ride flag distribution

Text:

```text
For High-Volume FHV trip records in partition year 2022, count trips by shared
request flag and shared match flag.
```

SQL:

```text
SELECT
  shared_request_flag,
  shared_match_flag,
  count(*) AS trips
FROM fhvhv_tripdata
WHERE "year" = '2022'
GROUP BY shared_request_flag, shared_match_flag
ORDER BY trips DESC;
```
