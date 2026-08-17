# Aggregation Query Specification

| Query | Operation                                         | Predicate                                                                                                              | Returned data                                                                   | Purpose                                             |
|-------|---------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|-----------------------------------------------------|
| Q01   | Full Yellow Taxi row count                        | None                                                                                                                   | Aggregate count                                                                 | Measures metadata-like count over `yellow_tripdata` |
| Q02   | Yellow Taxi count for one partition month         | `year = '2022'` and `month = '10'`                                                                                     | Aggregate count                                                                 | Measures count over one partition month             |
| Q03   | Yellow Taxi trips on one day                      | `tpep_pickup_datetime >= TIMESTAMP '2022-10-15 00:00:00'` and `tpep_pickup_datetime < TIMESTAMP '2022-10-16 00:00:00'` | Aggregate count                                                                 | Measures count with a physical timestamp predicate  |
| Q04   | Long and expensive Yellow Taxi trips              | `trip_distance >= 10.0` and `total_amount >= 40.0`                                                                     | Aggregate count                                                                 | Measures selective filtering over data columns      |
| Q05   | Yellow Taxi yearly amount and distance summary    | None                                                                                                                   | Yearly total amount, minimum distance, and maximum distance; SQL-based variants also return trip count | Measures grouped aggregation over `yellow_tripdata` |
| Q06   | Full High-Volume FHV row count                    | None                                                                                                                   | Aggregate count                                                                 | Measures metadata-like count over `fhvhv_tripdata`  |
| Q07   | High-Volume FHV count for one partition month     | `year = '2022'` and `month = '10'`                                                                                     | Aggregate count                                                                 | Measures count over one partition month             |
| Q08   | Long and expensive High-Volume FHV trips          | `trip_miles >= 10.0` and `base_passenger_fare >= 40.0`                                                                 | Aggregate count                                                                 | Measures selective filtering over data columns      |
| Q09   | High-Volume FHV yearly fare and distance summary  | None                                                                                                                   | Yearly passenger fare sum, driver pay sum, minimum miles, and maximum miles; SQL-based variants also return trip count | Measures grouped aggregation over `fhvhv_tripdata`  |
| Q10   | High-Volume FHV shared-request flag distribution  | `year = '2022'`                                                                                                        | Shared-request flag groups; SQL-based variants also return trip count by flag   | Measures grouped aggregation over a categorical flag |

Q01-Q05 run on `yellow_tripdata`. Q06-Q10 run on `fhvhv_tripdata`.

Runnable query files:

```text
plugins/parquet-adapter/benchmarks/query_lists/aggregation/aggregation_polypheny.sql
plugins/parquet-adapter/benchmarks/query_lists/aggregation/aggregation_polypheny_mql.sql
plugins/parquet-adapter/benchmarks/query_lists/aggregation/aggregation_sql.sql
```

The SQL-based variants and the Polypheny MQL variant use the same filters and
grouping keys. For Q05, Q09, and Q10, the MQL variant returns fewer aggregate
columns than the SQL-based variants: it omits the trip-count column in Q05 and
Q09, and returns only the grouped shared-request flag values in Q10.

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

## Q2 - Yellow Taxi count for one partition month

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

## Q3 - Yellow Taxi trips on one day

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

## Q4 - Long and expensive Yellow Taxi trips

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

## Q5 - Yellow Taxi yearly amount and distance summary

Text:

```text
For each Yellow Taxi partition year, calculate total gross amount, minimum trip
distance, and maximum trip distance. SQL-based variants also return trip count.
```

SQL:

```text
SELECT
  "year",
  count(*) AS trips,
  sum(total_amount) AS gross_amount,
  min(trip_distance) AS min_distance,
  max(trip_distance) AS max_distance
FROM yellow_tripdata
GROUP BY "year"
ORDER BY "year";
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

## Q7 - High-Volume FHV count for one partition month

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

## Q8 - Long and expensive High-Volume FHV trips

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

## Q9 - High-Volume FHV yearly fare and distance summary

Text:

```text
For each High-Volume FHV partition year, calculate total passenger fare, total
driver pay, minimum trip miles, and maximum trip miles. SQL-based variants also
return trip count.
```

SQL:

```text
SELECT
  "year",
  count(*) AS trips,
  sum(base_passenger_fare) AS passenger_fare,
  sum(driver_pay) AS driver_pay,
  min(trip_miles) AS min_miles,
  max(trip_miles) AS max_miles
FROM fhvhv_tripdata
GROUP BY "year"
ORDER BY "year";
```

## Q10 - High-Volume FHV shared-request flag distribution

Text:

```text
For High-Volume FHV trip records in partition year 2022, group records by shared
request flag. SQL-based variants also return the trip count per flag.
```

SQL:

```text
SELECT
  shared_request_flag,
  count(*) AS trips
FROM fhvhv_tripdata
WHERE "year" = '2022'
GROUP BY shared_request_flag
ORDER BY trips DESC;
```
