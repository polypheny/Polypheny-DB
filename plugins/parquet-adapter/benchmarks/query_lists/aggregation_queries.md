# Aggregation Queries

## Q01: Full Yellow Taxi Row Count

Find how many yellow taxi trips are in the dataset.

```sql
SELECT count(*) AS row_count
FROM tlc__yellow_tripdata;
```

## Q02: Yellow Taxi Count For One Partition Month

Find how many yellow taxi trips were in October 2022.

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';
```

## Q03: Yellow Taxi Trips On One Day

Find how many yellow taxi trips started on October 15, 2022.

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE tpep_pickup_datetime >= TIMESTAMP '2022-10-15 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP '2022-10-16 00:00:00';
```

## Q04: Long And Expensive Yellow Taxi Trips

Find how many yellow taxi trips were at least 10 miles long and
cost at least 40 dollars.

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;
```

## Q05: Monthly Yellow Taxi Amount And Distance Summary

Find, for each year and month, how many yellow taxi trips there
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

## Q06: Full High-Volume FHV Row Count

**Description:** Find how many high-volume FHV trips are in the dataset.

```sql
SELECT count(*) AS row_count
FROM fhvhv_tripdata;
```

## Q07: High-Volume FHV Count For One Partition Month

Find how many high-volume FHV trips were in October 2022.

```sql
SELECT count(*) AS row_count
FROM fhvhv_tripdata
WHERE "year" = '2022'
  AND "month" = '10';
```

## Q08: Long And Expensive High-Volume FHV Trips

Find how many high-volume FHV trips were at least 10 miles long
and had a base passenger fare of at least 40 dollars.

```sql
SELECT count(*) AS row_count
FROM fhvhv_tripdata
WHERE trip_miles >= 10.0
  AND base_passenger_fare >= 40.0;
```

## Q09: Monthly High-Volume FHV Fare And Distance Summary

Find, for each year and month, how many high-volume FHV trips
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

## Q10: High-Volume FHV Shared-Ride Flag Distribution

Find how many 2022 high-volume FHV trips fall into each
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
