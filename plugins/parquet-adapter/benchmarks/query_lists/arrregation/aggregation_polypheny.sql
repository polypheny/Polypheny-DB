-- Q01: Full Yellow Taxi Row Count
SELECT count(*) AS row_count
FROM tlcp__yellow_tripdata;

-- Q02: Yellow Taxi Count For One Partition Month
SELECT count(*) AS row_count
FROM tlcp__yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';

-- Q03: Yellow Taxi Trips On One Day
SELECT count(*) AS row_count
FROM tlcp__yellow_tripdata
WHERE tpep_pickup_datetime >= TIMESTAMP '2022-10-15 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP '2022-10-16 00:00:00';

-- Q04: Long And Expensive Yellow Taxi Trips
SELECT count(*) AS row_count
FROM tlcp__yellow_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;

-- Q05: Monthly Yellow Taxi Amount And Distance Summary
SELECT
  "year",
  "month",
  count(*) AS trips,
  sum(total_amount) AS gross_amount,
  min(trip_distance) AS min_distance,
  max(trip_distance) AS max_distance
FROM tlcp__yellow_tripdata
GROUP BY "year", "month"
ORDER BY "year", "month";

-- Q06: Full High-Volume FHV Row Count
SELECT count(*) AS row_count
FROM tlcp__fhvhv_tripdata;

-- Q07: High-Volume FHV Count For One Partition Month
SELECT count(*) AS row_count
FROM tlcp__fhvhv_tripdata
WHERE "year" = '2022'
  AND "month" = '10';

-- Q08: Long And Expensive High-Volume FHV Trips
SELECT count(*) AS row_count
FROM tlcp__fhvhv_tripdata
WHERE trip_miles >= 10.0
  AND base_passenger_fare >= 40.0;

-- Q09: Monthly High-Volume FHV Fare And Distance Summary
SELECT
  "year",
  "month",
  count(*) AS trips,
  sum(base_passenger_fare) AS passenger_fare,
  sum(driver_pay) AS driver_pay,
  min(trip_miles) AS min_miles,
  max(trip_miles) AS max_miles
FROM tlcp__fhvhv_tripdata
GROUP BY "year", "month"
ORDER BY "year", "month";

-- Q10: High-Volume FHV Shared-Ride Flag Distribution
SELECT
  shared_request_flag,
  shared_match_flag,
  count(*) AS trips
FROM tlcp__fhvhv_tripdata
WHERE "year" = '2022'
GROUP BY shared_request_flag, shared_match_flag
ORDER BY trips DESC;
