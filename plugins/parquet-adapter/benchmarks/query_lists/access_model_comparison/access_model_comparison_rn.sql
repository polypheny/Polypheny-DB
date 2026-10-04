-- Q01: Full record access
SELECT *
FROM tlcpn__green_tripdata;

-- Q02: Projection
SELECT
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  pulocationid,
  dolocationid,
  total_amount
FROM tlcpn__green_tripdata;

-- Q03: Filtered count
SELECT count(*) AS row_count
FROM tlcpn__green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;

-- Q04: Filtered full record access
SELECT *
FROM tlcpn__green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;

-- Q05: Filtered projection
SELECT
  lpep_pickup_datetime,
  pulocationid,
  dolocationid,
  trip_distance,
  total_amount
FROM tlcpn__green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;
