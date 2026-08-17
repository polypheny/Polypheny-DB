-- Q1_NP: Unpartitioned full table baseline
SELECT count(*) AS row_count
FROM yellow_tripdata;

-- Q2_NP: Unpartitioned physical year-column filter
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE "year" = '2022';

-- Q3_NP: Unpartitioned physical year/month-column filter
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';

-- Q4_NP: Unpartitioned filtered count, full scan
SELECT count(*) AS row_count
FROM green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;

-- Q5_NP: Unpartitioned filtered count with physical year column
SELECT count(*) AS row_count
FROM green_tripdata
WHERE  "year" = '2022'
  AND trip_distance >= 10.0
  AND total_amount >= 40.0;
