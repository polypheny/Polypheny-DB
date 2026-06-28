-- Q1_P: Repartitioned full table baseline
SELECT count(*) AS row_count
FROM yellow_tripdata;

-- Q2_P: Repartitioned filter by year
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE "year" = '2022';

-- Q3_P: Repartitioned filter by year and month
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';

-- Q4_P: Repartitioned filtered count, full scan
SELECT count(*) AS row_count
FROM green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;

-- Q5_P: Repartitioned filtered count with year partition
SELECT count(*) AS row_count
FROM green_tripdata
WHERE  "year" = '2022'
  AND trip_distance >= 10.0
  AND total_amount >= 40.0;
