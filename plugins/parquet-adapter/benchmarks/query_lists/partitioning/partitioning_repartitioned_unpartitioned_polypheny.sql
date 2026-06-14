-- Q01: Repartitioned full table baseline
SELECT count(*) AS row_count
FROM tlcr__yellow_tripdata;

-- Q02: Repartitioned partition by year
SELECT count(*) AS row_count
FROM tlcr__yellow_tripdata
WHERE "year" = '2022';

-- Q03: Repartitioned partition by month
SELECT count(*) AS row_count
FROM tlcr__yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';

-- Q04: Repartitioned filtered count, full scan
SELECT count(*) AS row_count
FROM tlcr__green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;

-- Q05: Repartitioned filtered count, partition by year
SELECT count(*) AS row_count
FROM tlcr__green_tripdata
WHERE  "year" = '2022'
  AND trip_distance >= 10.0
  AND total_amount >= 40.0;

-------------Unpartitioned

-- Q06: Unpartitioned full table baseline
SELECT count(*) AS row_count
FROM tlcu__yellow_tripdata;

-- Q07: Unpartitioned partition by year
SELECT count(*) AS row_count
FROM tlcu__yellow_tripdata
WHERE "year" = '2022';

-- Q08: Unpartitioned partition by month
SELECT count(*) AS row_count
FROM tlcu__yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';

-- Q09: Unpartitioned filtered count, full scan
SELECT count(*) AS row_count
FROM tlcu__green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;

-- Q10: Unpartitioned filtered count, partition by year
SELECT count(*) AS row_count
FROM tlcu__green_tripdata
WHERE  "year" = '2022'
  AND trip_distance >= 10.0
  AND total_amount >= 40.0;
