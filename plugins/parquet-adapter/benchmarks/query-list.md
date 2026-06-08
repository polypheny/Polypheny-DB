# SQL Query List

This section lists every benchmark query. SQL uses the DuckDB table names; the Polypheny client maps them to the `tlc__*` adapter tables.

## Q01 - Full scan count, yellow taxi

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata;
```

### Checks:
- full-table row counting.
- the fastest possible aggregate path.

### Base Functionality:
- count all rows in the yellow taxi table.

### Benchmark-Phase Optimization:
- calculate the total from Parquet metadata without reading trip rows.


## Q02 - Partition-pruned count, one yellow taxi "month"

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10';
```
### Checks:
- partition pruning for a selected month.
- the fast aggregate path after partition pruning.

### Base Functionality:
- discard files outside October 2022.

### Benchmark-Phase Optimization:
- calculate the total from metadata of the matching files without reading trip rows.

## Q03 - Projection-heavy scan, yellow taxi

```sql
SELECT
  PULocationID,
  DOLocationID,
  trip_distance,
  total_amount
FROM yellow_tripdata
WHERE "year" = '2022'
  AND "month" = '10'
LIMIT 100000;
```

### Checks:
- partition pruning for a selected month.
- efficient reading and transfer of 100,000 result rows.
- projection of selected columns from a wider table.

### Base Functionality:
- discard files outside October 2022.
- stop scanning after 100,000 rows have been produced.

### Benchmark-Phase Optimization:
- materialize flat projected values more efficiently using the lightweight row reader.

## Q04 - Timestamp row-group filter, yellow taxi

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE tpep_pickup_datetime >= TIMESTAMP '2022-10-15 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP '2022-10-16 00:00:00';
```

### Checks:
- timestamp predicate pushdown.
- row-group pruning using pickup-time statistics.
- filtered row counting.

### Base Functionality:
- push the pickup-time range filter into the Parquet reader.
- skip row groups that cannot contain trips from October 15, 2022.

### Benchmark-Phase Optimization:
- count matching rows inside the adapter without materializing complete taxi rows.
- read only the timestamp column needed to evaluate the filter.


## Q05 - Numeric row-group filter, yellow taxi

```sql
SELECT count(*) AS row_count
FROM yellow_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;
```

### Checks:
- numeric predicate pushdown.
- row-group pruning using distance and fare statistics.
- filtered row counting with multiple conditions.

### Base Functionality:
- push both numeric filters into the Parquet reader.
- skip row groups that cannot contain matching trips.

### Benchmark-Phase Optimization:
- count matching rows inside the adapter without materializing complete taxi rows.
- read only the two numeric columns needed to evaluate the filters.

## Q06 - Monthly yellow taxi aggregates

```sql
SELECT
  "year",
  "month",
  count(*) AS trips,
  sum(total_amount) AS gross_amount,
  avg(trip_distance) AS avg_distance
FROM yellow_tripdata
GROUP BY "year", "month"
ORDER BY "year", "month";
```

### Checks:
- grouping by partition columns.
- calculation of `COUNT`, `SUM`, and `AVG`.
- ordering of monthly aggregate results.

### Base Functionality:
- expose `year` and `month` folder values as table columns.
- group results by month and order them chronologically.

### Benchmark-Phase Optimization:
- determine the monthly group from folder partition values instead of reading grouping values from each row.
- calculate counts from metadata and aggregate only the required numeric columns without materializing complete taxi rows.

## Q07 - Top pickup zones by yellow taxi trip count, low-cardinality grouping

```sql
SELECT
  PULocationID,
  count(*) AS trips,
  avg(total_amount) AS avg_total_amount
FROM yellow_tripdata
WHERE "year" = '2022'
GROUP BY PULocationID
ORDER BY trips DESC
LIMIT 20;
```

### Checks:
- partition pruning for a selected year.
- grouping by pickup location.
- calculation of `COUNT` and `AVG`.
- ordering groups and returning the top 20 results.

### Base Functionality:
- discard files outside 2022.
- order pickup locations by trip count and return the 20 busiest locations.

### Benchmark-Phase Optimization:
- calculate grouped aggregates inside the Parquet adapter.
- read only the pickup-location and total-amount columns instead of complete taxi rows.


## Q08 - Top pickup/dropoff pairs for yellow taxi

```sql
SELECT
  PULocationID,
  DOLocationID,
  count(*) AS trips,
  avg(trip_distance) AS avg_distance,
  avg(total_amount) AS avg_total_amount
FROM yellow_tripdata
WHERE "year" = '2022'
GROUP BY PULocationID, DOLocationID
ORDER BY trips DESC
LIMIT 20;
```

### Checks:
- partition pruning for a selected year.
- grouping by two location columns.
- calculation of `COUNT` and `AVG`.
- ordering groups and returning the top 20 results. 
- compared with Q07, this creates more groups and tests the additional cost of tracking pickup(start)/dropoff(dest) combinations.

### Base Functionality:
- discard files outside 2022.
- order pickup/dropoff pairs by trip count and return the 20 busiest pairs.

### Benchmark-Phase Optimization:
- calculate grouped aggregates inside the Parquet adapter.
- read only the pickup-location, dropoff-location, distance, and total-amount columns instead of complete taxi rows.
- use an efficient representation for the two-column grouping key.


## Q09 - Full scan count, high-volume FHV

```sql
SELECT count(*) AS row_count
FROM fhvhv_tripdata;
```

### Checks:
- full-table row counting on the larger high-volume FHV dataset.
- the fastest possible aggregate path at a larger scale.

### Base Functionality:
- count all rows in the high-volume FHV table.

### Benchmark-Phase Optimization:
- calculate the total from Parquet metadata without reading trip rows.

## Q10 - Partition-pruned count, one high-volume FHV "month"

```sql
SELECT count(*) AS row_count
FROM fhvhv_tripdata
WHERE "year" = '2022'
  AND "month" = '10';
```

### Checks:
- partition pruning for a selected month on the larger **high-volume** FHV dataset.
- the fast aggregate path after partition pruning.
- This is the large-table counterpart of Q02

### Base Functionality:
- discard files outside October 2022.

### Benchmark-Phase Optimization:
- calculate the total from metadata of the matching files without reading trip rows.


## Q11 - Timestamp row-group filter, high-volume FHV

```sql
SELECT count(*) AS row_count
FROM fhvhv_tripdata
WHERE pickup_datetime >= TIMESTAMP '2022-10-15 00:00:00'
  AND pickup_datetime < TIMESTAMP '2022-10-16 00:00:00';
```

### Checks:
- timestamp predicate pushdown on the larger high-volume FHV dataset.
- row-group pruning using pickup-time statistics.
- filtered row counting.

### Base Functionality:
- push the pickup-time range filter into the Parquet reader.
- skip row groups that cannot contain trips from October 15, 2022.

### Benchmark-Phase Optimization:
- count matching rows inside the adapter without materializing complete trip rows.
- read only the timestamp column needed to evaluate the filter.


## Q12 - High-volume FHV fare and distance filter

```sql
SELECT count(*) AS row_count
FROM fhvhv_tripdata
WHERE trip_miles >= 10.0
  AND base_passenger_fare >= 40.0;
```

### Checks:
- numeric predicate pushdown on the larger high-volume FHV dataset.
- row-group pruning using distance and fare statistics.
- filtered row counting with multiple conditions.

### Base Functionality:
- push both numeric filters into the Parquet reader.
- skip row groups that cannot contain matching trips.

### Benchmark-Phase Optimization:
- count matching rows inside the adapter without materializing complete trip rows.
- read only the distance and fare columns needed to evaluate the filters.


## Q13 - Monthly high-volume FHV aggregates

```sql
SELECT
  "year",
  "month",
  count(*) AS trips,
  sum(base_passenger_fare) AS passenger_fare,
  sum(driver_pay) AS driver_pay,
  avg(trip_miles) AS avg_miles
FROM fhvhv_tripdata
GROUP BY "year", "month"
ORDER BY "year", "month";
```

### Checks:
- grouping by partition columns on the larger high-volume FHV dataset.
- calculation of `COUNT`, multiple `SUM` values, and `AVG`.
- ordering of monthly aggregate results.

### Base Functionality:
- expose `year` and `month` folder values as table columns.
- group results by month and order them chronologically.

### Benchmark-Phase Optimization:
- calculate monthly aggregates inside the Parquet adapter.
- determine the monthly group from folder partition values instead of reading grouping values from each row.
- calculate counts from metadata and aggregate only the required fare, driver-pay, and distance columns without materializing complete trip rows.

## Q14 - Top high-volume FHV pickup/dropoff pairs

```sql
SELECT
  PULocationID,
  DOLocationID,
  count(*) AS trips,
  avg(trip_miles) AS avg_miles,
  avg(base_passenger_fare) AS avg_fare
FROM fhvhv_tripdata
WHERE "year" = '2022'
GROUP BY PULocationID, DOLocationID
ORDER BY trips DESC
LIMIT 20;
```

### Checks:
- partition pruning for a selected year.
- grouping by starting-location and destination-location pairs.
- calculation of `COUNT` and `AVG`.
- ordering groups and returning the top 20 results on the larger high-volume FHV dataset.
- This is the larger-table counterpart of Q08.

### Base Functionality:
- discard files outside 2022.
- order location pairs by trip count and return the 20 busiest pairs.

### Benchmark-Phase Optimization:
- calculate grouped aggregates inside the Parquet adapter.
- read only the starting-location, destination-location, distance, and fare columns instead of complete trip rows.
- use an efficient representation for the two-column grouping key.

## Q15 - Low-cardinality shared-ride flag distribution, high-volume FHV

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

### Checks:
- partition pruning for a selected year.
- grouping by two low-cardinality text flags. (column has only a small number of distinct values)
- counting trips for each shared-ride flag combination.
- ordering combinations by trip count.

### Base Functionality:
- discard files outside 2022.
- order shared-ride flag combinations from most common to least common.

### Benchmark-Phase Optimization:
- calculate grouped counts inside the Parquet adapter.
- read only the two shared-ride flag columns instead of complete trip rows.

## Q16 - Green taxi monthly aggregates

```sql
SELECT
  "year",
  "month",
  count(*) AS trips,
  sum(total_amount) AS gross_amount,
  avg(trip_distance) AS avg_distance
FROM green_tripdata
GROUP BY "year", "month"
ORDER BY "year", "month";
```

### Checks:
- grouping by partition columns on the smaller green taxi dataset.
- calculation of `COUNT`, `SUM`, and `AVG`.
- ordering of monthly aggregate results.

### Base Functionality:
- expose `year` and `month` folder values as table columns.
- group results by month and order them chronologically.

### Benchmark-Phase Optimization:
- calculate monthly aggregates inside the Parquet adapter.
- determine the monthly group from folder partition values instead of reading grouping values from each row.
- calculate counts from metadata and aggregate only the required amount and distance columns without materializing complete trip rows.

Smaller-table counterpart of Q06

## Q17 - FHV monthly trip counts

```sql
SELECT
  "year",
  "month",
  count(*) AS trips
FROM fhv_tripdata
GROUP BY "year", "month"
ORDER BY "year", "month";
```

### Checks:
- grouping by partition columns on the classic FHV dataset.
- monthly `COUNT(*)` calculation.
- ordering of monthly results.

### Base Functionality:
- expose `year` and `month` folder values as table columns.
- group results by month and order them chronologically.

### Benchmark-Phase Optimization:
- determine the monthly group from folder partition values.
- calculate each monthly count from Parquet metadata without reading trip rows.

Simpler than Q06, Q13, and Q16 because it calculates only row counts

## Q18 - Yellow vs green monthly taxi totals

```sql
WITH yellow_monthly AS (
  SELECT "year", "month", count(*) AS yellow_trips, sum(total_amount) AS yellow_amount
  FROM yellow_tripdata
  GROUP BY "year", "month"
),
green_monthly AS (
  SELECT "year", "month", count(*) AS green_trips, sum(total_amount) AS green_amount
  FROM green_tripdata
  GROUP BY "year", "month"
)
SELECT
  y."year",
  y."month",
  y.yellow_trips,
  g.green_trips,
  y.yellow_amount,
  g.green_amount
FROM yellow_monthly y
JOIN green_monthly g
  ON y."year" = g."year"
 AND y."month" = g."month"
ORDER BY y."year", y."month";
```

### Checks:
- monthly aggregation on two taxi datasets.
- calculation of `COUNT` and `SUM`.
- joining monthly aggregate results by `year` and `month`.
- ordering the joined results chronologically.

### Base Functionality:
- join matching yellow and green taxi months.
- order the combined monthly results.

### Benchmark-Phase Optimization:
- calculate aggregates separately inside the Parquet adapter before joining the datasets.
- determine monthly groups from folder partition values.
- calculate counts from metadata and aggregate only the required amount columns.
- join the small monthly result sets instead of joining individual trip rows. 

The key idea is early data reduction: millions of trip rows become a few dozen monthly rows before the join occurs.

## Q19 - Taxi vs high-volume FHV monthly counts

```sql
WITH yellow_monthly AS (
  SELECT "year", "month", count(*) AS yellow_trips
  FROM yellow_tripdata
  GROUP BY "year", "month"
),
fhvhv_monthly AS (
  SELECT "year", "month", count(*) AS fhvhv_trips
  FROM fhvhv_tripdata
  GROUP BY "year", "month"
)
SELECT
  y."year",
  y."month",
  y.yellow_trips,
  f.fhvhv_trips
FROM yellow_monthly y
JOIN fhvhv_monthly f
  ON y."year" = f."year"
 AND y."month" = f."month"
ORDER BY y."year", y."month";
```

### Checks:
- monthly trip counting on two datasets with very different sizes.
- grouping by partition columns.
- joining monthly count results by `year` and `month`.
- ordering the joined results chronologically.

### Base Functionality:
- join matching yellow taxi and high-volume FHV months.
- order the combined monthly results.

### Benchmark-Phase Optimization:
- determine monthly groups from folder partition values.
- calculate both monthly counts from Parquet metadata without reading trip rows.
- join the small monthly result sets instead of joining individual trip rows.

This is a metadata-only counterpart of Q18. It verifies that early aggregation remains effective even when one input is the much larger high-volume FHV table.

## Q20 - Wide high-volume FHV scan with partition pruning

```sql
SELECT
  hvfhs_license_num,
  dispatching_base_num,
  originating_base_num,
  request_datetime,
  on_scene_datetime,
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  trip_miles,
  trip_time,
  base_passenger_fare,
  tolls,
  sales_tax,
  congestion_surcharge,
  airport_fee,
  tips,
  driver_pay,
  shared_request_flag,
  shared_match_flag,
  access_a_ride_flag,
  wav_request_flag,
  wav_match_flag
FROM fhvhv_tripdata
WHERE "year" = '2022'
  AND "month" = '10'
LIMIT 100000;
```

### Checks:
- partition pruning for a selected month.
- projection of 23 columns from the wider high-volume FHV table.
- efficient reading and transfer of 100,000 wide result rows.

### Base Functionality:
- discard files outside October 2022.
- push the projection into the Parquet adapter.
- read only the requested columns.
- stop scanning after 100,000 rows have been produced.

### Benchmark-Phase Optimization:
- materialize flat projected values more efficiently using the lightweight row reader.

This is the larger and wider counterpart of Q03. It measures how the optimized row reader behaves when each returned row contains substantially more data.

## Q21 - Yellow taxi projected scan without partition pruning

```sql
SELECT
  PULocationID,
  DOLocationID,
  trip_distance,
  total_amount
FROM yellow_tripdata
LIMIT 100000;
```

### Checks:
- projection of selected columns without partition pruning.
- efficient reading and transfer of 100,000 result rows.
- projected scan performance when files cannot be discarded first.

### Base Functionality:
- push the projection into the Parquet adapter.
- read only the four requested columns.
- stop scanning after 100,000 rows have been produced.

### Benchmark-Phase Optimization:
- materialize flat projected values more efficiently using the lightweight row reader.

This is the no-partition-filter counterpart of Q03. It isolates projected row reading from partition pruning.


## Q22 - High-volume FHV wide projected scan without partition pruning

```sql
SELECT
  hvfhs_license_num,
  dispatching_base_num,
  pickup_datetime,
  dropoff_datetime,
  PULocationID,
  DOLocationID,
  trip_miles,
  trip_time,
  base_passenger_fare,
  tolls,
  sales_tax,
  tips,
  driver_pay,
  shared_request_flag,
  shared_match_flag
FROM fhvhv_tripdata
LIMIT 100000;
```

### Checks:
- projection of 15 columns without partition pruning.
- efficient reading and transfer of 100,000 wide result rows.
- projected scan performance on the larger high-volume FHV dataset when files cannot be discarded first.

### Base Functionality:
- push the projection into the Parquet adapter.
- read only the requested columns.
- stop scanning after 100,000 rows have been produced.

### Benchmark-Phase Optimization:
- materialize flat projected values more efficiently using the lightweight row reader.

This is the no-partition-filter counterpart of Q20.

## Q23 - Yellow taxi timestamp and numeric conjunctive filter

```sql
SELECT count(*) AS matching_trips
FROM yellow_tripdata
WHERE tpep_pickup_datetime >= TIMESTAMP '2022-10-15 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP '2022-10-16 00:00:00'
  AND trip_distance >= 2.0
  AND total_amount >= 15.0;
```

### Checks:
- filtering with multiple conditions combined using `AND`.
- timestamp and numeric predicate pushdown.
- row-group pruning using pickup-time, distance, and amount statistics.
- filtered row counting.

### Base Functionality:
- push the timestamp, distance, and amount filters into the Parquet reader.
- skip files and row groups that cannot contain matching trips.

### Benchmark-Phase Optimization:
- count matching rows inside the adapter without materializing complete taxi rows.
- read only the timestamp, distance, and amount columns needed to evaluate the filters.

This extends Q04 and Q05 by combining timestamp and numeric filtering in one query.

## Q24 - Yellow taxi numeric OR filter

```sql
SELECT count(*) AS matching_trips
FROM yellow_tripdata
WHERE trip_distance >= 20.0
   OR total_amount >= 100.0;
```

### Checks:
- filtering with numeric conditions combined using `OR`.
- predicate pushdown for OR filter.
- filtered row counting for a more difficult filter shape.

### Base Functionality:
- push the distance and amount filter into the Parquet reader.
- retain a trip when either numeric condition matches.
- skip files and row groups only when neither condition can match.

### Benchmark-Phase Optimization:
- count matching rows inside the adapter without materializing complete taxi rows.
- read only the distance and amount columns needed to evaluate the filter.

OR is more difficult to optimize than AND: data can be discarded only when every alternative is impossible.

## Q25 - Yellow taxi IN-list and numeric filter

```sql
SELECT count(*) AS matching_trips
FROM yellow_tripdata
WHERE PULocationID IN (132, 138, 161, 236, 237)
  AND trip_distance >= 1.0
  AND total_amount >= 5.0;
```

### Checks:
- filtering by a list of pickup zones using `IN`.
- combining an `IN` list with numeric conditions using `AND`.
- filtered row counting.

### Base Functionality:
- retain trips starting in one of the five selected pickup zones.
- require matching trips to also satisfy the distance and amount conditions.

### Benchmark-Phase Optimization:
- convert the `IN` list into equivalent equality alternatives for predicate pushdown.
- count matching rows inside the adapter without materializing complete taxi rows.
- read only the pickup-zone, distance, and amount columns needed to evaluate the filters.


Conceptually: `PULocationID IN (132, 138, 161, 236, 237)` means `PULocationID = 132 OR PULocationID = 138 OR ...`

## Q26 - High-volume FHV combination of timestamp and text filtering

```sql
SELECT count(*) AS matching_trips
FROM fhvhv_tripdata
WHERE pickup_datetime >= TIMESTAMP '2022-10-15 00:00:00'
  AND pickup_datetime < TIMESTAMP '2022-10-16 00:00:00'
  AND shared_request_flag = 'Y';
```

### Checks:
- filtering with timestamp conditions and a text flag condition combined using `AND`.
- predicate pushdown on the larger high-volume FHV dataset.
- filtered row counting.

### Base Functionality:
- retain trips from October 15, 2022 where a shared ride was requested.
- skip files and row groups that cannot contain matching trips.

### Benchmark-Phase Optimization:
- count matching rows inside the adapter without materializing complete trip rows.
- read only the pickup-time and shared-request flag columns needed to evaluate the filters.

The shared_request_flag = 'Y' condition keeps trips for which the passenger requested a shared ride.

## Q27 - High-volume FHV numeric conjunctive filter

```sql
SELECT count(*) AS matching_trips
FROM fhvhv_tripdata
WHERE trip_miles >= 5.0
  AND base_passenger_fare >= 25.0
  AND driver_pay >= 10.0;
```

### Checks:
- filtering with three numeric conditions combined using `AND`.
- predicate pushdown across multiple physical columns.
- filtered row counting on the larger high-volume FHV dataset.

### Base Functionality:
- retain trips only when distance, passenger fare, and driver pay satisfy their thresholds.
- skip files and row groups that cannot contain matching trips.

### Benchmark-Phase Optimization:
- count matching rows inside the adapter without materializing complete trip rows.
- read only the distance, passenger-fare, and driver-pay columns needed to evaluate the filters.

This extends Q12 by adding a third numeric condition.

## Q28 - High-volume FHV text flag OR filter

```sql
SELECT count(*) AS matching_trips
FROM fhvhv_tripdata
WHERE shared_request_flag = 'Y'
   OR shared_match_flag = 'Y'
   OR wav_request_flag = 'Y';
```

### Checks:
- filtering with three text flag conditions combined using `OR`.
- predicate pushdown for a more difficult text-filter shape.
- filtered row counting on the larger high-volume FHV dataset.

### Base Functionality:
- retain a trip when any selected flag is set to `Y`.
- skip files and row groups only when none of the flag conditions can match.

### Benchmark-Phase Optimization:
- count matching rows inside the adapter without materializing complete trip rows.
- read only the three flag columns needed to evaluate the filter.

This is intentionally difficult: OR reduces pruning opportunities, and text flags usually match many rows.

## Q29 - Yellow taxi scalar aggregate over physical columns

```sql
SELECT
  count(*) AS trips,
  sum(total_amount) AS gross_amount,
  avg(total_amount) AS avg_total_amount,
  min(trip_distance) AS min_distance,
  max(trip_distance) AS max_distance
FROM yellow_tripdata;
```

### Checks:
- calculation of multiple aggregates without grouping.
- support for `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
- aggregation over physical Parquet columns.

### Base Functionality:
- return one summary row for the full yellow taxi table.

### Benchmark-Phase Optimization:
- calculate aggregates inside the Parquet adapter.
- obtain the row count from metadata.
- aggregate only the amount and distance columns instead of materializing complete taxi rows.
  
Scalar aggregate means that the query has no GROUP BY and returns a single summary row.

## Q30 - High-volume FHV scalar aggregate over physical columns

```sql
SELECT
  count(*) AS trips,
  sum(base_passenger_fare) AS passenger_fare,
  sum(driver_pay) AS driver_pay,
  avg(trip_miles) AS avg_miles,
  max(trip_time) AS max_trip_time
FROM fhvhv_tripdata;
```

### Checks:
- calculation of multiple aggregates without grouping on the larger high-volume FHV dataset.
- support for `COUNT`, multiple `SUM` values, `AVG`, and `MAX`.
- aggregation over physical Parquet columns.

### Base Functionality:
- return one summary row for the full high-volume FHV table.

### Benchmark-Phase Optimization:
- calculate aggregates inside the Parquet adapter.
- obtain the row count from metadata.
- aggregate only the fare, driver-pay, distance, and trip-time columns instead of materializing complete trip rows.

This is the larger and wider counterpart of Q29. A scalar aggregate summarizes an entire selected dataset into one result row.

## Q31 - Yellow taxi grouped aggregate by low-cardinality pickup location without partition pruning

```sql
SELECT
  PULocationID,
  count(*) AS trips,
  sum(total_amount) AS gross_amount,
  avg(trip_distance) AS avg_distance
FROM yellow_tripdata
GROUP BY PULocationID
ORDER BY trips DESC
LIMIT 50;
```

### Checks:
- grouping by pickup location without partition pruning.
- calculation of `COUNT`, `SUM`, and `AVG`.
- ordering groups and returning the top 50 results.

### Base Functionality:
- order pickup locations by trip count and return the 50 busiest locations.

### Benchmark-Phase Optimization:
- calculate grouped aggregates inside the Parquet adapter.
- read only the pickup-location, amount, and distance columns instead of complete taxi rows.

This is the no-partition-filter counterpart of Q07.

## Q32 - Yellow taxi grouped aggregate with `HAVING`

```sql
SELECT
  DOLocationID,
  count(*) AS trips,
  avg(total_amount) AS avg_total_amount
FROM yellow_tripdata
GROUP BY DOLocationID
HAVING count(*) >= 100000
ORDER BY avg_total_amount DESC
LIMIT 50;
```

### Checks:
- grouping by destination location without partition pruning.
- calculation of `COUNT` and `AVG`.
- filtering aggregate groups using `HAVING`.
- ordering groups and returning the top 50 results.

### Base Functionality:
- retain only destination locations with at least 100,000 trips.
- order the remaining locations by average amount and return the top 50 results.

### Benchmark-Phase Optimization:
- calculate grouped aggregates inside the Parquet adapter before applying `HAVING`.
- read only the destination-location and amount columns instead of complete taxi rows.

The `HAVING` condition is applied to the grouped result rather than to individual trip rows.

## Q33 - Yellow taxi two-key grouped aggregate without partition pruning

```sql
SELECT
  PULocationID,
  DOLocationID,
  count(*) AS trips,
  avg(total_amount) AS avg_total_amount
FROM yellow_tripdata
GROUP BY PULocationID, DOLocationID
ORDER BY trips DESC
LIMIT 50;
```

### Checks:
- grouping by starting-location and destination-location pairs without partition pruning.
- calculation of `COUNT` and `AVG`.
- ordering groups and returning the top 50 results.

### Base Functionality:
- order location pairs by trip count and return the 50 busiest pairs.

### Benchmark-Phase Optimization:
- calculate grouped aggregates inside the Parquet adapter.
- read only the starting-location, destination-location, and amount columns instead of complete taxi rows.
- use an efficient representation for the two-column grouping key.

This is the no-partition-filter counterpart of Q08.

## Q34 - Low-cardinality shared-ride flag aggregates without partition pruning, high-volume FHV

```sql
SELECT
  shared_request_flag,
  shared_match_flag,
  count(*) AS trips,
  avg(trip_miles) AS avg_miles
FROM fhvhv_tripdata
GROUP BY shared_request_flag, shared_match_flag
ORDER BY trips DESC;
```

### Checks:
- grouping by two low-cardinality text flags without partition pruning.
- calculation of `COUNT` and `AVG`.
- ordering shared-ride flag combinations by trip count.

### Base Functionality:
- order shared-ride flag combinations from most common to least common.

### Benchmark-Phase Optimization:
- calculate grouped aggregates inside the Parquet adapter.
- read only the two shared-ride flag columns and the distance column instead of complete trip rows.

This is the no-partition-filter counterpart of Q15.

## Q35 - High-volume FHV grouped aggregate by license and base

```sql
SELECT
  hvfhs_license_num,
  dispatching_base_num,
  count(*) AS trips,
  avg(base_passenger_fare) AS avg_fare,
  avg(driver_pay) AS avg_driver_pay
FROM fhvhv_tripdata
GROUP BY hvfhs_license_num, dispatching_base_num
ORDER BY trips DESC
LIMIT 50;
```

### Checks:
- grouping by license and dispatching-base identifiers.
- calculation of `COUNT` and multiple `AVG` values.
- ordering groups and returning the top 50 results.

### Base Functionality:
- order license and dispatching-base combinations by trip count.
- return the 50 busiest combinations.

### Benchmark-Phase Optimization:
- calculate grouped aggregates inside the Parquet adapter.
- read only the license, dispatching-base, fare, and driver-pay columns instead of complete trip rows.

## Q36 - High-volume FHV two-key zone grouped aggregate without partition pruning

```sql
SELECT
  PULocationID,
  DOLocationID,
  count(*) AS trips,
  avg(trip_miles) AS avg_miles,
  avg(base_passenger_fare) AS avg_fare
FROM fhvhv_tripdata
GROUP BY PULocationID, DOLocationID
ORDER BY trips DESC
LIMIT 50;
```

### Checks:
- grouping by starting-location and destination-location pairs over the full high-volume FHV table.
- calculation of `COUNT` and multiple `AVG` values.
- ordering groups and returning the top 50 results.

### Base Functionality:
- order location pairs by trip count and return the 50 busiest pairs.

### Benchmark-Phase Optimization:
- calculate grouped aggregates inside the Parquet adapter.
- read only the starting-location, destination-location, distance, and fare columns instead of complete trip rows.
- use an efficient representation for the two-column grouping key.

This is the full-table counterpart of Q14.

## Q37 - Green taxi scalar aggregate over physical columns

```sql
SELECT
  count(*) AS trips,
  sum(total_amount) AS gross_amount,
  avg(total_amount) AS avg_total_amount,
  avg(trip_distance) AS avg_distance
FROM green_tripdata;
```

### Checks:
- calculation of multiple aggregates without grouping on the smaller green taxi dataset.
- support for `COUNT`, `SUM`, and multiple `AVG` values.
- aggregation over physical Parquet columns.

### Base Functionality:
- return one summary row for the full green taxi table.

### Benchmark-Phase Optimization:
- calculate aggregates inside the Parquet adapter.
- obtain the row count from metadata.
- aggregate only the amount and distance columns instead of materializing complete taxi rows.

This is the smaller-table counterpart of Q29.

## Q38 - Yellow and high-volume FHV pickup-location aggregate join

```sql
WITH yellow_pickups AS (
  SELECT PULocationID, count(*) AS yellow_trips, sum(total_amount) AS yellow_amount
  FROM yellow_tripdata
  GROUP BY PULocationID
),
fhvhv_pickups AS (
  SELECT PULocationID, count(*) AS fhvhv_trips, sum(base_passenger_fare) AS fhvhv_fare
  FROM fhvhv_tripdata
  GROUP BY PULocationID
)
SELECT
  y.PULocationID,
  y.yellow_trips,
  f.fhvhv_trips,
  y.yellow_amount,
  f.fhvhv_fare
FROM yellow_pickups y
JOIN fhvhv_pickups f
  ON y.PULocationID = f.PULocationID
ORDER BY f.fhvhv_trips DESC
LIMIT 50;
```

### Checks:
- grouping by pickup location on two datasets without partition pruning.
- calculation of `COUNT` and `SUM`.
- joining pickup-location aggregate results.
- ordering groups and returning the top 50 results.

### Base Functionality:
- join matching pickup locations from the yellow taxi and high-volume FHV datasets.
- order locations by high-volume FHV trip count and return the top 50 results.

### Benchmark-Phase Optimization:
- calculate grouped aggregates separately inside the Parquet adapter before joining the datasets.
- read only the pickup-location and amount columns needed by each dataset.
- join the small pickup-location result sets instead of joining individual trip rows.

This is the no-partition grouped-aggregate counterpart of Q18 and Q19.

## Q39 - Distinct low-cardinality flag combinations, high-volume FHV

```sql
SELECT DISTINCT
  shared_request_flag,
  shared_match_flag,
  access_a_ride_flag,
  wav_request_flag,
  wav_match_flag
FROM fhvhv_tripdata
ORDER BY shared_request_flag, shared_match_flag, access_a_ride_flag, wav_request_flag, wav_match_flag;
```

### Checks:
- selecting distinct combinations of several low-cardinality text flags.
- grouping behavior without aggregate functions.
- ordering the distinct combinations.

### Base Functionality:
- remove duplicate flag combinations.
- order the distinct combinations by flag value.

### Benchmark-Phase Optimization:
- calculate distinct combinations as grouped results inside the Parquet adapter.
- read only the five flag columns instead of complete trip rows.

## Q40 - Yellow taxi distance bucket aggregate

```sql
SELECT
  CASE
    WHEN trip_distance < 1.0 THEN '00_short'
    WHEN trip_distance < 5.0 THEN '01_medium'
    WHEN trip_distance < 20.0 THEN '02_long'
    ELSE '03_very_long'
  END AS distance_bucket,
  count(*) AS trips,
  avg(total_amount) AS avg_total_amount
FROM yellow_tripdata
GROUP BY
  CASE
    WHEN trip_distance < 1.0 THEN '00_short'
    WHEN trip_distance < 5.0 THEN '01_medium'
    WHEN trip_distance < 20.0 THEN '02_long'
    ELSE '03_very_long'
  END
ORDER BY distance_bucket;
```

### Checks:
- grouping by a distance category derived from a `CASE` expression.
- calculation of `COUNT` and `AVG`.
- ordering the derived distance categories.

### Base Functionality:
- assign each trip to a distance category.
- order the category summaries.

### Benchmark-Phase Optimization:
- evaluate the derived grouping expression and calculate aggregates inside the Parquet adapter.
- read only the distance and amount columns instead of complete taxi rows.
- return one summary row per distance category instead of materializing complete taxi rows outside the adapter.

This checks the generic calculated-aggregate path rather than grouping by direct physical columns.
