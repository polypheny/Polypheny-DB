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
