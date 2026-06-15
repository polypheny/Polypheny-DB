# Access Model Comparison Query Specification

The same logical query set is executed across Polypheny relational flat,
Polypheny relational normalized, Polypheny document MQL, DuckDB, and Apache
Spark. The normalized relational variant intentionally uses only the root flat
table fields, with no generated child-table joins.

| Query | Operation           | Predicate                                          | Returned data                                      | Purpose                                                    |
|-------|---------------------|----------------------------------------------------|----------------------------------------------------|------------------------------------------------------------|
| Q01   | Full scan           | None                                               | All fields                                         | Measures full dataset read performance                     |
| Q02   | Projection          | None                                               | Selected fields                                    | Measures projection behavior                               |
| Q03   | Filtered count      | `trip_distance >= 10.0` and `total_amount >= 40.0` | Aggregate count                                    | Measures filter evaluation without returning matching rows |
| Q04   | Filtered scan       | `trip_distance >= 10.0` and `total_amount >= 40.0` | All matching records or documents                  | Measures filtering with full result consumption            |
| Q05   | Filtered projection | `trip_distance >= 10.0` and `total_amount >= 40.0` | Selected fields from matching records or documents | Measures combined filtering and projection                 |


## Q1 - Full scan

Text:

```text
Find all green taxi trip records and return every available field.
```

SQL:

```text
SELECT *
FROM green_tripdata;
```

## Q2 - Projection

Text:

```text
Find all green taxi trip records and return only pickup time, drop-off time,
pickup location, drop-off location, and total amount.
```

SQL:

```text
SELECT
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  pulocationid,
  dolocationid,
  total_amount
FROM green_tripdata;
```

## Q3 - Filtered count

Text:

```text
Count green taxi trip records where trip distance is at least 10.0 and total
amount is at least 40.0.
```

SQL:

```text
SELECT count(*) AS row_count
FROM green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;
```

## Q4 - Filtered full scan

Text:

```text
Find green taxi trip records where trip distance is at least 10.0 and total
amount is at least 40.0, and return every available field.
```

SQL:

```text
SELECT *
FROM green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;
```

## Q5 - Filtered projection

Text:

```text
Find green taxi trip records where trip distance is at least 10.0 and total
amount is at least 40.0, and return only pickup time, pickup location, drop-off
location, trip distance, and total amount.
```

SQL:

```text
SELECT
  lpep_pickup_datetime,
  pulocationid,
  dolocationid,
  trip_distance,
  total_amount
FROM green_tripdata
WHERE trip_distance >= 10.0
  AND total_amount >= 40.0;
```
