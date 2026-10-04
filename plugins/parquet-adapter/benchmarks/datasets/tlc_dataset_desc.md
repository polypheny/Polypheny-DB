# NYC TLC Partitioned Parquet Benchmark Dataset

## Overview

The benchmark dataset is a local, read-only snapshot of New York City Taxi and
Limousine Commission (NYC TLC) trip records stored as Parquet files. It is used
to evaluate scan, projection, filtering, partition pruning, aggregation, and
multi-file query behavior in the Polypheny Parquet adapter.

The local dataset root is:

```text
C:\PolyData\tlc_partitioned
```

The source data is based on the monthly Parquet downloads published on the
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
page. The local snapshot contains four trip-record categories:

- `yellow_tripdata`: yellow taxi trips
- `green_tripdata`: green taxi trips
- `fhv_tripdata`: for-hire vehicle (FHV) trips
- `fhvhv_tripdata`: high-volume for-hire vehicle (HVFHV) trips

The statistics in this document describe the files present under
`C:\PolyData\tlc_partitioned` on May 31, 2026. They should be recorded together
with benchmark results because the NYC TLC website continues to publish newer
monthly files.

## Physical Layout

The snapshot uses Hive-style directory partitions. Each trip-record category is
a logical table. Each table contains one Parquet file per month under
`year=YYYY\month=MM` folders:

```text
tlc_partitioned\
  yellow_tripdata\
    year=2020\
      month=01\
        yellow_tripdata_2020-01.parquet
      ...
    year=2023\
      month=01\
        yellow_tripdata_2023-01.parquet
  green_tripdata\
    year=YYYY\
      month=MM\
        green_tripdata_YYYY-MM.parquet
  fhv_tripdata\
    year=YYYY\
      month=MM\
        fhv_tripdata_YYYY-MM.parquet
  fhvhv_tripdata\
    year=YYYY\
      month=MM\
        fhvhv_tripdata_YYYY-MM.parquet
```

The `year` and `month` values are derived from the directory path. They are
virtual partition columns rather than physical columns stored inside the
Parquet files. This layout allows an engine to skip unrelated files when a
query restricts one or both partition keys.

Every table covers the same 37 monthly partitions:

- January 2020 through December 2020
- January 2021 through December 2021
- January 2022 through December 2022
- January 2023

Starting from February 2023 the Parquet schema has changed.

## Dataset Summary

| Table             | Monthly files |            Rows | Parquet row groups |       Stored bytes |   Stored size |
|-------------------|--------------:|----------------:|-------------------:|-------------------:|--------------:|
| `yellow_tripdata` |            37 |      98,276,264 |                190 |      1,605,205,637 |      1.49 GiB |
| `green_tripdata`  |            37 |       3,711,544 |                 37 |         69,056,558 |      0.06 GiB |
| `fhv_tripdata`    |            37 |      45,376,714 |                 38 |        465,048,942 |      0.43 GiB |
| `fhvhv_tripdata`  |            37 |     548,801,637 |                179 |     14,289,433,466 |     13.31 GiB |
| **Total**         |       **148** | **696,166,159** |            **444** | **16,428,744,603** | **15.30 GiB** |

The high-volume FHV table dominates the fixture. It contains approximately
78.83% of all rows and 86.98% of the stored bytes. Queries over
`fhvhv_tripdata` are therefore the primary stress cases for scan throughput,
filter evaluation, row-group pruning, aggregation, and result materialization.

Monthly file sizes vary substantially:

| Table             | Minimum file size | Average file size | Maximum file size |
|-------------------|------------------:|------------------:|------------------:|
| `yellow_tripdata` |          4.98 MiB |         41.37 MiB |        117.72 MiB |
| `green_tripdata`  |          0.68 MiB |          1.78 MiB |          6.85 MiB |
| `fhv_tripdata`    |          6.19 MiB |         11.99 MiB |         18.59 MiB |
| `fhvhv_tripdata`  |        141.37 MiB |        368.31 MiB |        532.98 MiB |

## Compression

The snapshot preserves the compression codecs of the monthly source files. It
is not normalized to one compression setting.

| Table             | GZIP files | SNAPPY files |
|-------------------|-----------:|-------------:|
| `yellow_tripdata` |         29 |            8 |
| `green_tripdata`  |         37 |            0 |
| `fhv_tripdata`    |         36 |            1 |
| `fhvhv_tripdata`  |         35 |            2 |

The codec mix is part of the benchmark fixture. Comparisons against another
engine should use the same files without rewriting them unless the rewritten
layout is reported as a separate experiment.

## Table Content

### Yellow Taxi Trips

`yellow_tripdata` contains pickup and drop-off timestamps and location IDs,
passenger count, trip distance, rate and payment information, and itemized fare
amounts.

Physical columns:

```text
VendorID
tpep_pickup_datetime
tpep_dropoff_datetime
passenger_count
trip_distance
RatecodeID
store_and_fwd_flag
PULocationID
DOLocationID
payment_type
fare_amount
extra
mta_tax
tip_amount
tolls_amount
improvement_surcharge
total_amount
congestion_surcharge
airport_fee
```

### Green Taxi Trips

`green_tripdata` has a similar taxi-trip shape with green-taxi pickup and
drop-off timestamps and additional green-taxi-specific fields.

Physical columns:

```text
VendorID
lpep_pickup_datetime
lpep_dropoff_datetime
store_and_fwd_flag
RatecodeID
PULocationID
DOLocationID
passenger_count
trip_distance
fare_amount
extra
mta_tax
tip_amount
tolls_amount
ehail_fee
improvement_surcharge
total_amount
payment_type
trip_type
congestion_surcharge
```

### For-Hire Vehicle Trips

`fhv_tripdata` is a compact FHV trip representation containing base identifiers,
timestamps, location IDs, and a shared-ride flag.

Physical columns:

```text
dispatching_base_num
pickup_datetime
dropOff_datetime
PUlocationID
DOlocationID
SR_Flag
Affiliated_base_number
```

### High-Volume For-Hire Vehicle Trips

`fhvhv_tripdata` is the largest and widest table. It contains operator
identifiers, request lifecycle timestamps, pickup and drop-off locations,
distance and duration values, itemized fare values, and accessibility and
shared-ride flags.

Physical columns:

```text
hvfhs_license_num
dispatching_base_num
originating_base_num
request_datetime
on_scene_datetime
pickup_datetime
dropoff_datetime
PULocationID
DOLocationID
trip_miles
trip_time
base_passenger_fare
tolls
bcf
sales_tax
congestion_surcharge
airport_fee
tips
driver_pay
shared_request_flag
shared_match_flag
access_a_ride_flag
wav_request_flag
wav_match_flag
```

All four tables additionally expose the virtual partition columns `year` and
`month`.

## Benchmark Characteristics

This fixture is useful for evaluating:

- multi-file table discovery and scans
- full-table and partition-pruned counts
- folder-based pruning using `year` and `month`
- Parquet row-group pruning using timestamp and numeric predicates
- projection pruning and row materialization
- grouped aggregates, ordering, limits, and aggregate joins
- behavior across small, medium, and high-volume tables

The Parquet files contain flat records. When the Polypheny document adapter is
used, each Parquet row can be exposed as one document. This makes the fixture
suitable for direct document-on-Parquet scan comparisons against another engine
that queries the same files in place.

The fixture does **not** contain nested structs or repeated fields. It should
not be used alone to claim performance results for nested document
reconstruction, nested-path filtering, array traversal, or unwind operations.
A separate nested Parquet fixture is required for those experiments.
