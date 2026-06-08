# Flickr8k Clean Parquet Nested Benchmark Dataset

## Overview

The Flickr8k Clean dataset is a small real-world image-caption dataset stored
as a Parquet file. It is intended for nested-data benchmark coverage in the
Parquet adapter, especially for repeated primitive columns and projection
behavior around large binary values.

The source dataset is
[Flickr 8k Dataset (Clean)](https://www.kaggle.com/datasets/habedi/flickr-8k-dataset-clean/data)
on Kaggle. The dataset page describes it as a cleaned Parquet version of
Flickr8k with approximately 8,000 images. Each image has five human-written
captions.

This dataset should be used as a complement to the NYC TLC benchmark dataset.
The TLC dataset exercises large flat and partitioned tables. Flickr8k exercises
a different physical shape: one row per image, a repeated captions field, and a
large binary image field.

## Logical Content

Each row represents one image and its captions.

| Column     | Expected type        | Description |
|------------|----------------------|-------------|
| `id`       | string               | Image filename, for example `1000268201_693b08cb0e.jpg`. |
| `captions` | list of strings      | Five short human-written text descriptions of the image. |
| `image`    | binary / JPEG bytes  | Raw JPEG image bytes stored inside the Parquet row. SQL benchmarks should treat this as binary data, not as a decoded image. |

The expected Parquet shape is shallow nested data:

```text
id: string
captions: list<string>
image: binary
```

The important nested field is `captions`. It is a repeated primitive value,
which makes the dataset useful for testing whether the adapter can read and
materialize list columns correctly. The `image` field is useful for projection
benchmarks because it is much larger than the metadata and caption fields.

## Benchmark Purpose

This dataset is useful for the following benchmark categories:

- Flat projection over image metadata, for example selecting only `id`.
- Nested projection over repeated values, for example selecting `id` and
  `captions`.
- Binary projection, for example selecting `id` and `image`.
- Full-row scans that read `id`, `captions`, and `image` together.
- Projection pruning checks that compare scans with and without the `image`
  column.
- Correctness checks for repeated primitive values.

The most important comparison is between queries that include `image` and
queries that exclude it. If projection pruning works well, queries over `id`
and `captions` should avoid paying the full cost of reading JPEG bytes.

## Limitations

Flickr8k Clean is a good starter dataset for nested Parquet behavior, but it is
not a complete nested-data stress benchmark.

- The nesting is shallow: `captions` is a list of strings. The dataset does not
  cover structs, maps, deeply nested lists, or `LIST<STRUCT<...>>` shapes.
- The repeated cardinality is regular: each image has five captions. This does
  not strongly test variable-length repeated fields.
- The row count is small compared with the TLC benchmark dataset. It is better
  for correctness, projection behavior, and regression testing than for large
  throughput measurements.
- Full scans that include `image` may be dominated by binary I/O. Those results
  should not be interpreted as measuring only nested-column reader performance.
- The dataset is not designed for aggregation benchmarks. It has few numeric
  columns and no partition layout.

For broader nested coverage, pair this dataset with a synthetic Parquet fixture
that includes variable-length arrays, nested structs, and arrays of structs.

## Suggested Benchmark Queries

The benchmark query set should keep separate query groups for metadata,
captions, binary image bytes, and full-row reads.

```sql
-- Metadata-only projection.
SELECT id
FROM flickr8k;

-- Repeated primitive projection.
SELECT id, captions
FROM flickr8k;

-- Binary projection.
SELECT id, image
FROM flickr8k;

-- Full-row projection.
SELECT id, captions, image
FROM flickr8k;
```

If the SQL surface supports nested access or unnesting in the benchmark path,
additional queries can be added later. Until then, the core value of this
dataset is measuring whole-list materialization and projection pruning.

## Reporting Notes

Benchmark results should record:

- dataset source and download date
- local file path
- Parquet file size
- row count
- row group count
- physical Parquet schema
- compression codec
- whether the query selected the `image` column

These details are important because binary payload size, row group layout, and
compression can dominate the observed runtime.

## License

The Kaggle dataset page lists the license as CC BY-NC 4.0. This is suitable for
internal, academic, and non-commercial benchmark work, but it should be reviewed
before using the dataset in a commercial or broadly redistributed benchmark
package.
