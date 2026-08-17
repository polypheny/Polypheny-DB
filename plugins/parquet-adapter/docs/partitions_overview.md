# Multi-File And Partitioned Folder Layouts

The relational and document Parquet sources can discover a single Parquet file,
a folder of compatible Parquet files, a Hive-style partitioned table folder, or
a dataset root containing multiple table folders.

Partition support is implemented by `ParquetFileDiscovery`,
`ParquetSourceFile`, `ParquetTableBinding`, `ParquetSchemaNormalizer`,
`ParquetRelationalSource`, and the source-file pruning evaluators.

## Supported Source Shapes

Single file:

```text
customers.parquet
```

Creates one table or collection backed by one `ParquetSourceFile`.

Folder with several compatible files:

```text
yellow_tripdata/
  yellow_tripdata_2025-01.parquet
  yellow_tripdata_2025-02.parquet
```

Creates one logical table backed by multiple `ParquetSourceFile` objects when
the exported schemas can be consolidated.

Partitioned table folder:

```text
yellow_tripdata/
  year=2025/
    month=01/
      part-1.parquet
    month=02/
      part-2.parquet
  year=2026/
    month=01/
      part-3.parquet
```

Creates one logical table with virtual partition columns:

- `year`
- `month`

Dataset root:

```text
tlc/
  yellow_tripdata/
    year=2025/month=01/part-1.parquet
  green_tripdata/
    year=2025/month=01/part-1.parquet
```

Creates one discovered table per child table folder.

## Partition Values

Partition values are parsed from folder names of the form `key=value`.
`ParquetFileDiscovery` normalizes partition keys with `ParquetNameNormalizer`
and stores them in `ParquetSourceFile.partitionValues()`.

Example:

```text
yellow_tripdata/year=2025/month=01/part-1.parquet
```

produces:

```text
year = "2025"
month = "01"
```

Partition columns are exposed as normal columns. They are not stored inside the
Parquet row; row projection fills them from the current `ParquetSourceFile`.

## Polypheny Physical Partition Metadata

Only the first discovered partition column is mapped to Polypheny physical list
partition metadata. This is handled by `ParquetRelationalSource`:

- `firstPolyphenyPartitionColumn(...)` selects the first partition key
- `createPartitionedTable(...)` groups files by that partition value
- each partition allocation receives a binding containing only the files for
  that value

Additional partition columns remain adapter-level partition values. They are
still visible as columns and can still be used by file pruning.

If there is only one value for the selected first partition column, the adapter
keeps the normal single physical allocation instead of creating a partitioned
layout.

## Schema Consolidation

`ParquetSchemaReader.exportedSchema(...)` returns visible columns and a mapping
from exported column names to physical Parquet paths.

`ParquetFileDiscovery` can expose multiple files as one table when:

- their exported column lists are compatible enough
- their exported source paths are compatible
- partition/data column collisions are either absent or proven safe

Schema evolution is handled by creating the union of compatible columns. When a
file does not contain a projected data column, path-based readers return `NULL`
for that value.

## Partition Column Collisions

A partition folder may have the same normalized name as a physical Parquet
column. The adapter allows this only when footer metadata proves that the
physical column contains the same constant value as the folder partition value.

If the physical value conflicts with the folder value, discovery rejects the
file/table because the adapter cannot expose one column name with two different
meanings.

## Query Execution

For a query such as:

```sql
SELECT *
FROM "prn__yellow_tripdata"
WHERE "year" = '2025' AND "month" = '01';
```

the flow is:

1. `ParquetRelFilterTranslator` translates supported predicates.
2. `ParquetFilterResolver` maps filter indexes to `ParquetColumnBinding`
   objects and resolves dynamic parameters.
3. `ParquetSourceFilePartitionFilterEvaluator` checks each file's
   `partitionValues`.
4. `ParquetSourceFileStatisticsFilterEvaluator` checks file-level footer
   statistics where possible.
5. Files proven not to match are skipped.
6. Remaining files are opened by `ParquetSourceReader`.
7. Residual filters are applied by row-level evaluators.

## Flat And Normalized Modes

In `flat` schema mode, partition columns are added to the root table.

In `normalized` schema mode, partition columns are added to generated root
tables. Nested child tables keep their structural synthetic columns and read
their data through the same source-file bindings as the root table.

## Limitations

- Polypheny physical partition metadata is created only for the first partition
  column.
- Hierarchical partition folders beyond the first column are represented at the
  adapter level, not as nested Polypheny physical partitions.
- File pruning keeps files when a filter cannot be proven true or false from
  partition values or footer statistics.
