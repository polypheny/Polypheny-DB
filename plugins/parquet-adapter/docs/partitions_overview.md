# Parquet Multi-File and Partitioned Folder Layout Design

## Goal

Add partitions support.

## General

Partitioning is a way to organize one logical table into multiple physical files or folders based on column values.

In our Parquet adapter, partition values come from Hive-style folder names, for example:

```text
yellow_tripdata/
  year=2025/
    month=01/
      yellow_tripdata_2025-01.parquet
```

This file belongs to the logical table `yellow_tripdata`, and the adapter derives two virtual column values from the path:

```text
year = '2025'
month = '01'
```

These partition columns are exposed together with the real Parquet data columns. When a query filters by partition columns, the adapter can skip files from non-matching folders before opening them. For example, `WHERE "year" = '2025' AND "month" = '01'` reads only files under `year=2025/month=01`.

So partitioning improves query performance by reducing the number of Parquet files that need to be opened and scanned.

## Logical and Physical Tables

If partition integration is triggered, then created:
- one logical table
- many physical tables: one physical table per value of the first partition column (one physical table per partition)

### Example:
```text
yellow_tripdata/
  year=2024/
    month=01/file.parquet
    month=02/file.parquet
  year=2025/
    month=01/file.parquet
    month=02/file.parquet
  year=2026/
    month=01/file.parquet
```

Logical table: 
- yellow_tripdata

Physical tables:

- yellow_tripdata partition year=2024
- yellow_tripdata partition year=2025
- yellow_tripdata partition year=2026

## New Functionality

The following functionality was implemented:

### 1. Support multiple Parquet files as one logical table.
- `ParquetFileDiscovery` discovers multiple files for one logical table.
- `DiscoveredTableBinding`, `ParquetTableBinding`, `ParquetSourceFile` store the file list.
- `ParquetRelTable` uses the file list at scan time.
- `ParquetMultiFileEnumerator` iterates over selected files one by one.
- `ParquetSchemaReader` can read schemas/footers from multiple files.

### 2. Support one common table schema across those files, including missing fields.
- `ParquetFileDiscovery` decides whether file schemas can be consolidated.
- `ParquetSchemaReader` builds a combined schema from multiple file footers.
- `ParquetSchemaNormalizer` creates normalized tables from discovered multi-file bindings.
- `ParquetTableBinding` maps logical/physical columns to Parquet source paths.
- `ParquetRelTable` decides when binding-aware scanning is needed.
- `ParquetPathValueExtractor` returns NULL when a source path is missing.

### 3. Add Hive-style partition discovery from `key=value` folders.
- `ParquetFileDiscovery` recursively discovers partition folders and parses key=value.
- `ParquetSourceFile` stores each file URL together with its partition values.
- `DiscoveredTable`, `DiscoveredTableBinding` carry discovered partitioned table metadata.
- `ParquetSchemaNormalizer` adds partition columns to normalized root tables.
- `ParquetTableBinding` marks those columns with `ParquetColumnRole.PARTITION`

### 4. Add file-level pruning from partition filters.
- `ParquetRelFilterTranslator` translates supported SQL filters into adapter filters.
- `ParquetRelScan`, `ParquetEnumerableFilterScanRule`, `ParquetEnumerableCalcScanRule` push filters into the scan.
- `ParquetRelTable` resolves filter columns to `ParquetColumnBinding`.
- `ParquetSourceFileFilterEvaluator` evaluates partition filters against `ParquetSourceFile.partitionValues()`.
- `ParquetPartitionAwareFilterEvaluator` evaluates partition filters during row-level scan fallback.

### 5. Integrate Polypheny's logical partition metadata for the first partition column.
- `ParquetRelationalSource` creates Polypheny partition `groups/partitions/allocations` for the first partition column.
- `ParquetRelationalSource.firstPolyphenyPartitionColumn` chooses the partition column.
- `ParquetRelationalSource.createPartitionedTable` creates the catalog partition layout.
- `AbstractQueryProcessor` maps detected partition filter values to accessed partition IDs.
- `StatisticsManagerImpl` aggregates statistics from multiple partition allocations.
- `ParquetStatisticsReader` provides partition-column statistics from folder values.


## Examples

### Table Creation

Given this non-partitioned folder:

```text
yellow_tripdata/
  yellow_tripdata_2025-01.parquet
  yellow_tripdata_2025-02.parquet
```

Polypheny should expose one table:

```text
parquetrelational1__yellow_tripdata
```

Given this partitioned folder:

```text
yellow_tripdata/
  year=2025/month=01/yellow_tripdata_2025-01.parquet
  year=2025/month=02/yellow_tripdata_2025-02.parquet
  year=2026/month=01/yellow_tripdata_2026-01.parquet
```

Polypheny should also expose one table:

```text
parquetrelational1__yellow_tripdata
```

The partitioned table should have:

- data columns from the consolidated Parquet file schemas
- virtual partition columns from folder names:
  - `year`
  - `month`

Example query:

```sql
SELECT *
FROM parquetrelational1__yellow_tripdata
WHERE "year" = '2025' AND "month" = '01';
```

Execution:

1. Translate supported filters as today.
2. Resolve `year = '2025'` and `month = '01'` as partition-column filters.
3. Select only files below `year=2025/month=01`.
4. Open only those files.
5. Apply existing native Parquet filters inside each selected file for non-partition columns.
6. Emit rows with `year` and `month` populated from the path values.

## Source and Table Recognition

### Definitions

Configured source location:

- the adapter setting chosen by the user
- can point to a single `.parquet` file, a table root directory, or a dataset root directory

Table root:

- the directory that represents one logical table
- all Parquet files below this root belong to that table unless excluded by discovery rules
- partition directories, when present, are below the table root

Partition root:

- the first `key=value` directory below the table root
- partition columns are derived from `key=value` directories under the table root, not above it

Example:

```text
dataset/
  yellow_tripdata/
    year=2025/
      month=01/
        yellow_tripdata_2025-01.parquet
```

Here:

- configured source can be `dataset/` or `dataset/yellow_tripdata/`
- table root is `dataset/yellow_tripdata/`
- partition root is `dataset/yellow_tripdata/year=2025/`
- partition columns are `year`, then `month`

### File Naming

Files such as:

```text
yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
```

are valid source files for the same table when they live under the same folder and their schemas consolidate:

```text
yellow_tripdata/
  yellow_tripdata_2025-01.parquet
  yellow_tripdata_2025-02.parquet
```

The adapter should not rely on filename prefixes as the main grouping rule. Schema compatibility is the deciding rule for direct files in one folder.

## Logic Flow

1. File/table discovery (ParquetFileDiscovery.discoverTables(...)) checks:
- one Parquet file
- folder with several Parquet files
- partitioned folder
- dataset root with several table folders

2. Schema creation:
- ParquetSchemaReader reads Parquet metadata from the discovered files and builds common schema.
- Before: one file -> one schema; Now: many files -> one common schema

3. Adapter discovery metadata
- ParquetFileDiscovery creates DiscoveredTable and DiscoveredTableBinding.
- This metadata describes table name, columns, source files, partition values, and column paths.

4. Logical table creation.
- Adapter returns columns through getExportedColumns().
- Polypheny creates the logical table in its catalog.


5. Physical tables creation. ParquetRelationalSource.createTable() uses the discovered metadata.


6. Add partitions on Polypheny level. ParquetRelationalSource.createPartitionedTable() adds needed information to catalog.


7. Query planning happens. ParquetRelScan called. Filters and projections can be pushed into this scan.


8. Runtime scan happens. ParquetRelTable:
- reads metadata
- applies partition pruning: ParquetSourceFileFilterEvaluator.prune(...) - returns only the files that can match the partition filter
- and reads only needed files: creates ParquetMultiFileEnumerator

9. Statistics updated
- A logical column can now be spread across several physical Parquet files, and also across several Polypheny physical partitions.
- During statistics discovery, we may need to inspect all existing files

## File/table discovery 
### 1. one Parquet file
```text
yellow_tripdata/
  yellow_tripdata_2025-01.parquet
```
one table backed by one file (old functionality)

### 2. folder with several Parquet files
```text
yellow_tripdata/
  yellow_tripdata_2025-01.parquet
  yellow_tripdata_2025-02.parquet
  yellow_tripdata_2025-03.parquet
```
There are no key=value partition folders here.

Result:
- one logical table
- one physical table
- many Parquet files in the binding

This is new functionality compared to old one-file behavior.

many monthly files -> one table

So the user can query:

```text
select *
from yellow_tripdata
```

and get rows from all files.

### 3. partitioned folder

```text
yellow_tripdata/
  year=2025/
    month=01/
      yellow_tripdata_2025-01.parquet
    month=02/
      yellow_tripdata_2025-02.parquet
  year=2026/
    month=01/
      yellow_tripdata_2026-01.parquet
```

Here the folders have this form: key=value

Result:
- one logical table
- physical tables per year (Polypheny supports only one partitioning level)
  - tw0 physical tables in this example
- many Parquet files in bindings
- partition columns exposed as normal columns

`month` is still stored in file metadata and exposed as a column, but it is not used to create Polypheny physical partitions in current implementation.
Adapter can skip files by month inside the physical table binding

### 4. dataset root with several table folders. Root directory contains multiple partitioned tables
```text
tlc/
    yellow_tripdata/
        year=2025/
            month=01/
                yellow_tripdata_2025-01.parquet

    green_tripdata/
        year=2025/
            month=01/
                green_tripdata_2025-01.parquet

    fhv_tripdata/
        year=2025/
            month=01/
                fhv_tripdata_2025-01.parquet
```
Each child folder is treated as a separate table:

- yellow_tripdata -> table
- green_tripdata  -> table
- fhv_tripdata    -> table

Result:

- multiple logical tables
  - logical table: yellow_tripdata
  - logical table: green_tripdata
  - logical table: fhv_tripdata
  
- multiple physical tables 
- multiple Parquet files
