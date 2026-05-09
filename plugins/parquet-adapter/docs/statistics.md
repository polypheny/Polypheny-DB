# Parquet Statistics

This document describes the statistics-related changes introduced to avoid loading Parquet data eagerly during statistics reevaluation.

## Goal

Before this change, the monitoring statistics subsystem computed table and column statistics by executing normal relational queries.
For Parquet-backed entities, this could trigger full scans during startup statistics reevaluation.

The new goal is:

1. ask the adapter for metadata-backed statistics first
2. use Parquet footer metadata when available
3. fall back to the old query-based path only when necessary

## Main Objects

### `AdapterStatisticsProvider`

File:

`core/src/main/java/org/polypheny/db/adapter/statistics/AdapterStatisticsProvider.java`

Responsibilities:

- generic adapter-side hook for statistics
- allows an adapter to provide:
  - entity statistics
  - column statistics

Methods:

- `getEntityStatistics(long logicalEntityId)`
- `getColumnStatistics(LogicalColumn column, int uniqueValueLimit)`

Default behavior:

- returns `Optional.empty()`
- meaning the monitoring subsystem must fall back to query execution

### `ProvidedEntityStatistics`

File:

`core/src/main/java/org/polypheny/db/adapter/statistics/ProvidedEntityStatistics.java`

Responsibilities:

- immutable return type for entity-level statistics

Fields:

- `rowCount`
  - row count, or `null` if unavailable

### `ProvidedColumnStatistics`

File:

`core/src/main/java/org/polypheny/db/adapter/statistics/ProvidedColumnStatistics.java`

Responsibilities:

- immutable return type for column-level statistics

Fields:

- `count`
  - non-null value count, or `null` if unavailable
- `min`
  - minimum value, or `null` if unavailable
- `max`
  - maximum value, or `null` if unavailable
- `uniqueValues`
  - bounded unique values, empty if unavailable
- `full`
  - whether the unique value list is complete

## Monitoring Integration

### `StatisticsManagerImpl`

File:

`monitoring/src/main/java/org/polypheny/db/monitoring/statistics/StatisticsManagerImpl.java`

Responsibilities after the change:

- still drives startup statistics reevaluation
- now asks physical entities for adapter-provided statistics before running query-based statistics

New behavior:

- `reevaluateRowCount()`
  - first calls `getEntityStatistics(...)`
  - if a provider returns `rowCount`, it uses that value directly
  - otherwise it falls back to the old `ROW_COUNT_TABLE` query

- `reevaluateField(...)`
  - first calls `getColumnStatistics(...)`
  - if a provider returns column statistics, it converts them into the existing `StatisticColumn` model
  - otherwise it falls back to the old query-based path

Helper methods added:

- `getEntityStatistics(long logicalEntityId)`
- `getColumnStatistics(QueryResult column)`
- `getStatisticsProvider(AllocationEntity allocation)`
- `toStatisticColumn(QueryResult column, ProvidedColumnStatistics provided)`


## Parquet Adapter Integration

### `ParquetRelTable`

File:

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/relational/schema/ParquetRelTable.java`

Responsibilities after the change:

- still exposes Parquet-backed relational tables to Polypheny
- now also implements `StatisticsProvider`

New fields:

- `schemaReader`
  - reusable Parquet footer/schema reader
- `statisticsReader`
  - Parquet-specific metadata statistics reader

New behavior:

- `getEntityStatistics(long logicalEntityId)`
  - returns entity statistics only for the matching logical table
  - delegates to `statisticsReader.getEntityStatistics(isNestedTable())`

- `getColumnStatistics(LogicalColumn column, int uniqueValueLimit)`
  - returns column statistics only for columns of the matching logical table
  - delegates to `statisticsReader.getColumnStatistics(column, uniqueValueLimit)`

This makes Parquet physical entities visible to the monitoring subsystem as statistics providers.

### `ParquetSchemaReader`

File:

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/io/ParquetSchemaReader.java`

Responsibilities:

- reads the Parquet footer once
- stores reusable schema and metadata

Provides:

- `getSchema()`
  - Parquet schema
- `getFooter()`
  - full Parquet footer metadata
- `getEstimatedRowCount()`
  - sum of row-group row counts

Purpose:

- centralize footer access for statistics logic
- avoid reopening or rescanning data when only metadata is needed

### `ParquetStatisticsReader`

File:

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/statistics/ParquetStatisticsReader.java`

Responsibilities:

- provide entity and column statistics from Parquet metadata
- use table/column bindings to map logical columns to Parquet source paths

Main methods:

- `getEntityStatistics(boolean nestedTable)`
- `getColumnStatistics(LogicalColumn column, int uniqueValueLimit)`



## Full Flow

### Step 1: Startup statistics reevaluation begins

`StatisticsManagerImpl.initializeStatisticSettings()` still triggers:

```text
asyncReevaluateAllStatistics()
```

when startup statistics are enabled.

### Step 2: Monitoring reevaluates row count and columns

During reevaluation:

- `reevaluateRowCount()` handles table row counts
- `reevaluateField(...)` handles per-column statistics

### Step 3: Monitoring asks the physical entity for statistics

For each logical entity or column, `StatisticsManagerImpl`:

- resolves allocations from the logical table
- resolves physical entities from the adapter catalog
- checks whether the physical entity unwraps to `StatisticsProvider`

If yes:

- entity statistics are requested through `getEntityStatistics(...)`
- column statistics are requested through `getColumnStatistics(...)`

### Step 4: ParquetRelTable answers the request

If the physical entity is a Parquet relational table:

- `ParquetRelTable` verifies the logical table or column belongs to it
- delegates the request to `ParquetStatisticsReader`

### Step 5: ParquetStatisticsReader reads footer metadata

`ParquetStatisticsReader` uses:

- `ParquetSchemaReader.getFooter()`
- `ParquetSchemaReader.getSchema()`
- `ParquetTableBinding`
- `ParquetColumnBinding`

to map the logical request to physical Parquet metadata.

### Step 6: Monitoring uses provider statistics or falls back

If a provider result is present:

- `StatisticsManagerImpl` converts it into the existing monitoring statistics model

If not:

- it falls back to the old query-based path through `StatisticQueryProcessor`

## Entity Statistics

### Root table row count

For a root Parquet table:

```text
row count = sum of row-group row counts
```

Implementation:

- `ParquetSchemaReader.getEstimatedRowCount()`

This comes directly from the Parquet footer and does not require row scans.

### Nested table row count

For a generated nested table:

```text
row count != root row count
```

because one root row can produce multiple nested rows.

Implementation:

- `ParquetStatisticsReader.getEntityStatistics(true)`
  - uses `estimateNestedRowCount()`

### `estimateNestedRowCount()`

Purpose:

- estimate how many rows a normalized nested table will expose

Logic:

1. iterate over all column bindings of the table
2. keep only `DATA` columns with non-empty Parquet source paths
3. call `estimateValueCount(...)` for each source path
4. take the maximum value count across those data columns
5. if no usable data column exists, fall back to root row count

Reason for using the maximum:

- data columns of the same generated nested table should usually have the same number of values
- the maximum is used as a robust estimate of the nested row count

### `estimateValueCount()`

Purpose:

- estimate how many values exist for one Parquet leaf path across the whole file

Logic:

1. iterate over all Parquet row groups
2. find the matching column chunk for the requested Parquet path
3. sum `column.getValueCount()` across all row groups
4. if the column is missing in any block, return `0`

This gives a metadata-based estimate for:

```text
how many values does this Parquet column contain?
```

## Column Statistics

### What `getColumnStatistics()` provides

For a logical column, `ParquetStatisticsReader.getColumnStatistics()` tries to provide:

- `count`
- `min`
- `max`
- `uniqueValues`
- `full`

The result is returned as `ProvidedColumnStatistics`.

### Resolution flow

1. resolve the `ParquetColumnBinding` for the logical column id
2. verify the column is a real `DATA` column
3. verify the binding contains a Parquet source path
4. resolve the primitive Parquet schema type for that path
5. read row-group metadata for that exact column path
6. convert metadata values into `PolyValue`

### Metadata source

The actual metadata aggregation happens in:

- `readColumnMetadataStatistics(List<String> sourcePathElements)`

It aggregates across all row groups:

- row count
- null count when available
- minimum non-null value
- maximum non-null value

### Count semantics

If Parquet null statistics are reliable:

```text
count = rowCount - nullCount
```

Otherwise:

```text
count = rowCount
```

So the count is best understood as:

```text
best available non-null count estimate from footer metadata
```

### Min/max semantics

If row-group min/max metadata is available and compatible with the logical Polypheny type:

- `min` is returned
- `max` is returned

If metadata is missing, unreliable, or conversion fails:

- `min = null`
- `max = null`

### Type conversion

`toStatisticValue(...)` converts Parquet metadata values into `PolyValue`.

Currently supported families in this conversion path:

- numeric
- temporal
- character

## Supported and Unsupported Cases

### Supported well

- root table row count from footer metadata
- nested table row-count estimation from value counts
- count/min/max for real Parquet data columns when metadata is available
- logical-column to Parquet-path mapping through table bindings

### Supported with fallback behavior

- columns whose metadata exists but min/max cannot be converted cleanly
- columns with missing or unreliable null statistics
- nested tables whose row count must be estimated from leaf value counts

### Not provided directly from Parquet metadata

- bounded unique value lists
- exact distinct counts
- fully semantic statistics for synthetic columns

For these cases, the provider currently returns:

- `uniqueValues = []`
- `full = true`
- and may return count-only fallback values with `min = null`, `max = null`

## Synthetic and Non-Data Columns

If a logical column:

- has no binding
- is not a `DATA` column
- has an empty source path

then `ParquetStatisticsReader.getColumnStatistics()` does not try to read Parquet leaf metadata for it.

Instead it returns a safe fallback:

- estimated count
- `min = null`
- `max = null`
- empty unique values

This is important for synthetic normalized columns such as:

- `__polypheny_row_id`
- `__polypheny_parent_row_id`
- `__polypheny_elem_ordinal`

## Summary

The statistics changes introduce a metadata-first path for Parquet-backed entities:

- a new generic `StatisticsProvider` hook in core
- monitoring-side integration in `StatisticsManagerImpl`
- Parquet-side footer readers in `ParquetSchemaReader` and `ParquetStatisticsReader`
- fallback to the old `StatisticQueryProcessor` path when metadata is unavailable

This reduces unnecessary Parquet row scans during startup statistics reevaluation, especially for:

- table row counts
- column count/min/max

while preserving compatibility with the existing monitoring statistics subsystem.
