# Statistics

The Parquet adapter provides metadata-backed statistics so Polypheny does not
need to scan Parquet rows during normal statistics reevaluation when footer
metadata is sufficient.

## Core Interfaces

Core statistics types:

- `AdapterStatisticsProvider`
- `ProvidedEntityStatistics`
- `ProvidedColumnStatistics`

`ParquetRelTable` implements `AdapterStatisticsProvider` and delegates to
`ParquetTableStatisticsReader`.

Current Parquet statistics classes:

- `ParquetTableStatisticsReader`
- `ParquetColumnStatisticsReader`
- `ParquetColumnStatistics`
- `ParquetSourceFile`

## Discovery-Time Statistics

`ParquetSourceFile.of(...)` reads footer statistics with
`ParquetColumnStatisticsReader.readAll(...)` and stores the result on the
source-file record.

The stored statistics are keyed by Parquet source path:

```text
["total_amount"]
["shipping_address", "country"]
```

This path-based keying is important because flat mode can expose non-repeated
nested fields under normalized column names while the physical Parquet path
remains nested.

## Entity Statistics

`ParquetRelTable.getEntityStatistics(logicalEntityId)` returns statistics only
for the matching logical table.

For root tables, `ParquetTableStatisticsReader` estimates row count from source
file metadata. For nested generated tables, it estimates row count from the
largest value count among data-column bindings. If nested value counts are not
available, it falls back to the root row-count estimate.

## Column Statistics

`ParquetRelTable.getColumnStatistics(column, uniqueValueLimit)` returns
statistics only when the logical column belongs to the table.

`ParquetTableStatisticsReader.getColumnStatistics(...)` handles:

- data columns with footer metadata
- partition columns derived from folder values
- synthetic normalized columns
- missing or unreliable statistics

For data columns, the reader aggregates per-file `ParquetColumnStatistics`:

- row count
- value count
- null count when available
- min
- max
- range reliability

Values are converted to compatible `PolyValue` instances through
`ParquetTypeConverter`.

## Partition Column Statistics

Partition columns are not physical Parquet columns. Their statistics are derived
from `ParquetSourceFile.partitionValues()`.

For partition columns, the reader can provide:

- estimated count
- lexical min/max over partition values
- bounded unique values if the number of distinct values does not exceed
  `uniqueValueLimit`

## Synthetic Columns

Synthetic normalized columns include:

- `__polypheny_row_id`
- `__polypheny_parent_row_id`
- `__polypheny_elem_ordinal`

They are generated during scan and do not have Parquet footer statistics. The
statistics provider returns safe fallback statistics for them, such as estimated
count with no min/max range.

## Fallback Behavior

The adapter returns metadata-backed statistics when it can. If no adapter
statistics provider is available for an entity or column, Polypheny's monitoring
statistics subsystem can fall back to its normal query-based path.

When a Parquet statistic is partially unavailable or unreliable, the adapter
keeps the result conservative:

- count may fall back to estimated row count
- min/max may be `PolyNull.NULL`
- unique values may be empty
- unknown statistics never justify dropping rows or files

## Related Runtime Use

The same per-file statistics also support query execution:

- `ParquetSourceFileStatisticsFilterEvaluator` uses min/max/null metadata for
  file pruning.
- `ParquetMetadataAggregateExecutor` uses footer statistics for exact metadata
  aggregates when possible.
