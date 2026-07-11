# File Pruning

File pruning avoids opening Parquet files that cannot satisfy the pushed
adapter filters. It works for both normal scans and aggregate execution.

## Stored File Metadata

Each physical file is represented by `ParquetSourceFile`:

```text
fileUrl
partitionValues
columnStatistics
```

`partitionValues` come from Hive-style `key=value` folders. `columnStatistics`
are read by `ParquetColumnStatisticsReader` from Parquet footer metadata when
the source file is discovered.

`ParquetTableBinding` stores the list of `ParquetSourceFile` objects and maps
logical physical columns to `ParquetColumnBinding` instances. A column binding
knows whether the column is real Parquet data, a partition value, or a synthetic
normalized-schema column.

## Evaluators

Current source-file pruning uses:

- `ParquetSourceFilePartitionFilterEvaluator`: evaluates filters over partition
  columns using `ParquetSourceFile.partitionValues()`
- `ParquetSourceFileStatisticsFilterEvaluator`: evaluates filters over data
  columns using per-file `ParquetColumnStatistics`
- `ParquetMultiFilterEvaluator`: combines evaluator results
- `ParquetSourceFileFilterReducer`: rejects impossible files and returns
  residual filters still needed at row level

All source-file evaluators return true, false, or unknown. Unknown means the
file is kept.

## Scan Flow

1. Planner rules attach supported filters to `ParquetRelScan`.
2. `ParquetRelTable.project(...)` resolves dynamic parameters and physical
   column bindings through `ParquetFilterResolver`.
3. `ParquetRelExecutorsFactory` creates a `ParquetMultiFileEnumerator`.
4. For each `ParquetSourceFile`, the file evaluator checks partition values and
   footer statistics.
5. If a file is proven false, the enumerator skips it without opening a reader.
6. If a file is proven true for some filters, those filters can be removed from
   the residual row-level filter list for that file.
7. Remaining filters are passed to `ParquetSourceReader` for possible native
   filtering and to the enumerator for exact row-level evaluation.

## Aggregate Flow

`ParquetDataAggregateExecutor` and `ParquetMetadataAggregateExecutor` use the
same partition/statistics file-evaluator chain. This allows aggregate plans to:

- skip files rejected by partition or footer metadata
- answer file-constant groups from file metadata
- keep residual filters when a file-level evaluator cannot prove the result

## What Can Be Pruned

File pruning is strongest when predicates target:

- partition columns such as `year` or `month`
- columns with reliable footer min/max or null-count metadata
- constant-per-file physical columns that can be proven from statistics

The adapter keeps files for:

- unsupported operators
- unsupported logical combinations
- missing or unreliable statistics
- repeated/nested paths that cannot be decided at file level
- predicates involving values only known after row expansion
