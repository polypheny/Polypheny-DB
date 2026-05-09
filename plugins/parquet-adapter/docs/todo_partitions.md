# Partitioning Issues

## Partitioned Join Flow

Adapter-level joins currently depend on both sides having the same backing source-file set. Polypheny catalog partitioning splits a root table into several physical allocations, while generated child tables can still describe the full file set. Because of that, a valid parent-child join over partitioned data may not be selected by the Parquet join rule, or it may need a more precise source compatibility check.

Partition columns also need explicit handling in the nested join runtime. Root scans materialize folder-derived partition values through `ParquetNestedNonRepeatedRelEnumerator`, but `ParquetPathValueExtractor` still returns `NULL` for `PARTITION`. If a partition column is projected or filtered through the nested join path, the join result can lose that value.

## Restore Flow

`ParquetRelationalSource.restoreTable` restores only the first physical entity from the provided entity list. A partitioned table can have multiple physical entities, so restore should be checked to make sure every partition allocation is re-registered.

## Polypheny Partition Model

Only the first discovered partition column is mapped to Polypheny catalog partitioning. This is a current Polypheny-side limitation and is not planned to be extended now. Multi-level folders such as `year=2025/month=01` can still be exposed as adapter partition columns, but only one column should drive the catalog partition layout.

## Partition Names

Generated partition names are normalized from `column_value`. Different raw values can normalize to the same name, for example `a-b`, `a_b`, and case-only variants. The creation flow should eventually add collision handling.

## Schema Consolidation

Schema consolidation was made stricter by requiring at least `0.8` column-name similarity between files. This reduces accidental grouping of unrelated files. If schema evolution support grows, the threshold should probably become a named constant or adapter setting and should be covered by tests for missing-column null materialization.

