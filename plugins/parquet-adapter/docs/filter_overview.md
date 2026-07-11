# Filter Overview

Filtering is represented once, as immutable `ParquetAdapterFilter` trees, and
then used by several layers:

1. planner translation from Rex predicates
2. source-file pruning before opening a Parquet file
3. optional native Parquet predicate pushdown in `ParquetSourceReader`
4. exact adapter-level row filtering in enumerators

Native Parquet filtering is an optimization. Exact adapter-level filtering and
residual-filter handling preserve correctness when native filtering cannot be
applied.

## Filter Representation

`ParquetAdapterFilter<T>` represents two shapes:

- leaf filter: column index, optional Parquet path, operator, literal value, or
  dynamic parameter index
- logical filter: `AND`, `OR`, or `NOT` with child filters and `columnIndex = -1`

`FiltersContainer` separates adapter filters from native reader filters.
`JoinFiltersContainer` extends this model with parent-only and child-only
filters for nested joins. `JoinFiltersSplitter` splits joined-row filters across
parent, child, native-reader, and residual joined-row evaluation paths.

## Relational Translation

`ParquetRelFilterTranslator` translates relational Rex predicates when the
predicate can be represented by the adapter.

Supported forms:

- `column OP literal`
- `column OP dynamicParameter`
- `literal OP column`
- `dynamicParameter OP column`
- casts around supported operands
- `column IS NULL`
- `column IS NOT NULL`
- `condition AND condition`
- `condition OR condition`
- `NOT condition`
- `column IN (value, ...)`

Supported binary operators:

- `=`
- `!=`
- `>`
- `>=`
- `<`
- `<=`

Supported relational type/operator combinations:

| Polypheny type | Supported operators |
| --- | --- |
| `BOOLEAN` | `=`, `!=`, `IS NULL`, `IS NOT NULL` |
| `VARCHAR`, `CHAR`, `TEXT` | `=`, `!=`, `IS NULL`, `IS NOT NULL` |
| `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DATE`, `TIME`, `TIMESTAMP` | `=`, `!=`, `>`, `>=`, `<`, `<=`, `IS NULL`, `IS NOT NULL` |

`IN` is translated as an `OR` tree of equality filters. The first operand must
be a column reference, all values must be supported literal or dynamic
parameters, and null values are rejected.

If any child of a logical filter cannot be translated, the whole logical filter
is rejected by the translator and remains outside the adapter-specific plan.

## Document Translation

`ParquetDocFilterTranslator` translates document predicates over exported
top-level fields.

Supported field references:

- a single-name `RexNameRef`
- lowered MQL field access:
  `MQL_QUERY_VALUE(document, ARRAY['field'])`

Supported document forms:

- `field OP literal`
- `field OP dynamicParameter`
- reversed value/field comparisons
- casts around supported operands
- `AND`, `OR`, and `NOT` when all child predicates are translatable

Document translation does not support:

- `IS NULL` or `IS NOT NULL`
- `IN`
- nested document paths such as `address.city`
- field-to-field comparisons
- arithmetic expressions or functions as the field side
- predicates on fields that are not exported Parquet columns

Document filters are stored on `ParquetDocScan`; there is no separate document
filter planner node.

## Native Parquet Filtering

`ParquetNativeFilterBuilder` converts supported `ParquetAdapterFilter` objects
into Parquet native predicates.

Native pushdown can be built for:

- top-level primitive columns addressed by column index
- non-repeated primitive nested paths addressed by `pathElements`
- logical `AND`, `OR`, and `NOT` when every child can become a native predicate
- supported null checks on primitive fields

Native pushdown is skipped for:

- group fields
- repeated paths
- unresolved paths
- unsupported physical primitive/operator combinations
- native `INT96` timestamp cases that Parquet cannot safely filter
- logical filters with a child that cannot become native

For a top-level list of filters, filters that cannot become native predicates
are skipped; native predicates that remain are combined with `AND`.

## Source-File Pruning

Before opening each file, `ParquetMultiFileEnumerator` and aggregate executors
use a `ParquetMultiFilterEvaluator<ParquetSourceFile>` composed from:

- `ParquetSourceFilePartitionFilterEvaluator`
- `ParquetSourceFileStatisticsFilterEvaluator`

`ParquetSourceFileFilterReducer` removes filters already proven true for a file,
rejects files proven false, and keeps residual filters for row-level evaluation.

See [File Pruning](file_pruning.md) for details.

## Row-Level Evaluation

Exact row filtering runs after rows are read or expanded:

- flat primitive rows: `ParquetPrimitiveValueFilterEvaluator`
- Parquet groups: `ParquetGroupFilterEvaluator`
- partition-aware rows: `ParquetPartitionAwareFilterEvaluator`
- normalized nested tables: `ParquetNestedFilterEvaluator`
- parent/child joins: `ParquetNestedJoinFilterEvaluator`

`ParquetFilterEvaluator` is tri-state. Unknown evaluations keep the row or file.
That behavior is intentional for pruning and residual evaluation because the
adapter must not drop data unless it can prove a predicate is false.
