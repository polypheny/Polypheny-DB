# Parquet Filter Overview

In particular, the current relational planner path is built around `ParquetConvention`, `ParquetRules`, `PatternMatchers`, `ParquetScan`, and `ParquetJoin`; older `ParquetRelScan` / `ParquetRelJoin` rule classes still exist, but their direct rule registration in `ParquetRelScan.registerRules()` is commented out.

## 1. Short Description

Filtering is handled in three layers:

1. Planner translation
   - Polypheny `RexNode` filter expressions are translated into `ParquetAdapterFilter` objects when the adapter knows how to represent them.
   - Unsupported filters are not attached to the Parquet adapter plan.

2. Native Parquet reader filtering
   - `ParquetNativeFilterBuilder` converts supported adapter filters into Parquet `FilterPredicate` objects.
   - `ParquetSourceReader` installs the resulting predicate into `ParquetReadOptions`.
   - This can use Parquet statistics, dictionary, column index, bloom, and record filters.
   - Native filtering is an optimization. Some adapter filters cannot become native filters.

3. Adapter-level row filtering
   - `AbstractParquetEnumerator` applies adapter filters after reading a Parquet row and after expanding nested rows.
   - Evaluators such as `ParquetGroupFilterEvaluator`, `ParquetNestedFilterEvaluator`, and `ParquetNestedJoinFilterEvaluator` decide whether the emitted row should be kept.
   - Partition values are handled specially because they may come from folder names rather than physical Parquet columns.

The central representation is `ParquetAdapterFilter`. It can represent:

- a leaf filter: column index, optional Parquet path, operator, literal value, or dynamic parameter index
- a logical filter tree: `AND`, `OR`, or `NOT` with child filters

## 2. Full Flow

### Relational Scan Flow

1. A relational Parquet table is exposed as `ParquetScan`.
   - `ParquetRelationalSource.getRelScan()` registers `FilterSetOpTransposeRule` and the Parquet convention rules.
   - It returns a `ParquetScan` over a `ParquetRelTable`.

2. `ParquetConvention` registers `ParquetRules`.
   - The active rule list is created in `ParquetRules.rules()`.
   - The rule bodies live in `PatternMatchers`.

3. Planner rules attach filters to `ParquetScan`.
   - `attachFilterToScanUnderLogicalRelFilter`
   - `attachFilterToScanUnderEnumerableFilter`
   - `attachProjectedFieldsAndFilterToScanUnderCalc`
   - Each rule calls `ParquetRelFilterTranslator.translate(...)`.
   - If translation fails, the rule does not transform the plan.

4. `ParquetScan` stores translated filters.
   - It stores the projected field indexes.
   - It stores zero or more `ParquetAdapterFilter` objects.
   - During enumerable implementation it calls `ParquetRelTable.project(dataContext, fields, filters)`.

5. Runtime filter resolution happens in `ParquetRelTable.project(...)`.
   - Dynamic parameters are replaced with runtime values from `DataContext`.
   - Projected output column indexes are mapped to `ParquetColumnBinding`.
   - Each leaf filter receives the real Parquet `sourcePathElements` from the binding.

6. Source-file pruning runs before reading.
   - `ParquetSourceFileFilterEvaluator` checks filters against partition values stored in `ParquetSourceFile`.
   - If a filter targets a partition column and evaluates to false for a source file, that file is skipped.
   - If the evaluator cannot decide, the file is kept.

7. `ParquetSourceReader` is created for each source file.
   - It receives the projected fields when a direct projection scan is safe.
   - It reads the full root schema when a binding/path scan is needed.
   - It builds a native Parquet filter from `filtersContainer.nativeFilters()`.

8. The relational enumerator applies exact adapter-level filtering.
   - `ParquetRelEnumerator` is used for direct flat scans.
   - `ParquetNestedNonRepeatedRelEnumerator` is used when values must be read by binding/path but one root row still maps to one output row.
   - `ParquetNestedRepeatedRelEnumerator` is used for generated nested repeated tables.
   - `AbstractParquetEnumerator.accept(...)` applies adapter filters to each row that may be emitted.

### Relational Join Flow

1. Join patterns in `PatternMatchers` detect supported joins over Parquet scans.
   - The current active join node is `ParquetJoin`.
   - It is used for supported normalized parent/child joins.

2. `ParquetJoin.create(...)` merges filters already attached to left and right input scans.
   - Right-side scan filters are shifted so their column indexes match the joined output row.

3. Filters on join inputs can be carried into `ParquetJoin`.
   - `ParquetJoin.create(...)` merges filters that were already attached to the left and right `ParquetScan` inputs.
   - In the current convention path, join matching and scan-side filter attachment are handled through `PatternMatchers`.
   - I did not find an active `ParquetJoin`-specific rule that attaches filters located above an already-created `ParquetJoin`.
   - Older `ParquetEnumerableFilterJoinRule` and `ParquetEnumerableCalcJoinRule` classes exist for the older `ParquetRelJoin` path, but that direct rule registration is not active in the current `ParquetRelScan.registerRules()` implementation.

4. Runtime execution calls `ParquetRelTable.nestedJoin(...)`.
   - Dynamic parameters are resolved.
   - Column indexes are mapped to parent or child `ParquetColumnBinding` objects.
   - Parent source files may be pruned by partition filters.

5. `JoinFiltersSplitter` splits filters into:
   - parent filters
   - child filters
   - adapter filters over the joined row
   - native reader filters

6. `ParquetNestedJoinEnumerator` applies the split filters.
   - Parent filters are evaluated before child rows are expanded.
   - Child filters are evaluated after child rows are expanded.
   - Adapter filters are evaluated on the combined parent/child row.
   - Parent filters with usable Parquet paths may also become native reader filters.

### Document Flow

1. `ParquetDocScan.register(...)` registers `ParquetDocFilterRule`.

2. `ParquetDocFilterRule` matches a `LogicalRelFilter`.
   - It splits only `AND` conjunctions into individual predicates.
   - Each predicate is translated by `ParquetDocFilterTranslator`.
   - If any predicate cannot be translated, the rule does not transform the plan.

3. The rule creates a `ParquetDocScan` containing translated filters.
   - It wraps the scan in `ParquetDocFilter`.
   - `ParquetDocFilter.implement(...)` delegates directly to the underlying `ParquetDocScan`.

4. `ParquetDocScan.implement(...)` calls `ParquetDocument.scanFiltered(...)`.

5. `ParquetDocument.scanFiltered(...)` resolves dynamic parameters.
   - It opens `ParquetSourceReader` with the filters as native and adapter filters.
   - It does not project fields; document scans read the row as a document shape.

6. `ParquetDocEnumerator` filters and converts rows.
   - `AbstractParquetEnumerator` applies adapter-level filters.
   - Accepted Parquet `Group` rows are converted into a single `PolyDocument`.

## 3. Existing Filter Types and Classes

### Filter Representation

- `ParquetAdapterFilter`
  - Immutable adapter filter representation.
  - Stores `columnIndex`, `pathElements`, `operator`, `polyValue`, optional `dynamicParamIndex`, and logical `operands`.
  - Logical filters use operator `AND`, `OR`, or `NOT` and have `columnIndex = -1`.

- `FiltersContainer`
  - Holds two filter lists:
    - adapter filters for exact row-level evaluation
    - native filters for `ParquetSourceReader`
  - `shared(...)` uses the same list for both layers.
  - `withoutPathElementsInAdapterFilters()` keeps paths for native filters but removes them from adapter filters. This is used when adapter evaluation should use projected column indexes.

- `JoinFiltersContainer`
  - Extends `FiltersContainer`.
  - Adds parent-only and child-only filter lists for nested join execution.

- `JoinFiltersSplitter`
  - Splits joined-row filters into parent, child, adapter, and native reader filters.
  - Splits top-level `AND` filters into individual child filters.
  - Keeps cross-side `OR` and other mixed-side logical filters as adapter-level joined-row filters.

### Translators

- `AbstractFilterTranslator`
  - Shared parser for simple binary predicates.
  - Supports `=`, `!=`, `>`, `>=`, `<`, `<=`.
  - Unwraps casts.
  - Allows the value to appear on the left side by reversing the comparison.
  - Converts literals and dynamic parameters into `ParquetAdapterFilter` leaves.

- `ParquetRelFilterTranslator`
  - Relational translator.
  - Supports simple comparisons, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`, and `IN`.
  - Validates column indexes, Polypheny data types, and operator support.
  - Translates `IN` into an `OR` tree of equality filters.

- `ParquetDocFilterTranslator`
  - Document translator.
  - Supports only simple single-field comparisons against exported document columns.
  - Uses a case-insensitive top-level field name match.
  - Does not translate document `OR`, `NOT`, `IN`, or null checks.

### Native Filter Layer

- `ParquetNativeFilterBuilder`
  - Converts `ParquetAdapterFilter` objects into Parquet native `FilterPredicate` objects.
  - Supports top-level primitive fields and non-repeated primitive paths.
  - Rejects group fields, repeated paths, unresolved paths, and unsupported operator/type combinations.
  - If no native predicate can be built, returns `FilterCompat.NOOP`.

- `ParquetSourceReader`
  - Opens a Parquet file.
  - Builds projection schema.
  - Installs the native filter into `ParquetReadOptions`.
  - Reads filtered row groups when a native filter exists.

### Adapter-Level Evaluators

- `FilterEvaluator<C>`
  - Base tri-state evaluator.
  - Returns true, false, or null.
  - `matches(...)` treats null as true, so uncertain pruning/evaluation keeps the data.
  - Handles logical `AND`, `OR`, and `NOT`.

- `ParquetGroupFilterEvaluator`
  - Evaluates filters against a Parquet `Group`.
  - Extracts values by column index or path.
  - Handles missing values through `PolyNull.NULL`.

- `ParquetPartitionAwareFilterEvaluator`
  - Extends group filtering with partition-value awareness.
  - Used when a row has virtual partition columns from folder names.

- `ParquetNestedFilterEvaluator`
  - Evaluates filters for nested repeated generated tables.
  - Uses `ParquetPathValueExtractor` and column bindings.

- `ParquetNestedJoinFilterEvaluator`
  - Evaluates filters on joined parent/child rows represented as `CombinedGroup`.
  - Also evaluates parent-only filters before child expansion.
  - Current caveat: its `canApplyFilter(...)` rejects leaf filters whose `polyValue` is null, so joined-row null checks are not enforced by this evaluator except where native parent reader filtering can apply.

- `ParquetSourceFileFilterEvaluator`
  - Prunes `ParquetSourceFile` objects before reading.
  - Only decides filters that target partition columns.
  - Unknown filters keep the file.

### Planner and Runtime Nodes

- `ParquetConvention`
  - Registers current Parquet convention rules.

- `ParquetRules` and `PatternMatchers`
  - Current active relational planning path for attaching filters/projections and replacing supported joins.

- `ParquetScan`
  - Current relational scan node in Parquet convention.
  - Carries projected fields and filters.

- `ParquetJoin`
  - Current Parquet convention join node for supported parent/child joins.
  - Carries joined-row filters.

- `ParquetRelTable`
  - Runtime table wrapper.
  - Resolves dynamic parameters.
  - Maps filter column indexes to `ParquetColumnBinding` and Parquet paths.
  - Prunes source files and selects the correct enumerator.

- `AbstractParquetEnumerator`
  - Shared runtime loop.
  - Expands rows when needed.
  - Applies adapter-level filters before emitting output rows.

- `ParquetDocFilterRule`, `ParquetDocFilter`, `ParquetDocScan`
  - Document planning path for supported document filters.

- `ParquetDocument`
  - Runtime document wrapper.
  - Resolves dynamic parameters and creates `ParquetDocEnumerator`.

## 4. Currently Supported and Unsupported Filters

### Relational Filters Supported by Translation

Relational translation supports the following filter forms:

- `column OP literal`
- `column OP dynamicParameter`
- `literal OP column`
- `dynamicParameter OP column`
- casts around either side of the predicate
- `column IS NULL`
- `column IS NOT NULL`
- `condition AND condition`
- `condition OR condition`
- `NOT condition`
- `column IN (literalOrDynamicParameter, ...)`

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

`IN` is supported only when:

- the first operand is a column reference
- there is at least one value
- every value can be translated as an equality comparison
- none of the values is a null literal

Logical filters are supported only when every child filter can be translated.
If any child is unsupported, the whole logical filter is rejected by the translator.

### Relational Native Parquet Pushdown

Native Parquet pushdown is attempted for translated filters, but only a subset can become Parquet native predicates.

Native pushdown can be built for:

- top-level primitive columns addressed by column index
- non-repeated primitive nested paths addressed by `pathElements`
- logical `AND`, `OR`, and `NOT` when all operands can be converted to native predicates
- null checks on most primitive types

Native pushdown is not built for:

- group fields
- repeated paths
- unresolved paths
- unsupported physical primitive/operator combinations
- `INT96` null checks
- logical filters where any operand cannot become a native predicate

For a top-level list of filters, `ParquetNativeFilterBuilder` skips filters that cannot become native predicates and builds an `AND` of the predicates that remain.

### Relational Adapter-Level Filtering

Adapter-level filtering is applied after reading rows.
It supports the same `ParquetAdapterFilter` tree shape as translation, provided the evaluator can extract the target value.

Current exact-evaluation paths:

- flat direct rows: `ParquetRelEnumerator`
- non-repeated path/binding scans: `ParquetNestedNonRepeatedRelEnumerator`
- repeated nested generated tables: `ParquetNestedRepeatedRelEnumerator`
- nested parent/child joins: `ParquetNestedJoinEnumerator`
- partition source-file pruning: `ParquetSourceFileFilterEvaluator`
- partition row values: `ParquetPartitionAwareFilterEvaluator`

Important behavior:

- If an evaluator returns unknown, `matches(...)` treats it as keep.
- This is intentional for pruning, where uncertain data must not be dropped.
- It also means exact filtering depends on the selected evaluator being able to evaluate the filter.

### Document Filters Supported by Translation

Document filter support is narrower than relational filter support.

Supported document forms:

- `field OP literal`
- `field OP dynamicParameter`
- `literal OP field`
- `dynamicParameter OP field`
- casts around either side
- conjunctions with `AND`, but only because `ParquetDocFilterRule` splits `AND` into separate simple predicates

Supported document binary operators:

- `=`
- `!=`
- `>`
- `>=`
- `<`
- `<=`

Additional document constraints:

- the field side must be a `RexNameRef`
- the field reference must contain exactly one name
- the field name must match an exported column name case-insensitively
- the value side must be a literal or dynamic parameter
- null literals are rejected by `toParquetAdapterFilter`

Document native pushdown uses the same `ParquetNativeFilterBuilder`, so the native layer may still skip a translated document filter if the Parquet physical type/path is not suitable.
Adapter-level document filtering then runs in `ParquetDocEnumerator` before converting an accepted row into `PolyDocument`.

### Unsupported Relational Filters

The relational translator does not support:

- field-to-field comparisons
- arithmetic expressions as the column side
- functions as the column side
- unsupported Polypheny types outside the listed supported type set
- string-like ordered comparisons such as `text_col > 'x'`
- boolean ordered comparisons such as `flag > true`
- binary comparison against a null literal, such as `col = NULL`
- `IN` with zero values
- `IN` with a non-column first operand
- `IN` containing null or otherwise unsupported values
- logical `AND`, `OR`, or `NOT` if any child is unsupported
- `LIKE`
- `BETWEEN`
- regex predicates
- `IS TRUE` / `IS FALSE`
- subqueries
- arbitrary user-defined or built-in function predicates

### Unsupported Document Filters

The document translator does not support:

- `OR`
- `NOT`
- `IN`
- `IS NULL`
- `IS NOT NULL`
- nested document paths such as `address.city`
- multi-name `RexNameRef` references
- field-to-field comparisons
- arithmetic expressions
- functions
- null literal comparisons
- predicates on fields that are not in the exported column list

### Current Notes on Older Documentation

The existing filtering documents are likely outdated if they state that:

- relational filtering only supports simple binary comparisons
- relational filtering does not support logical `AND` / `OR` / `NOT`
- relational filtering does not support `IN`
- relational filtering does not support `IS NULL` / `IS NOT NULL`
- the active relational planning path is centered on registered `ParquetRelScan` / `ParquetRelJoin` rules

They may still be conceptually accurate if they describe the broad separation between native Parquet filtering and adapter-level row filtering.
