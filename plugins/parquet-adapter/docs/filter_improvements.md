# Parquet Filter Improvements

This document briefly describes the latest filtering improvements in the Parquet adapter.

## Goal

The previous filtering implementation mainly handled single comparison predicates.

The new goal of this development step was:

1. support logical filter trees
2. resolve nested filter operands recursively
3. reuse shared translation logic between relational and document filters
4. allow both exact filtering and native Parquet pushdown to understand composed filters

## Main Changes

### `AbstractFilterTranslator`

File:

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/execution/AbstractFilterTranslator.java`

The old helper `ParquetFilterTranslationSupport` was replaced by `AbstractFilterTranslator`.

It now provides shared logic for:

- parsing binary comparison predicates
- unwrapping casts
- reversing comparison operators when the value is on the left
- converting literals and dynamic parameters into `ParquetAdapterFilter`

This logic is now reused by both:

- `ParquetRelFilterTranslator`
- `ParquetDocFilterTranslator`

### `ParquetAdapterFilter`

File:

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/filter/ParquetAdapterFilter.java`

`ParquetAdapterFilter` was extended from a simple leaf predicate into a filter tree node.

New capabilities:

- store logical operands
- represent:
  - `AND`
  - `OR`
  - `NOT`
- distinguish logical nodes from leaf predicates using `isLogical()`

This makes the filter model recursive.

### `ParquetRelFilterTranslator`

File:

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/relational/execution/ParquetRelFilterTranslator.java`

Relational filter translation was extended to support:

- `AND`
- `OR`
- `NOT`
- `IN`
- reversed comparisons such as:

```text
10 < column
```

`IN` is translated as:

```text
column IN (a, b, c)
-> OR(EQUALS(column, a), EQUALS(column, b), EQUALS(column, c))
```

Unsupported operands still return `null`, so unsupported predicates are not pushed into the adapter.

### `ParquetDocFilterTranslator`

File:

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/document/execution/ParquetDocFilterTranslator.java`

The document filter translator was aligned with the new shared translation base.

It now reuses the same parsing and value-handling logic as the relational translator.

### `ParquetRelTable`

File:

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/relational/schema/ParquetRelTable.java`

Filter resolution is now recursive.

Before this change, only flat leaf filters were resolved.
Now `resolveFilter(...)` walks the full filter tree and:

- resolves dynamic parameters
- resolves Parquet source paths through bindings
- rebuilds logical filters with resolved child operands

This is important for normalized tables and composed predicates.

### `AbstractParquetEnumerator`

File:

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/execution/AbstractParquetEnumerator.java`

Exact row-level filtering now understands logical filters.

The `matches(...)` function now evaluates:

- `AND` using `allMatch`
- `OR` using `anyMatch`
- `NOT` by negating its single operand

So enumerator-level filtering is no longer limited to single leaf predicates.

### `ParquetNativeFilterBuilder`

File:

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/filter/ParquetNativeFilterBuilder.java`

Native Parquet pushdown was extended to build logical predicate trees.

New behavior:

- recursively builds child predicates
- combines them with:
  - `FilterApi.and(...)`
  - `FilterApi.or(...)`
  - `FilterApi.not(...)`

If any child predicate cannot be translated safely, native pushdown for that logical filter returns `null`.

This keeps native filtering conservative and correct.

## Supported New Cases

The following filter shapes are now supported much better than before:

- conjunctions:

```text
a > 10 AND b = 20
```

- disjunctions:

```text
a = 10 OR a = 20
```

- negation:

```text
NOT (a = 10)
```

- `IN`:

```text
a IN (10, 20, 30)
```

- reversed comparison operands:

```text
10 < a
```

## Test Updates

File:

`plugins/parquet-adapter/src/test/java/org/polypheny/db/adapter/parquet/ParquetRelFilterTranslatorTest.java`

Tests were extended to verify:

- `AND` / `OR` translation
- `IN` translation into `OR` of equality filters
- operator reversal when the literal is on the left
- safe handling of unsupported native timestamp pushdown cases

## Changed Files

- `plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/document/execution/ParquetDocFilterTranslator.java`
- `plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/relational/execution/ParquetRelFilterTranslator.java`
- `plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/relational/schema/ParquetRelTable.java`
- `plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/execution/AbstractFilterTranslator.java`
- `plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/execution/AbstractParquetEnumerator.java`
- `plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/filter/ParquetAdapterFilter.java`
- `plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/filter/ParquetNativeFilterBuilder.java`
- `plugins/parquet-adapter/src/test/java/org/polypheny/db/adapter/parquet/ParquetRelFilterTranslatorTest.java`

## Summary

The filtering model was upgraded from single comparison predicates to recursive logical filter trees.

This improvement affects both:

- filter translation
- filter execution

and it applies to both:

- exact enumerator-level filtering
- native Parquet predicate pushdown

The result is better support for realistic composed predicates while keeping unsupported cases conservative.
