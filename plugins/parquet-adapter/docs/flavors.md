# Parquet Table Flavors

## Overview

The Parquet adapter defines three table flavors through `ParquetTable.Flavor`:

- `SCANNABLE`
- `FILTERABLE`
- `TRANSLATABLE`

These flavors represent different integration styles between the adapter and the Polypheny query engine. 
They represent different execution and planning strategies for reading the same Parquet data.
The active flavor is selected when the Parquet namespace creates table wrappers.

## Where Flavor Selection Happens

Flavor-based table creation is implemented in `ParquetNamespace`.

- `ParquetNamespace.createParquetTable(...)` prepares the metadata and source reference for a physical Parquet table.
- `ParquetNamespace.createTable(...)` instantiates the concrete table implementation according to the configured flavor.

The current Parquet source selects the `FILTERABLE` flavor in `ParquetSource.updateNamespace(...)`.


## Purpose of Each Flavor

### SCANNABLE

`SCANNABLE` is the simplest integration model.

In this flavor, the adapter behaves mainly as a row producer. 
The query engine asks the table to provide rows, and the adapter returns them as an enumerable result stream.

Characteristics:

- simple implementation model
- filtering is handled outside the adapter
- less efficient (it does not provide optimization)


### FILTERABLE

`FILTERABLE` extends the scan-based model by allowing the adapter to receive filter conditions from the query engine.

In this flavor, the adapter inspects query filters and pushes supported conditions into its own execution logic. 
This allows the Parquet reader to reduce the amount of data it reads and processes using native predicate pushdown.

Characteristics:

- supports filter pushdown
- supports projection-aware reading
- uses Parquet native filter stages such as statistics, dictionary, column index, bloom filter, and record filtering when available
- significantly more efficient than plain scan-only execution

This is the active flavor in the current Parquet adapter implementation.

### TRANSLATABLE

`TRANSLATABLE` is the most advanced integration style.

In this flavor, the adapter participates directly in query planning. 
Instead of only returning rows at execution time, it provides adapter-specific relational nodes and planner rules.

Characteristics:

- deeper integration with the query planner
- enables more advanced optimization
- more flexible than scan-only or filterable execution


## Flavor Workflows

The following sections describe how the same logical SQL query flows through each flavor.

Example query:

```sql
SELECT customer_id, name
FROM customers
WHERE customer_id > 3
```

### 1. SCANNABLE Workflow
The adapter mainly acts as a generic row reader.

Main class:

- `ParquetScannableTable`

Flow:

- The query engine decides a table scan is needed.
- The scannable table is asked to provide rows.
- The adapter reads rows from the Parquet file.
- Filtering may be applied later by the query engine rather than inside the adapter.
- Projection may also be handled outside the adapter depending on the execution path.

Practical consequence:

- more data may be read than actually needed
- pushdown opportunities are limited
- execution is simpler but less efficient
- appropriate when correctness and simplicity are more important than optimization.

### 2. FILTERABLE Workflow

The adapter receives a list of query filters and decides which ones it can handle internally.

Main classes:

- `ParquetFilterableTable`
- `ParquetEnumerator`
- `ParquetPredicateBuilder`

Flow:

- The query engine calls ParquetFilterableTable.scan(...).
- The table inspects incoming filter expressions.
- Only pushdown-safe filters are converted into adapter-specific `FilterInfo` objects.
- These filters are passed into ParquetEnumerator.
- The enumerator builds the requested projection schema from the projected columns.
- `ParquetPredicateBuilder` translates adapter filters into native Parquet predicates.
- `ParquetFileReader` is opened with `ParquetReadOptions` that enable native predicate pushdown stages.
- Matching rows are read directly from the projected Parquet schema and returned.

Practical consequence:

- fewer columns may be read
- fewer row groups and pages may be read when the Parquet file contains usable metadata
- less work is done outside the adapter

Current limitations:

- Only simple `column OP literal` predicates are pushed down.
- `BOOLEAN` and string-like fields currently support only `=` and `!=`.
- Legacy `INT96` timestamp columns are intentionally not pushed down.


### 3. TRANSLATABLE Workflow

The adapter participates in planning.

Main classes:

- `ParquetTranslatableTable`
- `ParquetScan`
- `ParquetProjectScanRule`

Flow:

- The planner encounters a Parquet-backed table.
- The translatable table produces an adapter-specific relational node.
- The query planner can rewrite the query plan using rules that know about your Parquet adapter.
- Projection and other operations can be integrated into adapter-specific planning structures.

Practical consequence:

- optimization can happen earlier and more explicitly
- this approach supports more sophisticated future optimizations
