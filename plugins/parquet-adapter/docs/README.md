# Parquet Adapter Documentation

The Parquet adapter exposes Parquet files as read-only Polypheny sources. It
supports relational tables, document collections, multi-file datasets,
Hive-style partition folders, projection and filter pushdown, metadata-backed
statistics, adapter-level structural joins, aggregate pushdown, and workflow
import/export helpers.

This folder contains project-code documentation only. The `REPORT` folder is a
separate report draft area and is not part of the adapter documentation set.

## Source Types

The plugin registers two source templates in `ParquetPlugin`:

- `Parquet Relational`: implemented by `ParquetRelationalSource`
- `Parquet Document`: implemented by `ParquetDocumentSource`

Both templates accept the same source-location modes:

- `upload`: uploaded Parquet files or folders
- `link`: a local file or directory path
- `url`: a URL pointing to Parquet data

The relational adapter has an additional `schema mode` setting:

- `flat`: expose each discovered table as one relational table, flattening
  non-repeated nested scalar fields into columns where possible
- `normalized`: expose nested structures as generated parent/child relational
  tables with synthetic structural columns

Adapter-backed entities are read-only. Workflow export to Parquet is handled by
the workflow engine and shared writer classes; it does not make adapter source
tables writable.

## Core Runtime Flow

1. `ParquetPlugin` registers adapter templates and PolyAlg display nodes.
2. `AbstractParquetSource` resolves the configured source location and delegates
   discovery to `ParquetFileDiscovery`.
3. `ParquetSchemaReader` reads Parquet footers and builds `ExportedSchema`
   objects containing visible columns and source-path metadata.
4. `ParquetNamespace` creates relational table wrappers or document collection
   wrappers.
5. Relational queries enter through `ParquetRelScan`; document queries enter
   through `ParquetDocScan`.
6. Runtime execution opens `ParquetSourceReader` instances through the
   appropriate executor or enumerator and returns Polypheny rows/documents.

## Relational Planning

The active relational planning path uses:

- `ParquetRelConvention`
- `ParquetRelRules`
- `ParquetRelPatternMatchers`
- `ParquetAlgOptRule`
- `EnumerableParquet`
- `ParquetRelScan`
- `ParquetRelJoin`
- `ParquetRelAggregate`
- `ParquetRelMetadataScan`
- `ParquetEnumerableUnion`

Always-registered rules attach supported projections and filters to scans and
convert Parquet-convention plans back to enumerable convention. When
`ParquetOptimizationSettings.isOptimizeAggregation()` is enabled, the planner
also registers structural join and aggregate rewrite rules.

## Document Planning

The active document planning path uses:

- `ParquetDocConvention`
- `ParquetDocRules`
- `ParquetDocPatternMatchers`
- `ParquetDocScan`
- `ParquetDocAggregate`
- `ParquetDocMetadataScan`
- `ParquetDocFilterTranslator`

Document filters are stored directly on `ParquetDocScan`. The current
implementation does not use a separate document-filter planner node.

## Feature Documentation

- [Workflow Integration](workflow.md)
- [Planner Basics](planner.md)
- [Filter Overview](filter_overview.md)
- [File Pruning](file_pruning.md)
- [Partitioned Layouts](partitions_overview.md)
- [Nested Fields and Normalized Schemas](nested_fields.md)
- [Adapter-Level Joins](joins.md)
- [Aggregation Planning](aggregation_planning.md)
- [Aggregation Runtime Flow](aggregation_flow.md)
- [Statistics](statistics.md)
- [Relational and Document Execution Flows](rel_execution_flows.md)

## Supported Filtering

Relational filter translation supports:

- `column OP literal`
- `column OP dynamicParameter`
- reversed value/column comparisons
- casts around supported operands
- `IS NULL` and `IS NOT NULL`
- `AND`, `OR`, and `NOT` when all operands are translatable
- `IN` as an `OR` tree of equality filters

Supported binary operators are `=`, `!=`, `>`, `>=`, `<`, and `<=`.

Relational type/operator support:

| Polypheny type family | Supported operators |
| --- | --- |
| `BOOLEAN` | `=`, `!=`, `IS NULL`, `IS NOT NULL` |
| `VARCHAR`, `CHAR`, `TEXT` | `=`, `!=`, `IS NULL`, `IS NOT NULL` |
| `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DATE`, `TIME`, `TIMESTAMP` | all comparison operators plus null checks |

Document filter translation supports top-level document fields referenced as
`RexNameRef` or lowered `MQL_QUERY_VALUE(document, ARRAY['field'])` expressions.
It supports the same six binary comparison operators plus logical `AND`, `OR`,
and `NOT` when all child predicates are translatable. Document null checks,
`IN`, nested document paths, field-to-field comparisons, and arbitrary
functions are not pushed into the adapter.

Native Parquet filtering is attempted when the translated filter can be mapped
to a primitive non-repeated Parquet field. Exact adapter-level filtering remains
the correctness path when native filtering is unavailable.

## Package Overview

Important packages:

- `org.polypheny.db.adapter.parquet`: plugin entry point
- `document`: document source, planning, execution, and schema wrappers
- `relational`: relational source, planning, execution, filters, and schema
  bindings
- `shared.execution`: common enumerators, filter translation helpers, writer
  buffer utilities, and virtual groups
- `shared.execution.aggregate`: shared aggregate executors and aggregate
  enumerators
- `shared.filter`: immutable adapter filter trees and row/file evaluators
- `shared.io`: file discovery, schema reading, source reading, primitive-row
  reading, and source writing
- `shared.optimization`: shared planner matcher wrappers and aggregate
  decomposition helpers
- `shared.planning`: shared PolyAlg display helpers and union marker node
- `shared.schema`: type conversion, name normalization, namespace helpers, and
  Parquet message building
- `shared.schema.inference`: workflow export schema inference
- `shared.statistics`: metadata-backed table and column statistics
- `shared.util`: Hadoop configuration setup for the plugin classloader

## Build And Tests

Run the Parquet adapter tests with:

```powershell
.\gradlew.bat :plugins:parquet-adapter:test
```

Workflow Parquet support has additional tests in the workflow engine module.

## Current Limitations

- Adapter source tables and collections are read-only.
- Graph model import/export is not supported by the Parquet adapter or Parquet
  workflow export.
- Only the first discovered partition column is represented as Polypheny
  physical partition metadata; deeper partition columns remain adapter-level
  partition values and can still be used for file pruning.
- Native Parquet predicate pushdown is an optimization and only applies to
  primitive non-repeated fields that Parquet can filter safely.
- Adapter-level structural joins are limited to generated parent/child
  relationships from the same physical Parquet structure.
- Aggregate pushdown is controlled by `ParquetOptimizationSettings`.
