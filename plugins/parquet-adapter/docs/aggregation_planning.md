# Aggregation Planning

Parquet aggregate pushdown replaces supported enumerable aggregate plans with
adapter-specific aggregate nodes. The current implementation supports both the
relational and document models.

Aggregate optimization is controlled by `ParquetOptimizationSettings`.

Configuration switches:

- system property: `polypheny.parquet.optimizeAggregation`
- environment variable: `POLYPHENY_PARQUET_OPTIMIZE_AGGREGATION`

The default is enabled.

## Planner Classes

Relational planning:

- `ParquetRelAggregate`
- `ParquetRelMetadataScan`
- `ParquetRelPatternMatchers.aggregateOnScan`
- `ParquetRelPatternMatchers.aggregateOnCalcScan`
- `ParquetRelRules`

Document planning:

- `ParquetDocAggregate`
- `ParquetDocMetadataScan`
- `ParquetDocPatternMatchers.aggregateOnScan`
- `ParquetDocPatternMatchers.aggregateOnProjectScan`
- `ParquetDocPatternMatchers.aggregateOnCalcScan`
- `ParquetDocRules`

Shared planning:

- `ParquetAggregatePatternMatchers.partialAggregateOnUnion`
- `ParquetAggregatePatternMatchers.partialAggregateOnCalcUnion`
- `ParquetEnumerableUnion`
- `AggregateDecomposition`
- `PartialAggregate`

## Relational Aggregate Rules

`aggregateOnScan` matches:

```text
EnumerableAggregate
  EnumerableParquet
    ParquetRelScan
```

and tries to create:

```text
EnumerableParquet
  ParquetRelAggregate
    ParquetRelScan or ParquetRelMetadataScan
```

`aggregateOnCalcScan` handles the same idea when an `EnumerableCalc` between the
aggregate and scan carries projection/filter work that can be represented by the
Parquet scan.

## Document Aggregate Rules

Document aggregate rules map projected top-level document fields to exported
Parquet columns before creating `ParquetDocAggregate`.

Supported shapes:

- aggregate directly over `ParquetDocScan`
- aggregate over field projection above `ParquetDocScan`
- aggregate over a project/filter calc above `ParquetDocScan`

Document aggregate pushdown is limited to projected top-level fields that can be
resolved to exported Parquet columns.

## Metadata Aggregate Mode

`ParquetRelAggregate` and `ParquetDocAggregate` first try metadata mode.

Metadata mode is exact-only. It is selected only when:

- aggregate calls are supported by `ParquetAggregateSupport`
- grouping fields are file-constant or partition-derived where required
- filters are decidable from partition values or footer statistics
- the runtime source confirms support through `supportsMetadataAggregate(...)`

Metadata-supported aggregate calls:

- `COUNT(*)`
- `COUNT(column)` when null-count metadata is available
- `MIN(column)`
- `MAX(column)`

Unsupported metadata cases fall through to data mode if data mode can execute
them.

## Data Aggregate Mode

Data mode scans Parquet data inside the adapter and aggregates before returning
rows to the rest of the Polypheny plan.

Supported data aggregate functions:

- `COUNT`
- `SUM`
- `MIN`
- `MAX`

Data mode supports numeric aggregation columns and supported grouping/filter
shapes. When a fast grouped reader path is not possible, the relational path can
fall back to row aggregation over Parquet enumerator output.

## Partial Aggregate Over Union

The shared union rules decompose aggregates above `UNION ALL` into partial
aggregates below the union and a final aggregate above it.

Example shape:

```text
EnumerableAggregate
  EnumerableUnion
    input1
    input2
```

becomes:

```text
EnumerableAggregate
  ParquetEnumerableUnion
    EnumerableAggregate
      input1
    EnumerableAggregate
      input2
```

`ParquetEnumerableUnion` marks that the union subtree has already been processed
for Parquet aggregate optimization.

## Runtime Entry Points

Relational runtime methods on `ParquetRelTable`:

- `metadataAggregate(...)`
- `dataAggregate(...)`
- `supportsMetadataAggregate(...)`
- `supportsDataAggregate(...)`

Document runtime methods on `ParquetDocument`:

- `metadataAggregate(...)`
- `dataAggregate(...)`
- `supportsMetadataAggregate(...)`
- `supportsDataAggregate(...)`

Shared runtime executors:

- `ParquetMetadataAggregateExecutor`
- `ParquetDataAggregateExecutor`

See [Aggregation Runtime Flow](aggregation_flow.md) and
[Relational and Document Execution Flows](rel_execution_flows.md) for runtime
details.
