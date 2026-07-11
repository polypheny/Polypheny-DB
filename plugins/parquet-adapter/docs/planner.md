# Planner Basics

Polypheny turns a query into an executable algebra tree by applying planner
rules. The Parquet adapter participates by registering Parquet-specific
conventions, nodes, and rewrite rules.

## Main Concepts

Plan node:

- represents one operation in the plan tree
- examples: `ParquetRelScan`, `ParquetRelJoin`, `ParquetRelAggregate`,
  `EnumerableParquet`

Rule:

- recognizes a plan shape and replaces it with a cheaper or more specific shape
- current Parquet rules are wrapped by `ParquetAlgOptRule`
- relational matchers live in `ParquetRelPatternMatchers`
- document matchers live in `ParquetDocPatternMatchers`
- shared aggregate union rewrites live in `ParquetAggregatePatternMatchers`

Convention:

- marks which runtime owns a physical plan fragment
- relational Parquet nodes use `ParquetRelConvention`
- document Parquet nodes use `ParquetDocConvention`
- Parquet plans are converted back to `EnumerableConvention` by
  `EnumerableParquet`

## Relational Planning Path

Relational tables are exposed through `ParquetRelTable`.

When Polypheny asks the source for a scan, `ParquetRelationalSource.getRelScan`
adds `FilterSetOpTransposeRule` and returns a `ParquetRelScan`. The scan
registers `ParquetRelConvention`, which registers rules from `ParquetRelRules`.

Always-registered relational rules:

- `ParquetRelPatternMatchers.attachFilterToJoinUnderCalc`
- `ParquetRelPatternMatchers.attachFieldsAndFiltersToScanUnderCalc`
- `ParquetRelRules.EnumerableParquetRule`

Rules registered when `ParquetOptimizationSettings.isOptimizeAggregation()` is
enabled:

- `ParquetRelPatternMatchers.joinWithScanOnLeftAndScanOnRight`
- `ParquetRelPatternMatchers.aggregateOnScan`
- `ParquetRelPatternMatchers.aggregateOnCalcScan`
- `ParquetAggregatePatternMatchers.partialAggregateOnUnion`
- `ParquetAggregatePatternMatchers.partialAggregateOnCalcUnion`

## Document Planning Path

Document collections are exposed through `ParquetDocument`.

Document plans use `ParquetDocConvention` and rules from `ParquetDocRules`.
Supported filters are attached directly to `ParquetDocScan`; the current code
does not have a separate document-filter node.

Always-registered document rules:

- `ParquetDocPatternMatchers.attachFiltersToScanUnderCalc`
- `ParquetDocRules.EnumerableParquetDocumentRule`

Aggregate rules registered when `ParquetOptimizationSettings` enables
aggregation optimization:

- `ParquetDocPatternMatchers.aggregateOnScan`
- `ParquetDocPatternMatchers.aggregateOnProjectScan`
- `ParquetDocPatternMatchers.aggregateOnCalcScan`
- `ParquetAggregatePatternMatchers.partialAggregateOnUnion`
- `ParquetAggregatePatternMatchers.partialAggregateOnCalcUnion`

## Projection And Filter Pushdown

The relational scan rule consumes a supported `EnumerableCalc` above an
`EnumerableParquet -> ParquetRelScan` subtree. It can:

- keep simple projected field indexes on `ParquetRelScan`
- translate supported predicates through `ParquetRelFilterTranslator`
- remove the calc when all of its work is represented by the updated scan

Document filter pushdown is similar, but the rule works with
`ParquetDocPatternMatchers.attachFiltersToScanUnderCalc` and
`ParquetDocFilterTranslator`.

## Structural Join Planning

Adapter-level joins use `ParquetRelJoin`. The join matcher recognizes an
`EnumerableJoin` whose inputs are both Parquet scans and whose condition is a
generated structural parent/child key comparison:

```text
child.__polypheny_parent_row_id = parent.__polypheny_row_id
```

The matcher checks the bindings through `ParquetRelJoin.supportedDirection` and
creates a `ParquetRelJoin` when the relationship is a direct generated
parent/child relationship.

## Aggregate Planning

Aggregate pushdown creates `ParquetRelAggregate` or `ParquetDocAggregate`.

The aggregate node chooses:

- `metadataAggregate`: exact footer/partition-statistics path for supported
  `COUNT`, `MIN`, and `MAX` cases
- `dataAggregate`: adapter-side row scan aggregate path for supported
  `COUNT`, `SUM`, `MIN`, and `MAX` cases

Shared union rewrites use `ParquetEnumerableUnion` as a marker so partial
aggregate decomposition is not repeatedly applied to the same union subtree.

## PolyAlg Names

`ParquetPlugin` registers display names for the current planner nodes:

| PolyAlg name | Class |
| --- | --- |
| `PE_CALC` | `EnumerableParquet` |
| `PE_UNION` | `ParquetEnumerableUnion` |
| `P_SCAN` | `ParquetRelScan` |
| `P_METADATA_SCAN` | `ParquetRelMetadataScan` |
| `P_JOIN` | `ParquetRelJoin` |
| `P_AGGREGATE` | `ParquetRelAggregate` |
| `P_DOC_SCAN` | `ParquetDocScan` |
| `P_DOC_METADATA_SCAN` | `ParquetDocMetadataScan` |
| `P_DOC_AGGREGATE` | `ParquetDocAggregate` |
