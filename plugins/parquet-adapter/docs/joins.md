# Adapter-Level Structural Joins

Adapter-level joins let the Parquet adapter execute direct parent/child joins
between generated normalized tables without materializing both sides as
independent generic relational inputs first.

## Supported Relationship

The supported relationship is the generated structural key relationship:

```text
child.__polypheny_parent_row_id = parent.__polypheny_row_id
```

Both tables must come from the same normalized Parquet structure, and the child
must be a direct generated child of the parent.

Examples:

- `orders` -> `orders__items`
- `orders` -> `orders__shipping_address`
- `orders__items` -> `orders__items__discounts`

## Planning

The active relational planner path uses:

- `ParquetRelConvention`
- `ParquetRelRules`
- `ParquetRelPatternMatchers`
- `ParquetAlgOptRule`
- `EnumerableParquet`
- `ParquetRelScan`
- `ParquetRelJoin`

`ParquetRelPatternMatchers.joinWithScanOnLeftAndScanOnRight` recognizes an
`EnumerableJoin` whose left and right inputs are Parquet scans. It asks
`ParquetRelJoin.supportedDirection(...)` whether the join condition is a direct
structural relationship.

When supported, the generic join is replaced with `ParquetRelJoin` and wrapped
back in `EnumerableParquet`.

The structural join matcher is registered when
`ParquetOptimizationSettings.isOptimizeAggregation()` is enabled. This setting
defaults to enabled unless the corresponding system property or environment
variable disables it.

## Runtime

`ParquetRelJoin.implement(...)` calls:

```text
ParquetRelTable.nestedJoin(...)
```

Runtime execution is handled by:

- `ParquetRelNestedJoinExecutor`
- `JoinFiltersSplitter`
- `ParquetNestedJoinEnumerator`
- `CombinedGroup`
- `VirtualGroup`

The executor:

1. resolves dynamic parameters
2. combines filters already attached to the left and right scans
3. maps filters to parent, child, native-reader, and joined-row residual paths
4. prunes parent source files using partition/statistics filters
5. reads parent rows
6. expands direct child rows from the nested Parquet structure
7. emits joined rows, including supported outer-join unmatched parent rows

## Filter Handling

Filters may originate from:

- the left scan
- the right scan
- a calc above the join

`ParquetRelPatternMatchers.attachFilterToJoinUnderCalc` can attach supported
filters above an already-created `ParquetRelJoin`.

At runtime, `JoinFiltersSplitter` separates filters into:

- parent-only filters
- child-only filters
- joined-row adapter filters
- native reader filters

Parent filters can participate in source-file pruning. Child and joined-row
filters are evaluated after nested child rows are expanded.

## Supported Join Types

The adapter-level node can represent inner and outer structural joins when the
planner produces a supported direct parent/child condition. Runtime uses the
`emitUnmatchedParents` flag to preserve parent rows for parent-preserving outer
join cases.

## Unsupported Or Fallback Cases

The adapter does not execute these as structural joins:

- root-to-root user-column joins
- joins between unrelated Parquet tables
- sibling child joins
- ancestor-to-grandchild joins that skip the direct parent
- self joins
- joins on user columns instead of generated structural keys
- multi-key structural joins
- non-equality structural joins
- disjunctive join conditions
- joins whose inputs contain operators that cannot be represented inside the
  Parquet join path, such as a limit below a join input

Unsupported cases remain available to the normal Polypheny planner and runtime.
