# TODO: Adapter Level Joins

## Tests

- Add tests for all supported join types: `INNER`, `LEFT`, `RIGHT`, and `FULL`.
- Add tests where the parent table is on the left side of the join.
- Add tests where the parent table is on the right side of the join.
- Add tests for joins where the parent table is the root table.
- Add tests for joins where the parent table is itself nested and the child table is below it.
- Add tests for filters above adapter-level joins.
- Add tests for calc nodes above adapter-level joins, especially filter plus projection.
- Add tests that unsupported joins fall back to Polypheny-level join execution.
- Add result-equivalence tests comparing adapter-level join output with normal Polypheny join output.

## Future Join Tests

- Add tests for child-child joins inside the same parent when this join shape is supported.
- Add tests for ancestor-descendant joins beyond direct parent-child joins when this join shape is supported.
- Add fallback tests proving child-child joins and ancestor-descendant joins remain Polypheny-level joins until adapter-level support is implemented.

## Filter Correctness

- Review the split between reader filters and adapter filters.
- Ensure filters pushed to the native Parquet reader are still validated at adapter level when native filtering may be incomplete or unsupported.
- Verify logical filters (`AND`, `OR`, `NOT`) are split safely when some operands refer to parent fields, child fields, or both sides.
- Verify dynamic parameters are resolved correctly for join filters on both left and right inputs.
- Fix the flat relational filter regression where path-based resolved filters can reach `ParquetRelValueExtractor`, which does not support path extraction.

## Join Semantics

- Clarify and test `FULL` join behavior for nested parent-child data.
- Document that the runtime preserves unmatched parent rows but does not perform an independent child-side outer scan outside the parent path.
- Verify null-side row construction for `LEFT`, `RIGHT`, and `FULL` joins in both parent-left and parent-right directions.

## Future Join Scope

- Add support for child-child joins inside the same parent. Example: root has repeated `orders` and repeated `payments`, and the query joins both child tables through the same parent key. The adapter could read one parent row, expand both child collections under it, and join them locally. This would require a new runtime strategy because the current `ParquetNestedJoinEnumerator` assumes one parent path and one descendant child path, not two sibling child paths.
- Add support for ancestor-descendant joins beyond direct parent-child joins. Example: `root -> order -> item`, where a query joins root to item or order to item. The current path-prefix logic is already close to this, but the key-role model may need clearer handling if the immediate `PARENT_KEY` points only to the direct parent.

## Planner Rules

- Check whether valid plans are missed when projections, filters, or calcs appear between the join and `ParquetRelScan`.
- Consider whether additional rules are needed to recognize supported joins after simple projection reshaping.
- Confirm that unsupported joins always remain executable through normal Polypheny planning.

## Costing

- Improve `ParquetRelJoin.computeSelfCost(...)`.
- Include source scan cost, nested expansion cost, expected child cardinality, and pushed filter selectivity when possible.
- Check whether the planner chooses adapter-level joins too eagerly or not eagerly enough.

## Runtime Robustness

- Add tests for empty parent groups, empty child groups, missing optional nested fields, and repeated child groups.
- Verify cancellation behavior during nested join expansion.
- Verify close/error handling when join execution fails while reading the Parquet source.
