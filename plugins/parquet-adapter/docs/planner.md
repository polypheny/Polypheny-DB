# Planner Basics

The planner takes a query and turns it into an executable plan.

In simple words:

```text
SQL query
  -> logical plan
  -> optimized physical plan
  -> runtime execution
```

## Plan Nodes

A plan node is one operation in the query plan.

Examples:

```text
Scan table
Filter rows
Project columns
Join two inputs
```

In code, plan nodes are classes such as:

```text
LogicalRelJoin
EnumerableJoin
ParquetRelScan
ParquetRelJoin
```

A plan is a tree of plan nodes. For example:

```text
LogicalRelProject
  LogicalRelFilter
    LogicalRelJoin
      LogicalRelScan(customers)
      LogicalRelScan(orders)
```

The node describes what should happen at that point in the tree.

## Logical And Physical Nodes

Logical nodes describe the operation, but not exactly how it will be executed.

Example:

```text
LogicalRelJoin
```

means:

```text
join these two inputs
```

It does not yet say whether the join will be executed by Polypheny, by an adapter, or by generated enumerable code.

Physical nodes describe a concrete execution strategy.

Examples:

```text
ParquetRelScan
ParquetRelJoin
EnumerableJoin
```

`ParquetRelJoin` means:

```text
execute this supported join inside the Parquet adapter
```

## Rules

A rule is not a plan node.

A rule is a planner transformation. It looks for a specific plan shape and replaces it with another plan shape.

Examples:

```text
ParquetRelScanRule
ParquetRelJoinRule
ParquetEnumerableJoinRule
ParquetEnumerableFilterJoinRule
ParquetEnumerableCalcJoinRule
```

A rule usually extends:

```text
AlgOptRule
ConverterRule
```

Plan nodes usually extend or implement algebra classes, for example:

```text
ParquetRelScan extends RelScan<ParquetRelTable> implements EnumerableAlg
ParquetRelJoin extends Join implements EnumerableAlg
```

So the difference is:

```text
Rule:
  changes the plan during optimization

Plan node:
  is part of the plan that will be optimized or executed
```

## Example: Projection Pushdown

`ParquetRelScanRule` matches:

```text
LogicalRelProject
  ParquetRelScan
```

If the project only selects existing columns, the rule replaces it with:

```text
ParquetRelScan(projected fields only)
```

So this:

```sql
SELECT name, age FROM customers
```

can become a scan that reads only `name` and `age`.

Here:

- `ParquetRelScanRule` is the rule.
- `LogicalRelProject` and `ParquetRelScan` are plan nodes.
- The rule removes the separate project node and creates a new `ParquetRelScan` node with fewer fields.

## Example: Adapter-Level Join

Before the join rule applies, the planner may have:

```text
LogicalRelJoin
  ParquetRelScan(parent)
  ParquetRelScan(child)
```

`ParquetRelJoinRule` checks whether this is a supported Parquet parent-child join.

If it is supported, the rule replaces the logical join with:

```text
ParquetRelJoin
  ParquetRelScan(parent)
  ParquetRelScan(child)
```

Here:

- `ParquetRelJoinRule` is the rule.
- `LogicalRelJoin`, `ParquetRelScan`, and `ParquetRelJoin` are plan nodes.
- `ParquetRelJoin` is the physical node that later generates runtime code.

At runtime, `ParquetRelJoin.implement(...)` calls:

```text
ParquetRelTable.nestedJoin(...)
```

which returns rows through:

```text
Enumerable<PolyValue[]>
```

## Convention

A convention describes how a plan node is implemented physically.

`Convention.NONE` means:

```text
logical node, no concrete execution strategy yet
```

`EnumerableConvention.INSTANCE` means:

```text
physical node that can execute through the enumerable row-iterator runtime
```

Enumerable does not mean there are many joins. It means rows are produced one by one through an `Enumerable` / `Enumerator`.

For example, a single `ParquetRelJoin` can be enumerable because it produces joined rows one by one.

## Short Mental Model

Use this mental model:

```text
Plan node = an operation in the plan tree
Rule      = a rewrite that changes the plan tree
Convention = the execution style of a plan node
```

Example:

```text
ParquetRelJoinRule sees LogicalRelJoin
  -> checks if adapter join is possible
  -> creates ParquetRelJoin
  -> ParquetRelJoin later executes the join
```
