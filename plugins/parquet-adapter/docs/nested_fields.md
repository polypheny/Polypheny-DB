# Nested Fields And Normalized Relational Schemas

Parquet files can contain nested groups, repeated groups, lists, and maps. The
relational adapter exposes these structures in two modes controlled by the
`schema mode` setting on `ParquetRelationalSource`.

## Flat Mode

Flat mode exposes one table per discovered table source.

Primitive top-level fields become normal columns. Non-repeated nested primitive
fields are flattened into parent-table columns using normalized composite names.
Repeated/list/map fields are preserved as structured values instead of being
flattened into repeated scalar columns.

Example Parquet shape:

```text
customer_id
shipping_address {
  city
  country
}
orders repeated group
```

Flat exposed columns:

```text
customer_id
shipping_address_city
shipping_address_country
orders
```

The visible column name is not enough to read the physical value, so
`ExportedSchema` stores column paths:

```text
shipping_address_city -> ["shipping_address", "city"]
shipping_address_country -> ["shipping_address", "country"]
```

`ParquetSchemaReader.collectFlatColumns(...)` builds these columns and paths.
`ParquetFileDiscovery` validates that compatible multi-file tables also have
compatible column paths.

## Normalized Mode

Normalized mode splits nested structures into generated relational tables:

- primitive field: data column on the current table
- non-repeated group: child table with at most one row per parent row
- repeated group: child table with multiple rows per parent row
- repeated primitive field: child table with one data value column

Generated table names are adapter-prefixed to avoid collisions between
different Parquet source instances and schema modes.

Example:

```text
orders.parquet
root
|- order_id: int64
|- customer: string
|- items: repeated group
   |- product_id: string
   |- quantity: int32
   |- discounts: repeated group
      |- code: string
      |- amount: decimal
```

Normalized tables:

```text
pn__orders
pn__orders__items
pn__orders__items__discounts
```

## Synthetic Columns

Normalized relational tables contain generated structural columns.

Root tables:

- `__polypheny_row_id`

Child tables:

- `__polypheny_row_id`
- `__polypheny_parent_row_id`
- `__polypheny_elem_ordinal`

Synthetic row ids are deterministic structural paths:

```text
0
0/items[1]
0/items[1]/discounts[0]
```

These columns are not stored in Parquet. They are generated during scan by
`VirtualGroup`, `GroupMetadata`, `ParquetNestedRepeatedRelEnumerator`, and
`ParquetNestedNonRepeatedRelEnumerator`.

## Bindings

Generated tables are virtual. `ParquetTableBinding` connects each physical
Polypheny table to the original source files and Parquet paths.

Important binding objects:

- `DiscoveredTableBinding`: discovery-time binding before physical table ids
  exist
- `ParquetTableBinding`: persisted runtime binding for a physical table
- `ParquetColumnBinding`: maps one physical column to its role and source path
- `ParquetColumnRole`: `DATA`, `PRIMARY_KEY`, `PARENT_KEY`, `ORDINAL`,
  `PARTITION`
- `ParquetBindingSerializer`: persists bindings through adapter settings

For a generated `pn__orders__items` table, the table binding stores:

```text
sourceFiles = original Parquet files
parentTableName = pn__orders
sourcePathElements = ["items"]
```

Column bindings describe how each output column is produced:

```text
__polypheny_row_id        -> PRIMARY_KEY
__polypheny_parent_row_id -> PARENT_KEY
__polypheny_elem_ordinal  -> ORDINAL
product_id               -> DATA, ["items", "product_id"]
quantity                 -> DATA, ["items", "quantity"]
```

## Scan Strategies

`ParquetRelTable` delegates scans to `ParquetRelExecutorsFactory`, which chooses
an execution strategy based on table shape and projection:

- `ParquetRowRelEnumerator`: fast primitive-row path for simple flat primitive
  projections
- `ParquetRelEnumerator`: generic flat row path
- `ParquetNestedNonRepeatedRelEnumerator`: path-based one-row-per-root scan
- `ParquetNestedRepeatedRelEnumerator`: expands repeated nested rows
- `ParquetNestedJoinEnumerator`: executes supported structural parent/child
  joins

Path-based scans use `ParquetPathValueExtractor` to read values from the
original Parquet group and to synthesize structural values.

## Querying Normalized Tables

Generated structural joins use parent and child synthetic keys:

```sql
SELECT o.order_id, i.product_id, i.quantity
FROM pn__orders o
JOIN pn__orders__items i
  ON i.__polypheny_parent_row_id = o.__polypheny_row_id;
```

For deeper nesting, join each child to its direct parent:

```sql
SELECT i.product_id, d.code, d.amount
FROM pn__orders__items i
JOIN pn__orders__items__discounts d
  ON d.__polypheny_parent_row_id = i.__polypheny_row_id;
```

The adapter can execute supported direct parent/child joins as `ParquetRelJoin`.
Other joins fall back to normal Polypheny planning.
