
# Relational Schema Normalization

## Example Parquet shape:
```
orders.parquet
root
|- order_id: int64
|- customer: string
`- items: repeated group
   |- sku: string
   |- quantity: int32
   `- discounts: repeated group
      |- code: string
      `- amount: decimal
```

## Flat Mode
preserve the Parquet file as one table

**_orders_**


| order_id | customer | items                                                                                                                                           |
|----------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| 1001     | Alice    | [{sku: "A-1", quantity: 2, discounts: [{code: "SUMMER", amount: 5.00}, {code: "VIP", amount: 2.00}]}, {sku: "B-5", quantity: 1, discounts: []}] |
| 1002     | Bob      | [{sku: "C-9", quantity: 4, discounts: []}]                                                                                                      |


```
orders.parquet
-> orders
```

## Normalized Mode
split nested structures into generated relational child tables

**_orders_**

| __polypheny_row_id | order_id | customer |
|--------------------|----------|----------|
| 1                  | 1001     | Alice    |
| 2                  | 1002     | Bob      |


**_orders__items_**

| __polypheny_row_id | __polypheny_parent_row_id | __polypheny_elem_ordinal | sku | quantity |
|--------------------|---------------------------|--------------------------|-----|----------|
| 10                 | 1                         | 0                        | A-1 | 2        |
| 11                 | 1                         | 1                        | B-5 | 1        |
| 12                 | 2                         | 0                        | C-9 | 4        |

**_orders__items__discounts_**

| __polypheny_row_id | __polypheny_parent_row_id | __polypheny_elem_ordinal | code   | amount |
|--------------------|---------------------------|--------------------------|--------|--------|
| 20                 | 10                        | 0                        | SUMMER | 5.00   |
| 21                 | 10                        | 1                        | VIP    | 2.00   |

```
orders.parquet
    -> orders
    -> orders__items
    -> orders__items__discounts
 ```

```sql
SELECT o.order_id, i.sku, i.quantity
FROM orders o
JOIN orders__items i
  ON i.__polypheny_parent_row_id = o.__polypheny_row_id;

```
## Behavior:

- All relational Parquet table names are adapter-prefixed, both flat and normalized. <br>
*Example:* `parquetrelational1__orders`, not just `orders`. This allows create different adapters for flat and normalized schema modes.

- Flat mode shows one table per Parquet file, with nested/repeated values kept inside the root table as structured values.

- Normalized mode shows root tables with direct primitive fields only.
- Every Parquet group field becomes a generated virtual relational table.
- Nested groups inside nested groups also become virtual tables. <br> *Example:* items.discounts becomes parquetrelational1__orders__items__discounts.
- Normalized tables include visible ***synthetic key columns***:
  - __polypheny_row_id
  - __polypheny_parent_row_id
  - __polypheny_elem_ordinal
- Root tables have: 
  - __polypheny_row_id
- Child tables have: 
  - __polypheny_row_id
  - __polypheny_parent_row_id
  - __polypheny_elem_ordinal - for repeated children
- Non-repeated child tables still have: 
  - __polypheny_parent_row_id
  - __polypheny_elem_ordinal is always 0

***Sql Example:***
```sql
SELECT o.order_id, i.sku, i.quantity
FROM parquetrelational1__orders o
JOIN parquetrelational1__orders__items i
  ON i.__polypheny_parent_row_id = o.__polypheny_row_id;

```
For deeper nesting, the same rule applies at each level. A row in parquetrelational1__orders__items__discounts points to its parent item row, not directly to the root order row:
```sql
SELECT i.sku, d.code, d.amount
FROM parquetrelational1__orders__items i
JOIN parquetrelational1__orders__items__discounts d
  ON d.__polypheny_parent_row_id = i.__polypheny_row_id;
```

## Code Changes

### AbstractParquetSource
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\shared\AbstractParquetSource.java`
- Adding shared metadata persistence, safer URL handling, and adapter-prefixed table names
- Store Parquet Bindings in adapter settings in persistParquetBindings() function
- Load Parquet Bindings from settings in constructor()
- Prefix is added to the table names to allow deployment multiple Parquet relational adapters over the same files, for example one flat and one normalized, without logical table name collisions - getPrefixTableName() function 

### ParquetColumnRole
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetColumnRole.java`
- Describes what kind of column a relational Parquet column is inside a ParquetTableBinding
  - DATA - a normal real Parquet value
  - PRIMARY_KEY - synthetic generated row id
  - PARENT_KEY - synthetic reference to the parent generated row
  - ORDINAL - position of a child value inside its parent

### ParquetColumnBinding
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetColumnBinding.java`
Metadata object that tells the Parquet scanner for specific Polypheny column, where does its value come from. Used for reading data.
Contains:
- columnId - Polypheny physical column id connected to an actual column in Polypheny’s physical table catalog
- columnName - relational column name
- role - ParquetColumnRole - what kind of column this is
- sourcePathElements - the path inside the Parquet schema that should be used to read this column. For example:
  - flat/root column - path: order_id
  - nested - path: shipping_address.city


### ParquetTableBinding
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetTableBinding.java`
Table-level metadata, describes the whole table, immutable record
Fields:
- sourceUrl - real Parquet file URL to read from
- parentTableName - generated parent relational table name, or null for root tables
- sourcePathElements - table-level path inside the Parquet file, For root: List.of(), for nested: List.of("items")
- columnsByColumnId - Map<Long, ParquetColumnBinding> - maps Polypheny physical column ids to column bindings

Example:
```
sourceUrl: orders.parquet
parentTableName: parquetrelational1__orders
sourcePathElements: ["items"]

columnPaths:
sku      -> ["items", "sku"] // sku - stock keeping unit (product code)
quantity -> ["items", "quantity"]
```

### ParquetUrlResolver
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\shared\io\ParquetUrlResolver.java`
Utility, Parquet file/directory URL handling


### ParquetSchemaMode
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetSchemaMode.java`
User-selectable adapter setting that controls how the Parquet schema
- FLAT
- NORMALIZED

### ParquetSchemaNormalizer
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetSchemaNormalizer.java`
Turns Parquet nested schema into multiple relational tables for normalized mode.
Contains the main normalization logic.
It creates Result containing: 
- table definition: exported relational table definitions: Map<String, List<ExportedColumn>> exportedColumns
- binding metadata: explains how those tables map back to the Parquet file: Map<String, DiscoveredTableBinding> tableBindings


### ParquetNormalizedSchema
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetNormalizedSchema.java`
contains normalizes schema information for all tables:
- Map<String, List<ExportedColumn>> tables - normalized columns per table name
- Map<String, DiscoveredTableBinding> bindings - binding per table name

### DiscoveredTableBinding
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\DiscoveredTableBinding.java`
temporary discovery metadata before a real ParquetTableBinding created
for ParquetTableBinding we need physical table ids, that are created after normalization step 

### ParquetBindingSerializer
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetBindingSerializer.java`
Saves and restores ParquetTableBinding metadata through adapter settings.

### ParquetBindingRelEnumerator
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\execution\ParquetBindingRelEnumerator.java`
Root-table scanner that reads column values by ParquetColumnBinding paths instead of by physical ordinal position.
Normalized mode may remove nested fields from the root table. It
- reads full root rows from the Parquet file
- extracts each value by path. If the binding path is: List.of("shipping_address", "city") it walks: "root -> shipping_address -> city"
- It supports walking through non-repeated nested groups.
- If it encounters a repeated group in the middle of a root value path, it returns NULL instead of trying to flatten it.
- It converts final values using ParquetRelValueExtractor

### ParquetNestedRelEnumerator
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\execution\ParquetNestedRelEnumerator.java`
used for tables that do not have their own Parquet file, but are created from a nested group inside a Parquet file

### ParquetRelationalSource
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\ParquetRelationalSource.java`
changed from “simple relational Parquet source that exports one table per file” into the controller for flat vs normalized relational Parquet mode.

### ParquetRelTable
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetRelTable.java`
ParquetRelTable changed from a simple wrapper that always scanned Parquet columns by ordinal into a wrapper that can choose between three scan strategies:
- old flat ordinal scan
- binding-aware root scan
- nested child-table scan

### ParquetNamespace
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\shared\schema\ParquetNamespace.java`
Make relational table creation binding-aware.
