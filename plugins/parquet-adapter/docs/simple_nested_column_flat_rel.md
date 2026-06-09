# Add separate columns for primitive nested filed in flat mode

Simple nested field becomes a separate column.

Path-aware discovery was added so flat mode can expose non-repeated 
nested leaf fields as parent-table columns while repeated/list/map fields stay as nested/document columns.

Example:
```text
customer {
  id
  address {
    city
    country
  }
  orders[] ...
}
```
Flat mode now exposes:
```text
id
address_city
address_country
orders
```

### ExportedSchema.java
`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/io/ExportedSchema.java`

Contains:
- columns - what Polypheny should expose in the catalog
- columnPaths - adapter-private metadata telling the scanner how to read the real value from the Parquet group. Map: column name - column path

```text
columns[i] = "shipping_address_city_name"
columnPaths[i] = {"shipping_address_city_name" -> List.of("Shipping Address", "City Name")}
```

After flattening, the relational column name is no longer enough to know where the value lives in Parquet.

Before: 
```text
Polypheny column name == Parquet top-level field name
```
After:
```text
address_city -> ["address", "city"]
address_country -> ["address", "country"]
orders -> ["orders"]
```

#### Example of column path:

Parquet Schema:

```text
customer_id
address {
  city
  country
}
orders LIST
```
Flat exposed columns:

```text
customer_id
address_city
address_country
orders
```
Column path:

```text
{
  "customer_id"     -> ["customer_id"],
  "address_city"    -> ["address", "city"],
  "address_country" -> ["address", "country"],
  "orders"          -> ["orders"]
}
```

### ParquetSchemaReader.java
`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/io/ParquetSchemaReader.java`

Create ExportedSchema instead of ExportedColumns.

#### Changes:

- exportedSchema() - returns ExportedSchema, not only columns
- collectFlatColumns() - recursively walks non-repeated nested groups. Decides whether a Parquet field becomes a column itself, or whether its children become columns on the parent table.
- addFlatColumn() - creates the ExportedColumn and stores its path.
- shouldPreserveAsNestedColumn() - prevents repeated/list/map fields from being flattened
- uniqueColumnName() - avoids name collisions after flattening.

### ParquetFileDiscovery.java
`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/io/ParquetFileDiscovery.java`

ParquetFileDiscovery changed from “discover table names and column lists” to 
“discover table names, column lists, and the binding metadata required to read those columns correctly.”

Discovery now tracks an ExportedSchema per source file instead of only a list of ExportedColumn.
For each discovered Parquet file we keep:
- columns      -  visible Polypheny columns
- columnPaths  - exported column name -> real Parquet path

#### Changes:
1. exportedSchema(...) instead of exportedColumns(...)
2. When it creates a DiscoveredTable, it now stores columnPaths in DiscoveredTableBinding
3. canConsolidateSchemas(...) also calls columnPathsAreCompatible(tables.values()). 
  - canConsolidateSchemas(...) decides whether several Parquet files can be exposed as one Polypheny table. 
  - Before flattened nested columns, checking exported column names and types was mostly enough.
  - After flattening, this is no longer enough.
  - Example: 
    - File1: `a { b int }` Flat exported column: `"a_b" -> "a_b" -> ["a", "b"]`
    - File2: `a_b int` Flat Exported column: `"a_b" -> ["a_b"]`
  - So we add validation by path: columnPathsAreCompatible 
4. Partition collision validation now uses the real source path: `exportedSchema.columnPaths().get(column.name())`
   - Example of partitioned folder:
   
     ```text
     customers/
         country=CH/
             part-1.parquet
         country=US/
             part-2.parquet
     ```
      
   - Polypheny adds country as a virtual partition column based on the folder name. But sometimes the Parquet file itself also contains a column with the same exported name: `part-1.parquet:
     country STRING`. 
   - That is a collision: `folder partition column: country`, `file data column: country`. 
   - The existing logic allows this only if the file column always has the same value as the folder partition value.
   - To validate that, we inspect Parquet column statistics. 
   - But with flattened nested columns, the exported column name may not be the actual Parquet field path.
   - When validation sees a collision on exported column name, which is flattened nested column `shipping_address_country`, it cannot look for a top-level Parquet column named `shipping_address_country`. That column does not exist.
   - It must look at the real Parquet path: `exportedSchema.columnPaths().get(column.name())` which gives: `["Shipping Address", "Country"]`
   - Then findColumnChunk(...) uses that path to find the real Parquet column statistics.

5. Consolidated/partitioned tables rebuild a combined path map
   - When Polypheny exposes multiple Parquet files as one logical table, it has to combine their discovered schemas.
   - ParquetFileDiscovery decides: these files are compatible enough, so expose them as one table
   - `columnPaths(tables.values(), columns)` collects paths from all per-file ExportedSchema objects, but only keeps paths for columns that are actually present in the final combined table.
