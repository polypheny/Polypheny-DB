# Parquet Module Implementation

## Overview

The Parquet adapter exposes `.parquet` files as Polypheny source tables.
At a high level, the adapter is responsible for:
- discovering Parquet files from the configured source location
- extracting relational schema metadata from Parquet file footers
- restoring and registering tables whose data is read from Parquet files
- executing read queries over Parquet data
- applying projection pushdown and supported native predicate pushdown during query execution

The adapter is read-only and currently targets relational source integration for embedded deployments.

The following sections describe the architecture, execution flow, package responsibilities, supported query behavior, and current limitations in more detail.

## Build and Integration Changes

1\. To integrate the Parquet adapter as a plugin, the Gradle configuration must include the Parquet module and its dependencies.

Affected files:
- `settings.gradle`
- `gradle.properties`
- `plugins/parquet-adapter/build.gradle`


2\. To make Parquet the default data source, `PolyphenyDB.java` sets `defaultSourceName` to `parquet`.<br>
During `restore()`, `Catalog.defaultSource` is populated from the configured adapter state.


## Execution Flow

1. **Plugin registration**
    - **Responsible class:** `ParquetPlugin`
    - Registers the Parquet adapter template during startup.
    - Removes the template during shutdown.
2. **Schema initialization**
    - **Responsible classes:** `ParquetSource`, `ParquetFileDiscovery`, `ParquetTypeConverter`
    - Resolves the configured Parquet directory or file location.
    - Discovers available `.parquet` files.
    - Builds table schemas from Parquet file metadata.
    - Maps Parquet field types to Polypheny types.
    - Normalizes physical table names and column names.
    - Initializes the information page from the extracted column definitions.
3. **Namespace and table creation**
    - **Responsible classes:** `ParquetSource`, `ParquetNamespace`, `ParquetTable`
    - Creates table wrappers for discovered Parquet files.
    - Table Wrapper implements Filterable, Scannable and Translatable functionality
    - Registers physical tables in the adapter catalog.
4. **Table restore during startup**
    - **Responsible classes:** `ParquetSource`, `ParquetNamespace`
    - During application startup, the adapter calls `restoreTable(...)` for persisted Parquet tables.
    - The adapter recreates table wrappers and re-registers them in the adapter catalog.
5. **Query entry into the adapter**
    - **Responsible class:** `ParquetFilterableTable`, `ParquetNamespace`, `ParquetTable`
    - The query engine resolves the registered Parquet-backed physical table.
    - The adapter uses the table metadata and source information to access the underlying .parquet file.
    - Projection and supported filter information are passed into the Parquet execution layer.

## Supported Filter Conditions

The active `FILTERABLE` execution path currently pushes only simple comparison conditions that can be translated into native Parquet predicates.

Supported operators:
- `=`
- `!=`
- `>`
- `>=`
- `<`
- `<=`

Supported condition shape:
- `column OP literal`

Notes:
- The left-hand side must be a column reference.
- The right-hand side must be a literal value.
- Simple casts on the column side are accepted.
- Only pushdown-safe conditions are removed from the outer filter list.
- Unsupported conditions remain outside the adapter-specific filter flow.

Supported pushdown types:
- `BOOLEAN`: `=` and `!=`
- `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DATE`, `TIME`, `TIMESTAMP`: all six comparison operators
- `VARCHAR`, `CHAR`, `TEXT`: `=` and `!=`

Notes:
- Pushdown uses Parquet native filter APIs through `FilterCompat.Filter`.
- Legacy Parquet `INT96` timestamp columns are currently not pushed down.


## Package Structure and Responsibilities

### `root`
Core adapter entry points and lifecycle integration.
- `ParquetPlugin`: registers and unregisters the adapter template.
- `ParquetSource`: main adapter implementation; manages source settings, schema export, information-page setup, namespace updates, table creation, and table restore.

### `io`
Parquet file access helpers.
- `ParquetFileDiscovery`: discovers valid `.parquet` files in the configured source.

### `schema`
Table wrappers and schema-related conversion logic.
- `ParquetNamespace`: creates Parquet table wrappers.
- `ParquetTable`: base class for Parquet-backed physical tables.
- `ParquetTypeConverter`: converts Parquet schema types, runtime values, and filter literals into Polypheny-compatible representations.

### `model`
Internal data representation.
- `FilterInfo`: immutable representation of a pushed filter condition.

### `execution`
Runtime data access and filter evaluation.
Classes responsible for reading Parquet data and returning rows.
- `ParquetEnumerator`: drives Parquet reading, projection handling, native filter configuration, row-group iteration, and row production.
- `ParquetPredicateBuilder`: translates adapter filters into native Parquet predicates.
- `ValueExtractor`: extracts raw values from Parquet records.

### `planning`
Classes that integrate the Parquet adapter with Polypheny’s query planner.
Handling query-plan representation and optimization.
- `ParquetScan`: Parquet scan operator for the planning layer.
- `ParquetProjectScanRule`: planner rule for projection handling over Parquet scans.

### `util`
Shared infrastructure helpers.
- `HadoopConfigurationFactory`: builds Hadoop configuration objects for Parquet access inside the plugin environment.

## Schema Mapping Rules

- Table names are derived from Parquet file names and normalized before registration.
- Column names are derived from Parquet field names and normalized before registration.
- Column types are mapped from Parquet schema types to Polypheny types.
- String-like Parquet fields are mapped to `TEXT`.
- Unlike SQL types with fixed length declarations, Parquet does not impose a fixed maximum string length.

## Flavor Selection

The adapter supports different levels of integration with the query engine:

- `SCANNABLE` is the simplest execution path. 
It performs plain row scanning through `ParquetScannableTable` and delegates reading to `ParquetEnumerator` without adapter-side filter pushdown. 

- `FILTERABLE` extends the scan-based model by accepting supported filter conditions from the query engine. 
In this path, `ParquetFilterableTable` translates supported expressions into `FilterInfo` objects,
`ParquetPredicateBuilder` converts them into native Parquet predicates,
and `ParquetEnumerator` applies projection pushdown and native predicate pushdown through `ParquetFileReader`. 

- `TRANSLATABLE` is the planner-oriented flavor. 
Instead of only returning rows directly, it allows the adapter to expose adapter-specific relational nodes and planner rules. 
This path is intended for deeper integration with query planning through `ParquetScan` and `ParquetProjectScanRule`.


## Information Page
The information page displays the following column metadata:

- Position
- Column name
- Type
- Nullable
- Filename
- Primary key flag


The following figure shows an example of the displayed schema information.
![Schema display](images/parquet_schema.png)

## Data Presentation
![Schema display](images/customers_data.png)

## Query Projection
![Schema display](images/query_projection.png)

## Query Filter
![Schema display](images/query_filter.png)



# Unit Tests
To run tests for parquet adapter:
- `.\gradlew.bat :plugins:parquet-adapter:test`

`plugins/parquet-adapter/build.gradle` should contain: 
- DBMS as a test dependency
- Polypheny JDBC driver dependency

## `ParquetPluginTest.java` 
contains the following tests:
1. `importsAllTablesAndReadsRows()`
2. `parquetSourceIsReadOnly()`
3. `readsExpectedRowsFromCustomers()`
4. `filtersRowsWithWhereClause()`
5. `projectsOnlyRequestedColumns()`
6. `supportsGreaterThanFilter()`
7. `supportsAllComparisonFilterOperations()`
8. `rejectsUpdateOnParquetSource()`
9. `projectsAndFiltersOtherTables()`
10. `returnsNestedShippingAddressAsJSON()`

## `ParquetPredicateBuilderTest.java`
contains focused unit tests for predicate translation behavior.

Current test coverage includes:
1. `rejectsInt96TimestampPredicatePushdown()`


## Current Gaps and Future Work
### Relational Parquet data source implementation
- Add write support - extended adapter beyond its current read-only design.
- Add partition-aware file discovery and query pruning for partitioned Parquet datasets.
- Add support for remote files.
- Extend filter support beyond simple comparisons of the form `column OP literal`.
  Planned examples include `BETWEEN`, `IN`, `IS NULL`, `IS NOT NULL`, and more complex boolean combinations such as `AND` and `OR`.
- Add optional native predicate support for additional Parquet physical encodings where safe and well-tested.
  One current example is legacy `INT96` timestamps, which are intentionally excluded from pushdown.
- Improve handling of nested and complex Parquet schemas instead of treating them only as textual representations.
- Document and strengthen error handling for invalid files, unsupported schema constructs, and conversion failures.


### Document Parquet data source implementation
