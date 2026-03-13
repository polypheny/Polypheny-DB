# Parquet Module Implementation

## Overview

The Parquet adapter exposes Parquet files as Polypheny source tables, derives schema from Parquet metadata, restores tables during adapter activation, and provides the infrastructure needed for query planning over data stored in Parquet files.

## Build and Integration Changes

1\. To integrate the Parquet adapter as a plugin, the Gradle configuration must include the Parquet module and its dependencies.

Affected files:
- `settings.gradle`
- `gradle.properties`
- `plugins/parquet-adapter/build.gradle`

<br>

2\. To make Parquet the default data source, `PolyphenyDB.java` sets `defaultSourceName` to `parquet`.<br>
During `restore()`, `Catalog.defaultSource` is populated from the configured adapter state.




## Main Execution Flow

1\. `ParquetPlugin` registers the adapter template on initialization and removes it on shutdown.

2\. `ParquetSource` is instantiated and configured with the Parquet source location.

3\. `ParquetFileDiscovery` identifies the available `.parquet` files in the configured source.

4\. `ParquetSource` reads Parquet metadata and extracts exported columns.

5\. `ParquetNamespace` creates table wrappers for each discovered file.

6\. Restored tables are registered in the adapter catalog.

7\. Query execution uses the planning layer through `ParquetScan` and related rules.


## Package Structure and Responsibilities

### `root`

Contains main classes

#### 1. `ParquetPlugin`

Registers the adapter template (`ParquetSource`) on initialization and removes the template on stop.

#### 2. `ParquetSource`

Core adapter entry point. Responsible for extracting column information from Parquet metadata, creating the information page, restoring tables, and coordinating namespace/table creation.

Main functions:

- `constructor()`

- `setParquetDir()`

- `createInformationPage()`

- `getExportedColumns()`

- `computePhysicalTableName()`

- `getValidTableName()`

- `getExportedColumnsFromFile()`

- `getExportedColumnFromField()`

- `getValidColumnNameFromField()`

- `getInformationTable()`

- `enableInformationPage()`

- `restoreTable()` called from `AdapterRestore.activate()`

- `createParquetTable()`


Restore flow:

- `restoreTable()` calls `parquetNamespace.createParquetTable()` to create physical tables by flavor.

- Restored tables are then added to `adapterCatalog`.


Create/refresh flow:

- `createParquetTable()` calls `parquetNamespace.createParquetTable()` and replaces the affected tables in `adapterCatalog`.

### `io`
Contains classes related to Parquet file access and discovery, including locating input files and handling Parquet-specific input operations.

#### 3. `ParquetFileDiscovery`

Helper functionality for identifying valid Parquet files.

Main functions:

- `listParquetFiles(dir)`

- `isParquetFile(filename)`


### `schema`

Contains namespace and table wrapper classes that expose Parquet-backed tables to Polypheny.


#### 4. `ParquetNamespace`

Creates table wrappers for Parquet files and chooses table creation by flavor.

Main functions:

- `createParquetTable()`

- `createTable()` to create the concrete table according to flavor


#### 5. `ParquetTable`

Base table abstraction containing metadata and other shared information.

Main elements:

- `Flavor` enum


#### 6. `ParquetFilterableTable` extends `ParquetTable`

Currently a placeholder or dummy implementation for filterable behavior.

#### 7. `ParquetScannableTable` extends `ParquetTable`

Currently a placeholder or dummy implementation for scannable behavior.

#### 8. `ParquetTranslatableTable` extends `ParquetTable`

Currently a placeholder or dummy implementation for translatable behavior.



### `model`

Contains Parquet-to-Polypheny type conversion logic.

#### 9. `ParquetTypeConverter`

Converts Parquet types to Polypheny types.

Main function:

- `fromParquetTypeToPolyType()`


### `planning`

Contains relational scan and planner rule classes for Parquet query support.

#### 10. `ParquetScan`

Represents the Parquet scan operator in the planning/execution layer.

#### 11. `ParquetProjectScanRule`

Planner rule associated with Parquet scans.


### `util`

Contains reusable infrastructure helpers such as Hadoop configuration setup for Parquet reading.

#### 12. `HadoopConfigurationFactory`

Creates Hadoop `Configuration` instances for Parquet readers and ensures the proper classloader and local filesystem registration are available in the plugin environment.



## Schema Extraction and Naming Rules

- Table names are derived from Parquet file names and normalized through `getValidTableName()`.

- Column metadata is extracted from Parquet footer/schema metadata.

- Column names are normalized through `getValidColumnNameFromField()`.

- Type mapping is delegated to `ParquetTypeConverter`.

- The document should explicitly mention how unsupported or ambiguous Parquet types are handled.


## Flavor Selection

Appropriate table wrapper is created for a discovered Parquet file, for example `filterable`, `scannable`, or `translatable`.
Currently only one flavor is active in practice.


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



## Current Gaps and Future Work

- Replace placeholder filterable, scannable, and translatable table variants with real implementations.
- Add explicit support for Hadoop-backed remote filesystems.
- Add write support
- Handle nested parquet schema
- implement partition discovery (?)
- Document error handling for invalid files, missing metadata, and unsupported types.
- Add tests and document the tested scenarios for discovery, schema extraction, restore, and planning.

