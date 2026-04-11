# Parquet Adapter Implementation

---

## Overview

The Parquet adapter exposes `.parquet` files as Polypheny source tables.
At a high level, the adapter is responsible for:
- discovering Parquet files from the configured source location
- extracting relational schema metadata from Parquet file footers
- restoring and registering tables whose data is read from Parquet files
- executing read queries over Parquet data
- applying projection pushdown and supported native predicate pushdown during query execution

The adapter is read-only and supports relational and document source integration.

The following sections describe the architecture, execution flow, package responsibilities, supported query behavior, and current limitations in more detail.

## Build and Integration Changes

1\. To integrate the Parquet adapter as a plugin, the Gradle configuration must include the Parquet module and its dependencies.

Affected files:
- `settings.gradle`
- `gradle.properties`
- `plugins/parquet-adapter/build.gradle`


2\. Optional: To make Parquet the default data source, `PolyphenyDB.java` sets `defaultSourceName` to `parquet`.<br>
During `restore()`, `Catalog.defaultSource` is populated from the configured adapter state.


## Execution Flow

1. **Plugin registration**
    - **Responsible class:** `ParquetPlugin`
    - Registers the Parquet adapter template during startup.
    - Removes the template during shutdown.
2. **Schema initialization**
    - **Responsible classes:** `AbstractParquetSource`, `ParquetFileDiscovery`, `ParquetTypeConverter`
    - Resolves the configured Parquet directory or file location.
    - Discovers available `.parquet` files.
    - Builds relational table or document collection metadata from Parquet file metadata.
    - Maps Parquet field types to Polypheny types.
    - Normalizes physical table names and column names.
    - Initializes the information page from the extracted column definitions.
3. **Namespace and entity creation**
    - **Responsible classes:** `ParquetNamespace`, `ParquetRelTable`, `ParquetDocument`
    - Creates table or collection wrappers for discovered Parquet files.
    - Registers physical entities in the adapter catalog.
4. **Restore during startup**
    - **Responsible classes:** `ParquetRelationalSource`, `ParquetDocumentSource`, `ParquetNamespace`
    - Recreates relational tables and document collections from persisted adapter state.
    - Re-registers the wrappers in the adapter catalog.
5. **Query entry into the adapter**
    - **Responsible classes:** `ParquetRelScan`, `ParquetDocScan`, `AbstractParquetEnumerator`, `ParquetGroupReader`
    - The query engine resolves the registered Parquet-backed physical entity.
    - Projection and supported filter information are passed into the Parquet execution layer.
    - Rows are read from the Parquet file and converted into Polypheny relational tuples or documents.

## Supported Filter Conditions

The active predicate pushdown path currently supports only simple comparison conditions that can be translated into native Parquet predicates.

Supported operators:
- `=`
- `!=`
- `>`
- `>=`
- `<`
- `<=`

Supported condition shape:
- `column OP literal`
- `column OP dynamic-parameter`

Notes:
- The left-hand side must be a column reference.
- The right-hand side must be a literal value or dynamic parameter.
- Simple casts on either side are unwrapped before translation.
- Only pushdown-safe conditions are translated into adapter-specific filters.
- Unsupported conditions remain outside the Parquet-specific filter flow.

Supported pushdown types:
- `BOOLEAN`: `=` and `!=`
- `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DATE`, `TIME`, `TIMESTAMP`: all six comparison operators
- `VARCHAR`, `CHAR`, `TEXT`: `=` and `!=`

Notes:
- Pushdown uses Parquet native filter APIs through `FilterCompat.Filter`.
- Legacy Parquet `INT96` timestamp columns are currently not pushed down.


## Parquet Adapter - Package Structure

### `org.polypheny.db.adapter.parquet`

Contains parquet functionality for Relational and Document data source.

- `ParquetPlugin` - plugin entry point. Registers the relational and document adapter templates after catalog initialization and removes them again during shutdown.

### `org.polypheny.db.adapter.parquet.document`

Document-model integration for exposing Parquet files as read-only Polypheny document collections.

- `ParquetDocumentSource` - document adapter implementation. Owns adapter metadata, performs discovery of Parquet-backed document collections, document restore, and delegation into the document scan APIs.

#### `document.execution`

- `ParquetDocEnumerator` - document runtime enumerator. Reads Parquet rows and create from each row a single `PolyDocument`.
- `ParquetDocFilterTranslator` - translates supported document filter expressions into adapter-level `AdapterFilter` instances.
- `ParquetDocValueExtractor` - converts Parquet groups and nested values into Polypheny document values and synthesizes `_id` values when they are missing.

#### `document.planning`

- `ParquetDocFilter` - enumerable filter node used when a document filter can stay inside the Parquet-specific execution path.
- `ParquetDocFilterRule` - planner rule that recognizes supported document filters and rewrites them into `ParquetDocFilter` plus `ParquetDocScan`.
- `ParquetDocScan` - document scan algebra node that builds the enumerable execution call for reading Parquet-backed documents.

#### `document.schema`

- `ParquetDocument` - physical collection wrapper for the document model. Represents one Parquet-backed collection inside Polypheny and connects planning and execution to the source adapter.

### `org.polypheny.db.adapter.parquet.relational`

Relational-model integration for exposing Parquet files as source tables.

- `ParquetRelationalSource` - relational adapter implementation. Manages exported table discovery, schema registration, information-page content, and restore of relational Parquet tables.

#### `relational.execution`

- `ParquetRelEnumerator` - relational runtime enumerator. Reads projected Parquet rows and returns them as relational tuples.
- `ParquetRelFilterTranslator` - converts adapter-level filters into Parquet native predicates for relational scans.
- `ParquetRelValueExtractor` - converts Parquet field values into Polypheny relational values with relational-specific scalar handling.

#### `relational.planning`

- `ParquetRelScan` - relational scan algebra node for Parquet-backed physical tables.
- `ParquetRelScanRule` - planner rule that rewrites compatible scans and projections to the Parquet-specific relational scan node.

#### `relational.schema`

- `ParquetRelTable` - physical table wrapper for the relational model. Exposes the Parquet-backed table to Polypheny and ties the planner, scanner, and adapter metadata together.

### `org.polypheny.db.adapter.parquet.shared`

Shared infrastructure used by both the relational and document adapters.

- `AbstractParquetSource` - common base class for both source types. Handles settings, file discovery, exported schema derivation, information-page setup, name normalization, and shared restore behavior.

#### `shared.execution`

- `AbstractParquetEnumerator` - common enumerator base. Manages row iteration, projection handling, cancellation support, and reader lifecycle for both models.
- `AbstractParquetValueExtractor` - shared conversion base for mapping Parquet primitive and structured values into Polypheny values.
- `ParquetFilterTranslationSupport` - reusable helper for parsing supported Rex predicates and turning them into adapter-level filters.
- `ParquetValueExtractor` - small interface implemented by value extractors that can convert a Parquet field into a `PolyValue`.

#### `shared.io`

- `ParquetSourceReader` - low-level Parquet row-group reader. Opens the file, applies projection and native predicates, and streams groups to the enumerators.
- `ParquetFileDiscovery` - locates valid `.parquet` files below the configured source location and filters out unsupported files.

#### `shared.filter`

- `ParquetAdapterFilter` - immutable filter description shared across planning and execution. Carries the target column index, comparison operator, literal value, and optional dynamic parameter index.
- `ParquetNativeFilterBuilder`- creates native Parquet filter predicates for the supported comparison operators and value types.

#### `shared.schema`

- `ParquetNamespace` - namespace helper that creates the correct Parquet-backed physical wrapper for either a relational table or a document collection.
- `ParquetTypeConverter` - converts Parquet schema types and runtime values into the Polypheny type system.
- `ParquetFieldNameNormalizer` - creates normalized filed names

#### `shared.util`

- `HadoopConfigurationFactory` - builds Hadoop `Configuration` instances that work correctly inside the plugin classloader environment.


## Schema Mapping Rules

- Table names are derived from Parquet file names and normalized before registration.
- Column names are derived from Parquet field names and normalized before registration.
- Column types are mapped from Parquet schema types to Polypheny types.
- String-like Parquet fields are mapped to `TEXT`.
- Unlike SQL types with fixed length declarations, Parquet does not impose a fixed maximum string length.


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




# Workflow

---

## 1. Extract Parquet

- ### ParquetExtractActivity

`org.polypheny.db.workflow.dag.activities.impl.extract.ParquetExtractActivity`

Workflow node definition for reading Parquet files into a workflow output pipe:
1. It defines how the activity appears in the workflow UI using settings. This provides all needed information for editor to show options.
2. It decides what type of data the node will output: Relational or Document
3. It executes the extraction at runtime as follows: the activity resolves the input files, iterates over them, and delegates the actual reading/conversion work to `ParquetWorkflowSupport.java`, which turns Parquet rows into either documents or relational rows and pushes them into the workflow output pipe.

#### ***Settings***

#### `file`
- Display name: `File Location`
- Type: file setting
- Required: yes
- Multiple values: yes
- Supported source types:
    - absolute file path
    - URL
- Purpose:
  Select one or more Parquet files, or a folder containing Parquet files, to extract from.
- Behavior:
  If multiple files are selected, the activity outputs the union of their rows.

#### `outputModel`
- Display name: `Output Type`
- Type: enum
- Required: yes
- Default: `document`
- Available values:
    - `document`
    - `relational`
- Purpose:
  Defines whether the extracted Parquet rows are exposed as workflow documents or as a relational table.

#### `nameField`
- Display name: `Add File Name Field`
- Type: boolean
- Required: no
- Default: `false`
- Purpose:
  Adds the source file name to the output.
- Behavior:
    - in document mode: adds a `fileName` field
    - in relational mode: adds a `fileName` column

#### `maxCount`
- Display name: `Maximum Row Count`
- Type: integer
- Required: no
- Default: `-1`
- Minimum: `-1`
- Group: advanced
- Purpose:
  Limits how many rows are extracted per file.
- Behavior:
    - `-1`: extract all rows
    - `>= 0`: extract at most that many rows from each selected file

#### ***Notes***

- The activity has no input ports and one output port of type `ANY`.
- The concrete output type is determined from `outputModel`.
- In relational mode, the schema is derived from the selected Parquet file.
- In document mode, the output is a document stream with generated document IDs where needed.
- Extract multiple files allowed only for Document model and Relational files with same schema

![Schema display](images/extract_workflow_act_settings.png)

- ### ParquetWorkflowExtractSupport

`org.polypheny.db.workflow.parquet.ParquetWorkflowExtractSupport`

Helper class contains Parquet extraction logic for the workflow engine.
It maps Parquet schemas to workflow output types, estimates row counts, generates dynamic activity names, and converts Parquet rows into either Polypheny documents or relational tuples.
Calls logic of Parquet Adapter Module


### Tests:

- Read parquet file as Relational Data Source and store to CSV file
- Read parquet file as Document Data Source and store to JSON file
- Add file name as column
- Add key id column
- Set Maximum number of rows = 5 in Advanced settings
- Extract multiple documents
- Extract multiple relational - same schema
- Extract multiple relational - different schema - not allowed


![Schema display](images/workflow1.png)

![Schema display](images/workflow2.png)

## 2. Load to Parquet File

- ### ParquetLoadActivity

`org.polypheny.db.workflow.dag.activities.impl.load.ParquetLoadActivity`

Workflow node definition for exporting workflow input into a Parquet file:
1. It defines how the activity appears in the workflow UI using settings. This provides all needed information for the editor to show the available export options.
2. It validates whether the connected workflow input is supported. Graph input is rejected and relational input is checked so that exporting without the workflow primary key does not produce an empty Parquet schema.
3. It executes the export at runtime as follows: the activity resolves the configured target file and export settings, prepares the output path, and delegates the actual schema inference and data conversion work to `ParquetWorkflowLoadSupport.java`.

#### ***Settings***

#### `file`
- Display name: `Target File`
- Type: file setting
- Required: yes
- Multiple values: no
- Supported source types:
    - absolute file path
- Purpose:
  Select the Parquet file that should be written.

#### `mode`
- Display name: `Handling of Existing File`
- Type: enum
- Required: yes
- Default: `fail`
- Available values:
    - `drop`
    - `fail`
- Purpose:
  Defines the behavior if the selected output file already exists.
- Behavior:
    - `drop`: overwrite the existing file
    - `fail`: abort the activity

#### `compression`
- Display name: `Compression`
- Type: enum
- Required: yes
- Default: `snappy`
- Available values:
    - `snappy`
    - `gzip`
    - `uncompressed`
- Purpose:
  Selects the Parquet compression codec used for the generated file.

#### `schemaSampleSize`
- Display name: `Schema Sample Size`
- Type: integer
- Required: yes
- Default: `100`
- Minimum: `1`
- Purpose:
  Controls how many input tuples are sampled to infer the Parquet schema before writing starts.

#### `conflictMode`
- Display name: `Conflict Mode Handling`
- Type: enum
- Required: yes
- Default: `stringify`
- Available values:
    - `stringify`
    - `fail`
- Purpose:
  Defines how incompatible sampled values should be handled while inferring the Parquet schema.
- Behavior:
    - `stringify`: use a shared string field when incompatible sampled values are encountered
    - `fail`: abort the activity when sampled values are incompatible

#### `keepId`
- Display name: `Include ID Field`
- Type: boolean
- Required: no
- Default: `true`
- Purpose:
  Keeps the `_id` field when the activity receives document input.

#### `keepPk`
- Display name: `Keep Primary Key Column`
- Type: boolean
- Required: no
- Default: `false`
- Purpose:
  Keeps the workflow primary key column when the activity receives relational input.

#### ***Notes***

- The activity has one input port of type `ANY` and no output ports.
- Relational input and document input are both supported.
- Graph input is not supported.
- For relational input, the workflow primary key can optionally be excluded.
- For document input, the schema is inferred from sampled documents because the document type does not provide field-level schema information.

![Schema display](images/load_workflow_act_settings.png)

![Schema display](images/extract_workflow_mult_validation.png)

- ### ParquetWorkflowLoadSupport

`org.polypheny.db.workflow.parquet.ParquetWorkflowLoadSupport`

Helper class that contains Parquet export logic for the workflow engine.
It prepares output files, builds dynamic activity names, infers Parquet schema from sampled workflow input, maps workflow values to Parquet schema definitions, and delegates low-level file writing to the shared Parquet adapter module.

- ### ParquetSourceWriter

`org.polypheny.db.adapter.parquet.shared.io.ParquetSourceWriter`

Shared low-level Parquet writer used by workflow export functionality.
It owns the Parquet writer lifecycle, creates Parquet row groups, writes rows to the target file, applies the configured compression codec, and exposes a small progress callback hook without depending on workflow-specific classes.

- ### ParquetMessageTypeBuilder

`org.polypheny.db.adapter.parquet.shared.schema.inference.ParquetMessageTypeBuilder`

Builds a Parquet `MessageType` from inferred workflow field schemas, including primitive, repeated, and nested group fields.

- ### Schema Inference Model

`org.polypheny.db.adapter.parquet.shared.schema.inference.FieldSchema`

Internal model for one exported Parquet field. It tracks the source field name, normalized Parquet field name, optional relational source index, and inferred value schema.

`org.polypheny.db.adapter.parquet.shared.schema.inference.ValueSchema`

Recursive internal model of a field value. It represents primitive values, nested groups, and repeated values before they are converted into a final Parquet schema.

`org.polypheny.db.adapter.parquet.shared.schema.inference.SchemaState`

Container for the currently inferred schema during export. It accumulates field definitions while sampled rows or documents are inspected.

### Tests:

- Read relational workflow input and export to Parquet file
- Read document workflow input and export to Parquet file
- Keep `_id` for document export
- Keep workflow primary key for relational export
- Write with different compression codecs
- Use `schemaSampleSize = 5`
- Export incompatible sampled values with `conflictMode = stringify`
- Fail export on incompatible sampled values with `conflictMode = fail`

![Schema display](images/workflow_parquet_parquet.png)

# Unit Tests

---

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

## `ParquetRelFilterTranslatorTest.java`
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
