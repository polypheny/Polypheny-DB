# Test Coverage

This document tracks the existing Parquet-related tests. The main inventory is grouped by feature area.

## Discovery And Schema Normalization

| Test                                                                   | File                            | What It Checks                                                                                           |
|------------------------------------------------------------------------|---------------------------------|----------------------------------------------------------------------------------------------------------|
| `groupsSchemaEvolvedFilesIntoOneTable`                                 | `ParquetFileDiscoveryTest.java` | Groups compatible schema-evolved Parquet files into one discovered table and keeps the union of columns. |
| `keepsUnrelatedFilesAsSeparateTables`                                  | `ParquetFileDiscoveryTest.java` | Keeps Parquet files with unrelated schemas as separate discovered tables.                                |
| `discoversPartitionFoldersAsOneTable`                                  | `ParquetFileDiscoveryTest.java` | Discovers Hive-style partition folders as one table and exposes folder values as columns.                |
| `discoversPartitionedTableFoldersUnderRoot`                            | `ParquetFileDiscoveryTest.java` | Discovers multiple table folders below a dataset root, including a partitioned table folder.             |
| `hidesParquetColumnWhenItMatchesPartitionValue`                        | `ParquetFileDiscoveryTest.java` | Hides a physical Parquet column when it duplicates a matching folder partition value.                    |
| `hidesPhysicalColumnWhenNormalizedPartitionNameCollidesAndValuesMatch`  | `ParquetFileDiscoveryTest.java` | Hides a physical column when a normalized partition folder key collides with it and values match.        |
| `failsWhenParquetColumnConflictsWithPartitionValue`                    | `ParquetFileDiscoveryTest.java` | Rejects a file when a physical Parquet column conflicts with the folder partition value.                 |
| `normalizedSchemaUsesDiscoveredMultiFileTable`                         | `ParquetFileDiscoveryTest.java` | Normalizes a multi-file partitioned table and adds the synthetic row-id column.                          |
| `flatDiscoveryFlattensNonRepeatedNestedColumnsAndKeepsRepeatedColumns` | `ParquetFileDiscoveryTest.java` | Flattens non-repeated nested fields in flat mode and keeps repeated fields as document columns.          |
| `flatDiscoveryKeepsOrdersDbPrimaryKeys`                                | `ParquetFileDiscoveryTest.java` | Verifies primary-key detection for the bundled `orders_db` flat tables.                                  |

## Filter Translation And Containers

| Test                                                | File                                  | What It Checks                                                                                          |
|-----------------------------------------------------|---------------------------------------|---------------------------------------------------------------------------------------------------------|
| `removesOnlyAdapterFilterPaths`                     | `FiltersContainerTest.java`           | Removes path elements only from adapter filters while keeping native filter paths intact.               |
| `keepsReaderParentFilterForAdapterValidation`       | `JoinFiltersSplitterTest.java`        | Keeps a parent-side filter available both for native Parquet reading and parent-row adapter validation. |
| `keepsCrossSideOrAsJoinedRowFilter`                 | `JoinFiltersSplitterTest.java`        | Keeps an `OR` spanning parent and child sides as a joined-row adapter filter.                           |
| `translatesAndOrTree`                               | `ParquetRelFilterTranslatorTest.java` | Translates nested `AND`/`OR` Rex filters into Parquet adapter filter trees.                             |
| `translatesInAsOrOfEquals`                          | `ParquetRelFilterTranslatorTest.java` | Rewrites `IN` filters as an `OR` of equality filters.                                                   |
| `translatesNullChecks`                              | `ParquetRelFilterTranslatorTest.java` | Translates `IS NULL` and `IS NOT NULL` predicates.                                                      |
| `evaluatesNullChecks`                               | `ParquetRelFilterTranslatorTest.java` | Evaluates Parquet null-check filter semantics against null and non-null values.                         |
| `reversesComparisonWhenLiteralIsOnTheLeft`          | `ParquetRelFilterTranslatorTest.java` | Reverses comparison direction when the literal or dynamic parameter appears on the left side.           |
| `ignoresUnsupportedInt96TimestampPredicatePushdown` | `ParquetRelFilterTranslatorTest.java` | Avoids failing native predicate creation for unsupported INT96 timestamp pushdown.                      |
| `buildsNativeNullCheckPredicates`                   | `ParquetRelFilterTranslatorTest.java` | Builds native Parquet predicates for supported null checks.                                             |
| `docFilterTranslatorTranslatesTopLevelNameAndRejectsNestedPaths` | `ParquetSharedDocumentAdapterTest.java` | Translates top-level document field filters and avoids unsupported nested-path pushdown.                |

## File-Level Filter Pruning

| Test                                                         | File                            | What It Checks                                                                                                  |
|--------------------------------------------------------------|---------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `partitionEvaluatorMatchesValuesAndTreatsMissingPartitionsAsNull` | `ParquetFilterPruningTest.java` | Evaluates partition-value filters, mismatches, missing partition values, and non-partition fallback.            |
| `statisticsEvaluatorPrunesByRangeAndProvesConstantFiles`     | `ParquetFilterPruningTest.java` | Uses footer min/max/null statistics to reject files, prove constant-file matches, and keep unknown cases.       |
| `reducerDropsExactMatchesRejectsExactFailuresAndKeepsResiduals` | `ParquetFilterPruningTest.java` | Removes exact file-level matches, rejects impossible files, and preserves residual filters for `OR` and `NOT`. |
| `filterResolverMapsProjectionIndexesAndResolvesDynamicParameters` | `ParquetFilterPruningTest.java` | Rewrites projected/physical filter indexes and resolves dynamic parameters with source path bindings.          |
| `primitiveValueFilterEvaluatorHandlesScalarComparisonsAndUnknownIndexes` | `ParquetExecutionAndWriterTest.java` | Evaluates primitive row filter comparisons for numeric, boolean, binary, null, default, and unknown-index cases. |

## Primitive Reading And Projection

| Test                                                                       | File                                       | What It Checks                                                                                             |
|----------------------------------------------------------------------------|--------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `readsPrimitiveProjectionWithoutGroupMaterialization`                      | `ParquetPrimitiveRowReaderTest.java`       | Reads a flat primitive projection directly, including nullable values, without full group materialization. |
| `rejectsRepeatedProjection`                                                | `ParquetPrimitiveRowReaderTest.java`       | Rejects repeated fields for the primitive-row fast path.                                                   |
| `nestedListTableProjectsParquetRootFieldsFromBindings`                     | `ParquetNestedRepeatedProjectionTest.java` | Projects repeated nested list rows using table bindings and generated row/parent/ordinal fields.           |
| `flatTableProjectsNonRepeatedNestedFieldsAndRepeatedDocumentsFromBindings` | `ParquetNestedRepeatedProjectionTest.java` | Projects flattened nested scalar fields and repeated document fields from binding paths.                   |

## Document Adapter Behavior

| Test                                                    | File                              | What It Checks                                                                                  |
|---------------------------------------------------------|-----------------------------------|-------------------------------------------------------------------------------------------------|
| `extractDocumentNormalizesNestedValuesAndGeneratesMissingId` | `ParquetSharedDocumentAdapterTest.java` | Converts Parquet rows into documents, normalizes field names, preserves nested values, and adds `_id`. |
| `extractDocumentPreservesSourceIdAndGeneratesIdForAllNullRows` | `ParquetSharedDocumentAdapterTest.java` | Preserves source `_id` values and generates `_id` for rows where all optional fields are absent. |
| `extractDocumentConvertsRepeatedNestedGroupsToDocumentLists` | `ParquetSharedDocumentAdapterTest.java` | Converts repeated nested Parquet groups into `PolyList` values containing nested documents. |
| `docEnumeratorAppliesFiltersAndGeneratesIds`            | `ParquetSharedDocumentAdapterTest.java` | Enumerates document rows through real Parquet IO, applies filters, and creates generated row ids. |

## Document Planning And Schema

| Test                                                    | File                               | What It Checks                                                                                   |
|---------------------------------------------------------|------------------------------------|--------------------------------------------------------------------------------------------------|
| `documentBuildsTupleTypeFromExportedColumnsAndFallsBackToDocumentId` | `ParquetDocumentPlanningTest.java` | Builds document tuple types from exported columns and falls back to the `_id` document type.      |
| `docScanCopiesFiltersDerivesDocumentRowAndRegistersFilterRule` | `ParquetDocumentPlanningTest.java` | Copies document scan filters, derives the document row shape, includes filters in compare strings, and registers the filter rule. |
| `docFilterCopyPreservesEntityAndDelegatesToScanInput`  | `ParquetDocumentPlanningTest.java` | Copies Parquet document filter nodes while preserving the entity and input scan.                  |
| `docFilterRuleSplitsNestedAndConjunctions`             | `ParquetDocumentPlanningTest.java` | Splits nested `AND` Rex conditions into individual document filter predicates.                    |

## Schema And Binding Utilities

| Test                                                       | File                               | What It Checks                                                                                           |
|------------------------------------------------------------|------------------------------------|----------------------------------------------------------------------------------------------------------|
| `bindingSerializerRoundTripsSourceFilesPartitionsColumnsAndStatistics` | `ParquetSchemaUtilitiesTest.java` | Round-trips table bindings with encoded paths, partitions, column roles, and metadata statistics.       |
| `nameNormalizerNormalizesAndUniquifiesNestedFields`        | `ParquetSchemaUtilitiesTest.java`  | Normalizes physical table/field names and resolves duplicate top-level and nested Parquet names.        |
| `typeConverterConvertsTypedStringsAndComparesNumerically`  | `ParquetSchemaUtilitiesTest.java`  | Converts string statistics into typed PolyValues and compares numeric strings by numeric value.         |
| `schemaStateSkipsPrimaryKeyAndStringifiesConflictingSampleValues` | `ParquetSchemaUtilitiesTest.java` | Initializes relational schema inference, skips workflow PK export, and stringifies incompatible samples. |
| `valueSchemaMergesNestedFieldsAndWidensNumericTypes`       | `ParquetSchemaUtilitiesTest.java`  | Merges nested schemas and widens numeric value schemas during inference.                                |
| `messageTypeBuilderBuildsRepeatedAndNestedFields`          | `ParquetSchemaUtilitiesTest.java`  | Builds repeated and nested Parquet fields and rejects empty Parquet schemas.                            |
| `namespaceCreatesRootBindingAndEntityWrappers`             | `ParquetTableStatisticsReaderTest.java` | Creates namespace root table bindings from a physical table and wraps them in a `ParquetRelTable`.      |

## IO Utilities

| Test                                                    | File                         | What It Checks                                                                                 |
|---------------------------------------------------------|------------------------------|------------------------------------------------------------------------------------------------|
| `cancellationChecksOnlyAtConfiguredRowIntervals`        | `ParquetIoUtilitiesTest.java` | Checks cancellation only at the configured row interval and respects the cancellation flag.     |
| `urlResolverNormalizesDirectoriesAndKeepsExplicitParquetFiles` | `ParquetIoUtilitiesTest.java` | Normalizes directory URLs, preserves explicit `.parquet` source URLs, and resolves child files. |
| `bufferedIteratorReplaysSampleRowsBeforeConvertedRemainder` | `ParquetExecutionAndWriterTest.java` | Replays buffered schema-sample rows before converting the remaining iterator values. |
| `virtualGroupDelegatesToSourceGroupAndKeepsMetadata` | `ParquetExecutionAndWriterTest.java` | Delegates Parquet group access through `VirtualGroup` and preserves synthetic row metadata. |
| `combinedGroupRoutesFieldsByJoinSideAndRejectsIndexedAccess` | `ParquetExecutionAndWriterTest.java` | Routes joined parent/child fields by join side, detects null join rows, and rejects direct indexed access. |
| `filterableSourceFileDefensivelyCopiesFilters` | `ParquetExecutionAndWriterTest.java` | Defensively copies filter lists attached to source files. |
| `sourceWriterRoundTripsRelationalRowsThroughRelEnumerator` | `ParquetExecutionAndWriterTest.java` | Writes relational rows with `ParquetSourceWriter` and reads them back through `ParquetRelEnumerator`. |
| `sourceWriterRoundTripsDocumentsAndGeneratesMissingIds` | `ParquetExecutionAndWriterTest.java` | Writes document rows, skips source `_id` when requested, reads values back, and generates missing ids. |
| `sourceWriterReportsCompressionAndValueConversionFailures` | `ParquetExecutionAndWriterTest.java` | Reports unknown compression settings and incompatible value/schema writes as runtime errors. |
| `compileReturnsAlwaysTrueForEmptyFiltersAndRejectsUnsupportedFilters` | `ParquetPrimitivePredicateTest.java` | Compiles empty primitive predicates and rejects invalid indexes, unsupported operators, null comparisons, and unsupported logical `OR`. |
| `compiledAndPredicateReadsColumnsOnceAndReusesConsumedValues` | `ParquetPrimitivePredicateTest.java` | Compiles primitive `AND` predicates, reads int/binary/boolean values once, and reuses consumed cached values. |
| `nullPredicatesMatchAbsentAndPresentValues` | `ParquetPrimitivePredicateTest.java` | Evaluates primitive `IS NULL` and `IS NOT NULL` predicates for absent and present optional values. |

## Aggregates

| Test                                                 | File                                            | What It Checks                                                                                     |
|------------------------------------------------------|-------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `readsPrimitiveColumnAggregateResults`               | `ParquetNoFilterColumnAggregateReaderTest.java` | Reads SUM, COUNT, MIN, and MAX for primitive numeric columns without row filters.                  |
| `rejectsNonNumericProjection`                        | `ParquetNoFilterColumnAggregateReaderTest.java` | Rejects non-numeric projections for the no-filter column aggregate reader.                         |
| `supportsDistinctOnlyGroupBy`                        | `ParquetGroupedAggregateRelEnumeratorTest.java` | Supports distinct-only grouped output without aggregate calls.                                     |
| `scalarCountReturnsZeroWhenStatisticsPruneEveryFile` | `ParquetGroupedAggregateRelEnumeratorTest.java` | Returns scalar `COUNT(*) = 0` when statistics/file pruning rejects all files.                      |
| `supportsCountStarWithSumMinAndMax`                  | `ParquetRowAggregateRelEnumeratorTest.java`     | Computes grouped row aggregates with `COUNT(*)`, `SUM`, `MIN`, and `MAX`, including null handling. |
| `supportsMultipleGroupingColumnsAndNullGroupKeys`     | `ParquetRowAggregateRelEnumeratorTest.java`     | Aggregates with multiple grouping columns and preserves groups whose key contains null values.     |
| `evaluatesPredicateForEveryRow`                      | `ParquetCountAggregatePageReaderTest.java`      | Applies a primitive predicate to every row while counting through the page reader.                 |
| `aggregateInputColumnAddsPresentNumericValuesAndSkipsNulls` | `AggregateAccumulatorTest.java` | Adds present numeric column values to count/sum/min/max slots and skips optional null values. |
| `aggregateInputColumnSharesConsumedValuesAcrossCalls` | `AggregateAccumulatorTest.java` | Reuses consumed numeric column values across multiple aggregate calls without rereading the column. |
| `aggregateRowAccumulatorBuildsCountStarAndColumnAggregates` | `AggregateAccumulatorTest.java` | Builds row accumulators from aggregate descriptors and column descriptors. |
| `aggregateInputColumnRejectsUnsupportedPrimitiveTypes` | `AggregateAccumulatorTest.java` | Rejects unsupported primitive types for numeric aggregate input columns. |
| `aggregateGroupStateHandlesObjectRowsAndMergesPartialStates` | `AggregateAccumulatorTest.java` | Aggregates object rows, ignores null values, and merges partial aggregate states. |
| `aggregateDecompositionExposesPartialCallsAndFinalFunctions` | `ParquetAggregateOptimizationTest.java` | Exposes partial aggregate calls and their final aggregation functions for aggregate decomposition. |

## Metadata And File-Constant Aggregates

| Test                                                                      | File                                | What It Checks                                                                                           |
|---------------------------------------------------------------------------|-------------------------------------|----------------------------------------------------------------------------------------------------------|
| `metadataAggregateAllowsPartitionFiltersAndGroups`                        | `ParquetMetadataAggregateTest.java` | Accepts metadata aggregates with partition filters/groups and rejects unsupported data-column cases.     |
| `projectedScanUsesFolderPartitionValuesWhenPhysicalColumnsCollide`        | `ParquetMetadataAggregateTest.java` | Uses folder partition values when physical Parquet columns with the same names contain different values. |
| `projectedScanReadsMinimalDataColumnWhenOnlyPartitionColumnsAreProjected` | `ParquetMetadataAggregateTest.java` | Reads a minimal backing data column when projecting only partition columns.                              |
| `metadataAggregateUsesFileConstantPhysicalFilters`                        | `ParquetMetadataAggregateTest.java` | Executes metadata aggregate counts using dynamic filters on file-constant physical columns.              |
| `metadataAggregateRejectsMixedPhysicalFilterColumns`                      | `ParquetMetadataAggregateTest.java` | Rejects metadata aggregation when a physical column is not constant across the relevant file.            |
| `metadataAggregateGroupsByFileConstantPhysicalColumns`                    | `ParquetMetadataAggregateTest.java` | Groups metadata aggregate output by file-constant physical columns.                                      |
| `fileGroupedAggregateGroupsPhysicalConstantColumns`                       | `ParquetMetadataAggregateTest.java` | Groups file-level aggregate results by physical columns that are constant per file.                      |
| `streamingGroupedAggregateSkipsFileConstantPhysicalFilter`                | `ParquetMetadataAggregateTest.java` | Drops file-constant filters from residual row evaluation after file pruning.                             |
| `streamingGroupedAggregateRetainsResidualAndFilter`                       | `ParquetMetadataAggregateTest.java` | Keeps residual row filters when an `AND` also contains file-constant predicates.                         |
| `streamingGroupedAggregateRetainsResidualOrFilter`                        | `ParquetMetadataAggregateTest.java` | Keeps residual row filters for `OR` predicates that cannot be fully decided at file level.               |
| `fileGroupedScalarCountReturnsZeroWhenFiltersRejectEveryFile`             | `ParquetMetadataAggregateTest.java` | Returns scalar `COUNT(*) = 0` when file-level grouped aggregate filters reject every file.               |
| `constantColumnResolverRejectsMixedNullAndNonNullValues`                  | `ParquetMetadataAggregateTest.java` | Rejects file-constant resolution when statistics show mixed null and non-null values.                    |

## Statistics Provider

| Test                                                   | File                                  | What It Checks                                                                                         |
|--------------------------------------------------------|---------------------------------------|--------------------------------------------------------------------------------------------------------|
| `providedColumnStatisticsDefensivelyCopiesUniqueValues` | `ParquetTableStatisticsReaderTest.java` | Defensively copies adapter-provided unique values and handles null unique-value input.                 |
| `readerAggregatesDataAndPartitionColumnStatistics`     | `ParquetTableStatisticsReaderTest.java` | Aggregates metadata counts/ranges for data columns and derives bounded partition-column unique values. |
| `columnStatisticsReaderConvertsTemporalAndBinaryFooterStatistics` | `ParquetTableStatisticsReaderTest.java` | Converts footer statistics for date, time, timestamp, and binary Parquet columns.                       |
| `readerFallsBackForMissingColumnsAndUnreliableRanges`  | `ParquetTableStatisticsReaderTest.java` | Returns full/null-range statistics for missing columns and type-mismatched or unreliable ranges.       |
| `nestedEntityStatisticsUsesLargestNestedValueCount`    | `ParquetTableStatisticsReaderTest.java` | Estimates nested-table row count from the largest nested data-column value count.                       |
| `providedEntityStatisticsAllowsUnknownRowCount`        | `ParquetTableStatisticsReaderTest.java` | Allows adapter-provided entity statistics to represent unknown row counts.                              |
| `parquetRelTableProvidesStatisticsOnlyForMatchingLogicalEntity` | `ParquetTableStatisticsReaderTest.java` | Exposes entity/column statistics through `ParquetRelTable` only for matching logical table ids.        |

## Nested Join Planning

| Test                                            | File                                 | What It Checks                                                                                |
|-------------------------------------------------|--------------------------------------|-----------------------------------------------------------------------------------------------|
| `plansFullJoinWithProjectedFilteredParentInput` | `ParquetNestedJoinPlanningTest.java` | Executes a full parent-child join where the parent input is projected and filtered.           |
| `appliesLimitAfterFullJoinFilter`               | `ParquetNestedJoinPlanningTest.java` | Applies `LIMIT` correctly after a filtered full parent-child join.                            |
| `plansFullJoinWithLimitAsParquetJoin`           | `ParquetNestedJoinPlanningTest.java` | Confirms a full parent-child join with limit plans as `ParquetRelJoin`, not `EnumerableJoin`. |
| `plansLeftJoinWithLimitAsParquetJoin`           | `ParquetNestedJoinPlanningTest.java` | Confirms a left parent-child join with limit plans as `ParquetRelJoin`, not `EnumerableJoin`. |

## Plugin Integration

| Test                                    | File                     | What It Checks                                                                                         |
|-----------------------------------------|--------------------------|--------------------------------------------------------------------------------------------------------|
| `importsAllTablesAndReadsRows`          | `ParquetPluginTest.java` | Creates a Parquet source from bundled `orders_db` resources and verifies imported tables contain rows. |
| `parquetSourceIsReadOnly`               | `ParquetPluginTest.java` | Rejects `DELETE` statements against a Parquet source table.                                            |
| `readsExpectedRowsFromCustomers`        | `ParquetPluginTest.java` | Reads known customer rows and validates values, including timestamp conversion.                        |
| `filtersRowsWithWhereClause`            | `ParquetPluginTest.java` | Executes a text equality filter with ordering and limit.                                               |
| `projectsOnlyRequestedColumns`          | `ParquetPluginTest.java` | Reads only requested columns from a projected query.                                                   |
| `supportsGreaterThanFilter`             | `ParquetPluginTest.java` | Executes a greater-than filter against the imported customer table.                                    |
| `supportsAllComparisonFilterOperations` | `ParquetPluginTest.java` | Executes `=`, `!=`, `>`, `>=`, `<`, and `<=` filters through JDBC.                                     |
| `rejectsUpdateOnParquetSource`          | `ParquetPluginTest.java` | Rejects `UPDATE` statements against a Parquet source table.                                            |

## Workflow Registry

| Test                                 | File                        | What It Checks                                                         |
|--------------------------------------|-----------------------------|------------------------------------------------------------------------|
| `extractParquetActivityIsRegistered` | `ActivityRegistryTest.java` | Verifies the workflow registry contains the `extractParquet` activity. |
| `loadParquetActivityIsRegistered`    | `ActivityRegistryTest.java` | Verifies the workflow registry contains the `loadParquet` activity.    |

## Workflow Parquet Support

| Test                                                                    | File                              | What It Checks                                                                                                        |
|-------------------------------------------------------------------------|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| `prepareTargetFileFailsOrDeletesExistingFile`                           | `ParquetWorkflowSupportTest.java` | Covers fail, overwrite, and invalid target-file handling before Parquet export.                                       |
| `relationalWriteAndExtractRoundTripHonorsPrimaryKeyFileNameAndMaxCount` | `ParquetWorkflowSupportTest.java` | Writes relational workflow rows, skips the workflow PK, extracts rows with file-name column, and enforces `maxCount`. |
| `documentWriteAndExtractRoundTripHonorsKeepIdNameFieldAndMaxCount`      | `ParquetWorkflowSupportTest.java` | Writes documents without `_id`, extracts generated ids and file-name fields, and enforces `maxCount`.                 |
| `loadActivityPipeWritesRelationalWithKeptPrimaryKeyAndGzip`             | `ParquetWorkflowSupportTest.java` | Executes `ParquetLoadActivity.pipe` for relational input with `keepPk=true` and gzip compression.                    |
| `relationalWriterRoundTripsTemporalAndBinaryValues`                     | `ParquetWorkflowSupportTest.java` | Round-trips DATE, TIME, TIMESTAMP, and VARBINARY values through the workflow Parquet writer and extractor.           |
| `loadActivityPipeRejectsGraphInputAtRuntime`                            | `ParquetWorkflowSupportTest.java` | Verifies runtime graph rejection in `ParquetLoadActivity.pipe`.                                                       |
| `loadActivityPipeFailsWhenSampleConflictsWithDeclaredRelationalSchema`  | `ParquetWorkflowSupportTest.java` | Verifies `CONFLICT_FAIL` rejects sampled values incompatible with the declared relational schema.                    |
| `extractActivityLocksOutputTypeAndPipesMultipleSources`                 | `ParquetWorkflowSupportTest.java` | Locks output type and extracts multiple selected Parquet sources through `ParquetExtractActivity.pipe`.              |
| `documentWriteKeepsOnlyIdWhenRequestedAndRejectsEmptyDroppedIdSchema`   | `ParquetWorkflowSupportTest.java` | Writes documents containing only `_id` when kept and rejects empty schemas when `_id` is dropped.                    |
| `loadActivityPreviewRejectsEmptyRelationalExportAndBuildsDynamicName`   | `ParquetWorkflowSupportTest.java` | Validates `ParquetLoadActivity` relational preview behavior and dynamic names.                                        |
| `extractSupportBuildsDynamicNamesAndDocumentOutputType`                 | `ParquetWorkflowSupportTest.java` | Validates extract output types, dynamic names, and tuple-count estimation.                                            |

## To Do

### Coverage Check Summary

The current suite covers the main relational read path, flat and partitioned discovery, primitive projection, primitive and group filter evaluation, file-level pruning, metadata aggregates, row and column aggregate accumulation, aggregate-decomposition data holders, nested join planning, plugin-level relational integration, selected document conversion/enumeration behavior, document tuple-type and planner-node basics, namespace wrapper creation, source writer/enumerator round trips, statistics provider basics, schema utilities, IO utilities, and workflow helper/activity round trips.

The remaining gaps are mostly full integration paths, planner rewrite matrix cases, catalog lifecycle behavior, and hard-to-simulate IO failure paths. Focused unit coverage has been added for the previously listed workflow dispatch, document conversion, document planning helpers, footer statistics, writer/enumerator handling, partition collision, file pruning, aggregate, and IO utility branches.

### Known Remaining Gaps

| Area                                | Representative Classes                                                                                                                                                       | Still Missing                                                                                                                                                                                                                                                       |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Document source integration         | `ParquetDocumentSource`, `ParquetDocument`                                                                                                                                   | Creating/restoring document collections through the adapter catalog, imported document collections queried through Polypheny, multi-file document collection scans, document read-only behavior, and `scanFiltered` dynamic-parameter resolution. |
| Document planner rewrite            | `ParquetDocScan`, `ParquetDocFilter`, `ParquetDocFilterRule`                                                                                                                 | Planner-level checks that supported document filters become `ParquetDocFilter`/filtered `ParquetDocScan`, unsupported predicates stay outside the adapter path, translated `AND` predicates are pushed together through an optimizer rule call, and document scan/filter cost scaling remains stable. |
| Relational source/catalog lifecycle | `ParquetRelationalSource`, `AbstractParquetSource`, `ParquetNamespace`, `ParquetBindingSerializer`                                                                           | Adapter creation from `upload`/`link`/`url` settings, source URL resolution failures, serialized binding restore into physical tables/collections, table restore/drop behavior, namespace updates, and catalog metadata consistency after restore. |
| Partition integration               | `ParquetFileDiscovery`, `ParquetSchemaNormalizer`, `ParquetTableBinding`, `ParquetRelTable`                                                                                  | Missing partition columns across files, mixed partition values and data columns beyond collision checks, multi-level folder behavior in Polypheny partition metadata, restore of partitioned tables, and partition values projected through nested join paths. |
| Planner and optimization matrix     | `ParquetRules`, `PatternMatcher`, `PatternMatchers`, `ParquetRelAggregate`, `ParquetRelMetadataScan`, `ParquetEnumerableUnion` | Matrix coverage from `join_test_list.md` and `partition_test_list.md`, aggregate decomposition/fallback decisions inside `PatternMatchers`, metadata-scan vs data-scan planning, union planning, unsupported join/filter/aggregate fallback to Enumerable, and plan-display stability. |
| Aggregate stress cases              | `ParquetRowAggregator`, `AggregateColumnAccumulator`, `ParquetGroupedAggregatePageReader`                                                                                    | Decimal/float precision stress, all-null groups, multiple grouping columns combined with row filters, unsupported aggregate rejection beyond current numeric checks, and overflow/large-count behavior. |
| IO failure paths                    | `ParquetSourceReader`, `ParquetPrimitiveRowReader`, `HadoopConfigurationFactory`                                                                                              | Cancellation during active reads, invalid URL/path handling that requires filesystem/Hadoop failures, projection schema errors, mixed-schema multi-file read failures outside discovery, Hadoop configuration failures, and reader close/error propagation. |
