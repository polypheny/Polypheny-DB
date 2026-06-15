# Parquet Relational And Document Execution Flows

These diagrams describe the current Parquet runtime paths after aggregate
execution was moved into shared infrastructure and document aggregation pushdown
was added. They focus on runtime entry points reached from generated planner
nodes, not on full planner rule matching.

Relational execution starts at `ParquetRelTable`. Document execution starts at
`ParquetDocument`. Both models use the shared aggregate executors through the
`ParquetAggregateSource` abstraction.

## Aggregate Entry Points

```mermaid
flowchart TD
    relPlan["ParquetRelAggregate.implement(...)"]
    docPlan["ParquetDocAggregate.implement(...)"]

    relDataEntry["ParquetRelTable.dataAggregate(...)"]
    relMetadataEntry["ParquetRelTable.metadataAggregate(...)"]
    docDataEntry["ParquetDocument.dataAggregate(...)"]
    docMetadataEntry["ParquetDocument.metadataAggregate(...)"]

    relDataWrapper["ParquetRelDataAggregateExecutor"]
    relMetadataWrapper["ParquetRelMetadataAggregateExecutor"]
    docWrapper["ParquetDocAggregateExecutor"]

    relSource["relational ParquetAggregateSource"]
    docSource["DocumentAggregateSource"]

    dataExecutor["ParquetDataAggregateExecutor"]
    metadataExecutor["ParquetMetadataAggregateExecutor"]

    relPlan -->|"mode = DATA"| relDataEntry
    relPlan -->|"mode = METADATA"| relMetadataEntry
    docPlan -->|"mode = DATA"| docDataEntry
    docPlan -->|"mode = METADATA"| docMetadataEntry

    relDataEntry --> relDataWrapper --> relSource --> dataExecutor
    relMetadataEntry --> relMetadataWrapper --> relSource --> metadataExecutor
    docDataEntry --> docWrapper --> docSource --> dataExecutor
    docMetadataEntry --> docWrapper --> docSource --> metadataExecutor
```

## Shared Data Aggregate Flow

```mermaid
flowchart TD
    relEntry["ParquetRelDataAggregateExecutor.createEnumerator(...)"]
    docEntry["ParquetDocAggregateExecutor.createDataEnumerator(...)"]
    shared["ParquetDataAggregateExecutor.createEnumerator(...)"]

    resolveFilters["ParquetFilterResolver.resolveFilters(...)"]
    strategy{"enumerator strategy"}

    fileProjection["tryBuildAggregateProjection(...)"]
    fileEnumerator["ParquetFileGroupedAggregateEnumerator"]
    fileFilter["partition/statistics file filters"]
    fileGroup["fileGroupKey(...) from ParquetConstantColumnResolver"]
    countStarRows["COUNT(*) from source row count"]
    columnReader["ParquetNoFilterColumnAggregateReader"]
    columnAccumulator["AggregateColumnAccumulator"]
    fileRows["build aggregate result rows"]

    groupedCountProjection["tryBuildGroupedCountProjection(...)"]
    groupedProjection["tryBuildGroupedAggregateProjection(...)"]
    groupedEnumerator["ParquetGroupedAggregateEnumerator"]
    groupedReader["ParquetGroupedAggregateReader"]
    pageStrategy{"page strategy"}
    countPage["ParquetCountAggregatePageReader"]
    groupedPage["ParquetGroupedAggregatePageReader"]

    fallbackRows["relational fallback rows from ParquetEnumeratorsFactory"]
    rowAggregate["ParquetRowAggregateEnumerator"]
    rowEngine["ParquetRowAggregator"]
    directProjector["DirectAggregateRowProjector"]

    unsupported["GenericRuntimeException"]

    relEntry -->|"with row fallback"| shared
    docEntry -->|"without row fallback"| shared
    shared --> resolveFilters --> strategy

    strategy -->|"file-constant groups and exact file filters"| fileProjection --> fileEnumerator
    fileEnumerator --> fileFilter --> fileGroup
    fileGroup --> countStarRows --> fileRows
    fileGroup -->|"aggregate columns projected"| columnReader --> columnAccumulator --> fileRows

    strategy -->|"COUNT(*) grouped by flat data columns"| groupedCountProjection --> groupedEnumerator
    strategy -->|"flat grouped aggregates with supported filters"| groupedProjection --> groupedEnumerator
    groupedEnumerator --> groupedReader --> pageStrategy
    pageStrategy -->|"COUNT(*) only and no group fields"| countPage
    pageStrategy -->|"default grouped path"| groupedPage

    strategy -->|"no aggregate reader strategy and relational fallback exists"| fallbackRows
    fallbackRows --> rowAggregate --> rowEngine --> directProjector
    strategy -->|"no aggregate reader strategy and no fallback"| unsupported
```

## Shared Metadata Aggregate Flow

```mermaid
flowchart TD
    relEntry["ParquetRelMetadataAggregateExecutor.createEnumerator(...)"]
    docEntry["ParquetDocAggregateExecutor.createMetadataEnumerator(...)"]
    executor["ParquetMetadataAggregateExecutor.createEnumerator(...)"]

    resolveFilters["ParquetFilterResolver.resolveFilters(...)"]
    evaluator["ParquetMultiFilterEvaluator"]
    partitionEval["ParquetSourceFilePartitionFilterEvaluator"]
    statsEval["ParquetSourceFileStatisticsFilterEvaluator"]

    loop["iterate ParquetAggregateSource.sourceFiles()"]
    matches{"matchesExactly(...)"}
    skip["skip source file"]
    groupKey["fileGroupKey(...) from ParquetConstantColumnResolver"]
    accumulator["MetadataAggregateAccumulator[] per group"]
    aggregateKind{"aggregate kind"}
    countStar["COUNT(*) uses sourceRowCount(...)"]
    countColumn["COUNT(column) uses rowCount - nullCount"]
    minMax["MIN/MAX uses ParquetColumnStatistics ranges"]
    emptyGroup["if no groups and no matches, create empty aggregate row"]
    rows["build PolyValue[] result rows"]
    enumerable["Linq4j.asEnumerable(rows)"]

    supportGate["supportsMetadataAggregate(...)"]
    supportChecks["file-constant groups, metadata-decidable filters, COUNT/MIN/MAX support"]

    relEntry --> executor
    docEntry --> executor
    executor --> resolveFilters --> evaluator
    evaluator --> partitionEval
    evaluator --> statsEval
    evaluator --> loop --> matches
    matches -->|"false"| skip
    matches -->|"true"| groupKey --> accumulator --> aggregateKind
    aggregateKind -->|"COUNT(*)"| countStar --> rows
    aggregateKind -->|"COUNT(column)"| countColumn --> rows
    aggregateKind -->|"MIN/MAX"| minMax --> rows
    loop --> emptyGroup --> rows --> enumerable

    supportGate -.-> supportChecks -.-> executor
```

## Relational Projection And Row Scan Flow

```mermaid
flowchart TD
    entry["ParquetRelTable.project(...)"]
    factory["ParquetRelExecutorsFactory.getExecutor(ParquetRelProjectExecutor.class)"]
    executor["ParquetRelProjectExecutor.createEnumerator(...)"]
    resolveFilters["ParquetFilterResolver.resolveFilters(...)"]
    enumeratorsFactory["ParquetEnumeratorsFactory.create(...)"]
    multiFile["ParquetMultiFileEnumerator"]
    fileEvaluator["partition/statistics source-file evaluator"]
    fileMatches{"source file matches?"}
    skip["skip source file"]
    enumForFile["ParquetRelExecutor.enumeratorForFile(...)"]
    shape{"table and projection shape"}

    primitiveReader["ParquetPrimitiveRowReader"]
    readerFilters["readerFilters(...)"]
    outputProjection["primitiveRowProjection(...)"]
    primitiveEnumerator["ParquetRowRelEnumerator"]

    sourceReaderNested["ParquetSourceReader"]
    nestedRepeated["ParquetNestedRepeatedRelEnumerator"]

    sourceReaderBinding["ParquetSourceReader"]
    nestedNonRepeated["ParquetNestedNonRepeatedRelEnumerator"]

    sourceReaderGeneric["ParquetSourceReader"]
    relationalEnumerator["ParquetRelEnumerator"]

    entry --> factory --> executor --> resolveFilters --> enumeratorsFactory --> multiFile
    multiFile --> fileEvaluator --> fileMatches
    fileMatches -->|"false"| skip
    fileMatches -->|"true"| enumForFile --> shape

    shape -->|"flat primitive projection"| primitiveReader --> readerFilters --> outputProjection --> primitiveEnumerator
    shape -->|"nested table"| sourceReaderNested --> nestedRepeated
    shape -->|"binding scan needed"| sourceReaderBinding --> nestedNonRepeated
    shape -->|"generic flat row path"| sourceReaderGeneric --> relationalEnumerator
```

## Document Scan Flow

```mermaid
flowchart TD
    plan["ParquetDocScan.implement(...)"]
    scan["ParquetDocument.scanFiltered(...)"]
    register["register involved Parquet adapter"]
    resolveDynamic["resolve dynamic filter parameters"]
    filters["FiltersContainer.shared(...)"]
    multiFile["ParquetMultiFileEnumerator"]
    sourceReader["ParquetSourceReader"]
    docEnumerator["ParquetDocEnumerator"]
    extractor["ParquetDocValueExtractor"]
    row["PolyValue[] containing one PolyDocument"]

    plan --> scan --> register --> resolveDynamic --> filters --> multiFile
    multiFile -->|"for each source file"| sourceReader
    sourceReader --> docEnumerator --> extractor --> row
```

## Document Aggregate Planning To Runtime

```mermaid
flowchart TD
    scan["ParquetDocScan"]
    filterRule["attachDocumentFiltersToScanUnderCalc"]
    aggregateRules{"document aggregate rule"}
    aggregateOnScan["documentAggregateOnScan"]
    aggregateOnProject["documentAggregateOnProjectScan"]
    aggregateOnCalc["documentAggregateOnCalcScan"]
    fieldMapping["map top-level document fields to exported Parquet columns"]
    aggregateNode["ParquetDocAggregate"]
    mode{"aggregate mode"}
    metadataScan["ParquetDocMetadataScan"]
    metadataRuntime["ParquetDocument.metadataAggregate(...)"]
    dataRuntime["ParquetDocument.dataAggregate(...)"]
    sharedMetadata["ParquetMetadataAggregateExecutor"]
    sharedData["ParquetDataAggregateExecutor"]

    scan --> filterRule --> scan
    scan --> aggregateRules
    aggregateRules --> aggregateOnScan
    aggregateRules --> aggregateOnProject
    aggregateRules --> aggregateOnCalc
    aggregateOnScan --> fieldMapping
    aggregateOnProject --> fieldMapping
    aggregateOnCalc --> fieldMapping
    fieldMapping --> aggregateNode --> mode
    mode -->|"metadata supported"| metadataScan --> metadataRuntime --> sharedMetadata
    mode -->|"data aggregate supported"| dataRuntime --> sharedData
```

## Notes

- Aggregate planner rules are registered only when
  `ParquetOptimizationSettings.isOptimizeAggregation()` is enabled.
- Metadata aggregation is exact-only. It requires file-constant group fields,
  metadata-decidable filters, and supported `COUNT`, `MIN`, or `MAX` aggregate
  calls.
- Data aggregation supports the shared fast paths for file-level aggregates,
  grouped counts, and grouped flat-column aggregates. The relational path also
  provides a row fallback; the document path relies on the shared aggregate
  readers.
- Document aggregate pushdown only maps projected top-level document fields that
  correspond to exported Parquet columns. More complex document expressions stay
  outside the adapter aggregate path.
