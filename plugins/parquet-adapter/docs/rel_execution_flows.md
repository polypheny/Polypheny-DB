# Relational And Document Execution Flows

This document summarizes the current runtime entry points for relational scans,
document scans, structural joins, and aggregate execution.

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
    docSource["document ParquetAggregateSource"]

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

## Data Aggregate Flow

```mermaid
flowchart TD
    relEntry["ParquetRelDataAggregateExecutor.createEnumerator(...)"]
    docEntry["ParquetDocAggregateExecutor.createDataEnumerator(...)"]
    shared["ParquetDataAggregateExecutor.createEnumerator(...)"]

    resolveFilters["ParquetFilterResolver.resolveFilters(...)"]
    strategy{"strategy"}
    fileGrouped["ParquetFileGroupedAggregateEnumerator"]
    grouped["ParquetGroupedAggregateEnumerator"]
    fallbackRows["relational fallback rows"]
    rowAggregate["ParquetRowAggregateEnumerator"]
    unsupported["unsupported aggregate shape"]

    relEntry --> shared
    docEntry --> shared
    shared --> resolveFilters --> strategy
    strategy -->|"file-constant groups and filters"| fileGrouped
    strategy -->|"grouped primitive readers"| grouped
    strategy -->|"relational fallback available"| fallbackRows --> rowAggregate
    strategy -->|"no supported strategy"| unsupported
```

## Metadata Aggregate Flow

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
    match{"file matches exactly?"}
    skip["skip file"]
    groupKey["file group key"]
    accumulate["metadata accumulators"]
    rows["result rows"]

    relEntry --> executor
    docEntry --> executor
    executor --> resolveFilters --> evaluator
    evaluator --> partitionEval
    evaluator --> statsEval
    evaluator --> loop --> match
    match -->|"false"| skip
    match -->|"true"| groupKey --> accumulate --> rows
```

## Relational Projection Flow

```mermaid
flowchart TD
    entry["ParquetRelTable.project(...)"]
    factory["ParquetRelExecutorsFactory"]
    executor["ParquetRelProjectExecutor.createEnumerator(...)"]
    resolveFilters["ParquetFilterResolver.resolveFilters(...)"]
    enumeratorsFactory["ParquetEnumeratorsFactory.create(...)"]
    multiFile["ParquetMultiFileEnumerator"]
    fileEval["partition/statistics file evaluators"]
    shape{"table and projection shape"}
    primitive["ParquetPrimitiveRowReader + ParquetRowRelEnumerator"]
    nestedRepeated["ParquetNestedRepeatedRelEnumerator"]
    nestedNonRepeated["ParquetNestedNonRepeatedRelEnumerator"]
    generic["ParquetRelEnumerator"]

    entry --> factory --> executor --> resolveFilters --> enumeratorsFactory --> multiFile
    multiFile --> fileEval --> shape
    shape -->|"flat primitive projection"| primitive
    shape -->|"nested repeated table"| nestedRepeated
    shape -->|"binding/path scan"| nestedNonRepeated
    shape -->|"generic flat row path"| generic
```

## Structural Join Flow

```mermaid
flowchart TD
    plan["ParquetRelJoin.implement(...)"]
    table["ParquetRelTable.nestedJoin(...)"]
    executor["ParquetRelNestedJoinExecutor.createEnumerator(...)"]
    split["JoinFiltersSplitter"]
    files["parent source-file pruning"]
    reader["ParquetSourceReader"]
    joinEnum["ParquetNestedJoinEnumerator"]
    rows["joined PolyValue[] rows"]

    plan --> table --> executor --> split --> files --> reader --> joinEnum --> rows
```

## Document Scan Flow

```mermaid
flowchart TD
    plan["ParquetDocScan.implement(...)"]
    scan["ParquetDocument.scanFiltered(...)"]
    resolveDynamic["resolve dynamic filter parameters"]
    multiFile["ParquetMultiFileEnumerator"]
    reader["ParquetSourceReader"]
    docEnum["ParquetDocEnumerator"]
    extractor["ParquetDocValueExtractor"]
    row["PolyValue[] containing one PolyDocument"]

    plan --> scan --> resolveDynamic --> multiFile
    multiFile -->|"for each source file"| reader
    reader --> docEnum --> extractor --> row
```

## Notes

- `ParquetRelTable` is the relational runtime facade for scans, joins, and
  aggregates.
- `ParquetDocument` is the document runtime facade for scans and aggregates.
- `ParquetSourceReader` is the low-level reader used after source-file pruning.
- `ParquetMultiFileEnumerator` handles file iteration and residual filter
  reduction for multi-file tables.
- Metadata aggregate execution is exact-only and uses partition values plus
  footer statistics.
- Data aggregate execution can use direct readers or relational row fallback,
  depending on the shape of the aggregate.
