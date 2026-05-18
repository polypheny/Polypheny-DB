# File Pruning

- Not matching file sources is filtered according to parquet statistics.
- Statistics loaded when adapter created.
- Statistics stored in Adapter Settings as part of Bindings and reused for Polypheny statistics (`ParquetTableStatisticsReader`)

## Changes

### ParquetColumnStatistics
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetColumnStatistics.java`

- per-file statistics

### ParquetSourceFile - record
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetSourceFile.java`

- Store column statistics per column: `Map<List<String>, ParquetColumnStatistics> columnStatistics`. Column path is a column identifier because field can be nested
- ParquetSourceFile stored in ParquetTableBindings `Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetTableBinding.java`

### ParquetBindingSerializer
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetBindingSerializer.java`

New functions:
- serializeColumnStatistics()
- deserializeColumnStatistics()

### ParquetColumnStatisticsReader
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\shared\statistics\ParquetColumnStatisticsReader.java`
- Reads column statistics from Parquet footer metadata
- returns ParquetColumnStatistics

### ParquetTableStatisticsReader
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\shared\statistics\ParquetTableStatisticsReader.java`
- load Polypheny statistics from bindings
- returns ProvidedColumnStatistics

### ParquetSourceFileStatisticsFilterEvaluator
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\execution\ParquetSourceFileStatisticsFilterEvaluator.java`
- evaluateLeaf() function get statistics by column path and evaluates if file should be skipped
- called from abstract class FilterEvaluator.evaluate() which is called from match()

### ParquetSourceFilePartitionFilterEvaluator
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\execution\ParquetSourceFilePartitionFilterEvaluator.java`
- evaluateLeaf() function evaluate by folder name if file should be skipped
- called from abstract class FilterEvaluator.evaluate() which is called from match()

### ParquetMultiFileEnumerator
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\execution\ParquetMultiFileEnumerator.java`

- creates relevant enumerator using factory

### ParquetRelTable
`Polypheny-DB\plugins\parquet-adapter\src\main\java\org\polypheny\db\adapter\parquet\relational\schema\ParquetRelTable.java'`

- creates ParquetMultiFileEnumerator
- ParquetMultiFileEnumerator receives chain of evaluators
- createParquetSourceFileEvaluatorsChain() - chain contains two evaluators: partition `ParquetSourceFilePartitionFilterEvaluator` and statistics `ParquetSourceFileStatisticsFilterEvaluator`

## Flow

1. Adapter creation / file discovery
   Stores partition values + footer stats in ParquetSourceFile
2. Adapter settings persist those ParquetSourceFiles. 
3. Adapter/table loading
   Restore ParquetSourceFiles containing partition values + footer from settings
4. Query execution calls ParquetRelTable.project(...) / scan(...) / nestedJoin(...),
   which creates ParquetMultiFileEnumerator.
5. ParquetMultiFileEnumerator checks each ParquetSourceFile before opening it. Per-file pruning happens inside: `ParquetMultiFileEnumerator.moveNext()`
6. Partition evaluator compares filters with sourceFile.partitionValues(). 
7. Statistics evaluator compares filters with sourceFile.columnStatistics(). 
8. If evaluators prove the file cannot match, the file is skipped. 
9. If the result is unknown or possibly matching, the file reader is opened. 
10. Row-level filtering still happens after opening the file.
