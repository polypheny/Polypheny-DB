# Aggregation Flow

## ParquetMetadataAggregateExecutor
`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/execution/aggregate/ParquetMetadataAggregateExecutor.java`
Performs the following:
1. filter by file level: partition values / column statistics
2. find group key for each file
3. update COUNT / MIN / MAX using file metadata
4. return result as Enumerable

## ParquetDataAggregateExecutor
`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/execution/aggregate/ParquetDataAggregateExecutor.java'`
ParquetAggregateExecutor creates following Enumerators:
1. ParquetFileGroupedAggregateEnumerator
   - aggregation can be done per whole file, because each file belongs to exactly one group.
2. ParquetGroupedAggregateEnumerator 
   - grouped COUNT(*). For queries like:
      ```text
       SELECT category, COUNT(*)
       FROM parquet_table
       GROUP BY category;
       ```
      Conditions:
      - all aggregate calls are COUNT(*)
      - filters are empty or partition filters

    - general grouped aggregate. For queries like:
      ```text
      SELECT category, MIN(price), MAX(price), SUM(price)
      FROM parquet_table
      WHERE category IN (VALUE1, VALUE2, ...)
      GROUP BY category;
      ```
      Conditions:
      - filters are supported 
      - aggregate functions by column
3. ParquetRowAggregateEnumerator - default

## Flow Diagram 

![aggragation_flow.drawio.png](images/aggregation/aggragation_flow.drawio.png)
