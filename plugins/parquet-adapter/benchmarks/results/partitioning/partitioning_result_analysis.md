# Partitioning - Benchmark Analysis

## Results Plot
![partitioning_plot.png](../plots/partitioning_plot.png)

## Interpretation

### General

- The partitioning benchmark completed successfully for all evaluated systems: Polypheny Relational, DuckDB, and Apache Spark.
- All benchmark executions completed successfully (`5/5` successful measured runs for every query).
- Every query produced the expected single aggregate result, confirming consistent execution across all systems.
- The low standard deviations observed across repeated runs indicate stable benchmark measurements.

## Query Groups

The benchmark consists of five logical query groups. Each query is evaluated on both dataset layouts:

- **RP**: repartitioned Hive-style layout, where `year` and `month` are encoded as directory partitions.
- **UP**: unpartitioned layout, where `year` and `month` are stored as regular columns in the Parquet files.

Queries **Q1–Q3** are evaluated on the `yellow_tripdata` dataset, while **Q4–Q5** use the `green_tripdata` dataset.


## Main Findings

- Q1 serves as the baseline query. Since no predicates are applied to partition columns, both the partitioned and unpartitioned layouts require access to all Parquet files. 
Consequently, Hive partitioning provides no opportunity for partition pruning, and both layouts exhibit comparable performance. 
The observed runtime differences are likely attributable to implementation overhead rather than the dataset organization itself.

- Q2 demonstrates the effectiveness of Hive-style partition pruning. 
The query filters on the partition column year, allowing the partitioned layout to skip all directories except those corresponding to the selected year. 
In contrast, the unpartitioned layout must inspect every Parquet file because year is stored as a regular column. 
As a result, all evaluated systems execute the partitioned query substantially faster, with runtime reductions of approximately 31% for Polypheny, 60% for DuckDB, and 70% for Apache Spark.
- Polypheny shows a smaller difference between the partitioned and unpartitioned layouts in Q2 because the COUNT query is already highly optimized using Parquet metadata.

- Q3 further demonstrates the benefit of Hive-style partitioning. 
By filtering on both partition columns (year and month), the partitioned layout accesses only a single monthly partition, 
while the unpartitioned layout must still inspect all Parquet files. As a result, all evaluated systems achieve lower execution times on the partitioned dataset. 
- As in Q2, the performance gap is smaller for Polypheny because the metadata-based COUNT optimization already minimizes the execution cost of the query.
- In the current implementation, Polypheny performs planner-level partition pruning only on the first partition level (year). 
The second partition level (month) is handled within the adapter by filtering the remaining partition directories before reading the Parquet files.

- Q4 query does not filter on the partition columns (year or month), thus Hive-style partition pruning cannot be applied.
Consequently, both the partitioned and unpartitioned layouts must scan all Parquet files. As expected, partitioning does not provide a performance benefit for this query.
These results confirm that partitioning provides a performance benefit only when queries contain predicates on the partition columns.

- Q5 combines a partition predicate with row-level filtering. 
Unlike Q4, the filter on year enables partition pruning, allowing the partitioned layout to skip files belonging to other years. 
Polypheny and Apache Spark therefore execute the query faster on the partitioned dataset, 
demonstrating the practical benefit of Hive-style partitioning for selective analytical workloads. 
Unlike Polypheny and Apache Spark, DuckDB exhibits similar performance for the partitioned and unpartitioned layouts, with the unpartitioned layout being slightly faster in this benchmark. 
his suggests that Hive-style partitioning provides only a limited benefit for this query in DuckDB


## Related Documents

Summary:

```text
plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_summary.md
```

Plot:
```text
plugins/parquet-adapter/benchmarks/results/plots/partitioning_plot.png
plugins/parquet-adapter/benchmarks/results/plots/partitioning_plot.pdf
plugins/parquet-adapter/benchmarks/results/plots/partitioning_plot.svg
```
