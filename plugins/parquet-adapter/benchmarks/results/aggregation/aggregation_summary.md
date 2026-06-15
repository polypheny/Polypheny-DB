# Aggregation Summary

Phase summarized: `measured`.
Warmup rows are excluded. Mean and median values use successful runs only.

## Source Files

| System | CSV |
| --- | --- |
| Polypheny Relational | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_results.csv` |
| Polypheny Document MQL | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_results.csv` |
| DuckDB | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_results.csv` |
| Apache Spark | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_results.csv` |

## Mean Elapsed Time (ms)

| Query | Description | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark | Row counts |
| --- | --- | --- | --- | --- | --- | --- |
| Q01 | Full Yellow Taxi Row Count | 10.2 | 121.8 | 28.6 | 1,408.2 | 1 |
| Q02 | Yellow Taxi Count For One Partition Month | 11.6 | 162.4 | 12.0 | 369.4 | 1 |
| Q03 | Yellow Taxi Trips On One Day | 1,092.4 | 612.2 | 232.6 | 5,150.2 | 1 |
| Q04 | Long And Expensive Yellow Taxi Trips | 1,941.4 | 2,184.8 | 690.6 | 3,190.8 | 1 |
| Q05 | Monthly Yellow Taxi Amount And Distance Summary | 1,711.0 | 2,426.4 | 1,169.6 | 4,950.0 | differs |
| Q06 | Full High-Volume FHV Row Count | 10.8 | 323.0 | 92.4 | 1,705.2 | 1 |
| Q07 | High-Volume FHV Count For One Partition Month | 12.2 | 248.4 | 26.4 | 335.6 | 1 |
| Q08 | Long And Expensive High-Volume FHV Trips | 11,461.8 | 15,905.2 | 4,990.0 | 13,269.0 | 1 |
| Q09 | Monthly High-Volume FHV Fare And Distance Summary | 13,246.6 | 14,864.4 | 9,216.8 | 30,931.6 | differs |
| Q10 | High-Volume FHV Shared-Ride Flag Distribution | 5,832.8 | 5,107.6 | 1,252.6 | 7,823.6 | differs |

## Result Row Counts

| Query | Description | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- | --- |
| Q01 | Full Yellow Taxi Row Count | 1 | 1 | 1 | 1 |
| Q02 | Yellow Taxi Count For One Partition Month | 1 | 1 | 1 | 1 |
| Q03 | Yellow Taxi Trips On One Day | 1 | 1 | 1 | 1 |
| Q04 | Long And Expensive Yellow Taxi Trips | 1 | 1 | 1 | 1 |
| Q05 | Monthly Yellow Taxi Amount And Distance Summary | 37 | 4 | 37 | 37 |
| Q06 | Full High-Volume FHV Row Count | 1 | 1 | 1 | 1 |
| Q07 | High-Volume FHV Count For One Partition Month | 1 | 1 | 1 | 1 |
| Q08 | Long And Expensive High-Volume FHV Trips | 1 | 1 | 1 | 1 |
| Q09 | Monthly High-Volume FHV Fare And Distance Summary | 37 | 4 | 37 | 37 |
| Q10 | High-Volume FHV Shared-Ride Flag Distribution | 4 | 2 | 4 | 4 |

## Detailed Summary (ms)

| System | Query | Description | Runs | Mean | Median | Min | Max | Rows | Columns | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Polypheny Relational | Q01 | Full Yellow Taxi Row Count | 5/5 | 10.2 | 10.0 | 7.0 | 17.0 | 1 | 1 | ok |
| Polypheny Relational | Q02 | Yellow Taxi Count For One Partition Month | 5/5 | 11.6 | 12.0 | 9.0 | 13.0 | 1 | 1 | ok |
| Polypheny Relational | Q03 | Yellow Taxi Trips On One Day | 5/5 | 1,092.4 | 1,116.0 | 970.0 | 1,179.0 | 1 | 1 | ok |
| Polypheny Relational | Q04 | Long And Expensive Yellow Taxi Trips | 5/5 | 1,941.4 | 1,956.0 | 1,861.0 | 2,001.0 | 1 | 1 | ok |
| Polypheny Relational | Q05 | Monthly Yellow Taxi Amount And Distance Summary | 5/5 | 1,711.0 | 1,675.0 | 1,447.0 | 2,022.0 | 37 | 6 | ok |
| Polypheny Relational | Q06 | Full High-Volume FHV Row Count | 5/5 | 10.8 | 11.0 | 8.0 | 13.0 | 1 | 1 | ok |
| Polypheny Relational | Q07 | High-Volume FHV Count For One Partition Month | 5/5 | 12.2 | 13.0 | 10.0 | 13.0 | 1 | 1 | ok |
| Polypheny Relational | Q08 | Long And Expensive High-Volume FHV Trips | 5/5 | 11,461.8 | 11,529.0 | 11,244.0 | 11,552.0 | 1 | 1 | ok |
| Polypheny Relational | Q09 | Monthly High-Volume FHV Fare And Distance Summary | 5/5 | 13,246.6 | 13,213.0 | 12,810.0 | 14,053.0 | 37 | 7 | ok |
| Polypheny Relational | Q10 | High-Volume FHV Shared-Ride Flag Distribution | 5/5 | 5,832.8 | 5,768.0 | 5,266.0 | 6,506.0 | 4 | 3 | ok |
| Polypheny Document MQL | Q01 | Full Yellow Taxi Row Count | 5/5 | 121.8 | 125.0 | 108.0 | 130.0 | 1 | 1 | ok |
| Polypheny Document MQL | Q02 | Yellow Taxi Count For One Partition Month | 5/5 | 162.4 | 158.0 | 141.0 | 192.0 | 1 | 1 | ok |
| Polypheny Document MQL | Q03 | Yellow Taxi Trips On One Day | 5/5 | 612.2 | 611.0 | 599.0 | 626.0 | 1 | 1 | ok |
| Polypheny Document MQL | Q04 | Long And Expensive Yellow Taxi Trips | 5/5 | 2,184.8 | 2,493.0 | 1,162.0 | 3,390.0 | 1 | 1 | ok |
| Polypheny Document MQL | Q05 | Monthly Yellow Taxi Amount And Distance Summary | 5/5 | 2,426.4 | 2,400.0 | 2,331.0 | 2,522.0 | 4 | 4 | ok |
| Polypheny Document MQL | Q06 | Full High-Volume FHV Row Count | 5/5 | 323.0 | 346.0 | 224.0 | 418.0 | 1 | 1 | ok |
| Polypheny Document MQL | Q07 | High-Volume FHV Count For One Partition Month | 5/5 | 248.4 | 257.0 | 212.0 | 283.0 | 1 | 1 | ok |
| Polypheny Document MQL | Q08 | Long And Expensive High-Volume FHV Trips | 5/5 | 15,905.2 | 14,670.0 | 13,069.0 | 21,529.0 | 1 | 1 | ok |
| Polypheny Document MQL | Q09 | Monthly High-Volume FHV Fare And Distance Summary | 5/5 | 14,864.4 | 14,719.0 | 14,655.0 | 15,375.0 | 4 | 5 | ok |
| Polypheny Document MQL | Q10 | High-Volume FHV Shared-Ride Flag Distribution | 5/5 | 5,107.6 | 5,052.0 | 4,747.0 | 5,694.0 | 2 | 1 | ok |
| DuckDB | Q01 | Full Yellow Taxi Row Count | 5/5 | 28.6 | 29.0 | 27.0 | 30.0 | 1 | 1 | ok |
| DuckDB | Q02 | Yellow Taxi Count For One Partition Month | 5/5 | 12.0 | 13.0 | 9.0 | 14.0 | 1 | 1 | ok |
| DuckDB | Q03 | Yellow Taxi Trips On One Day | 5/5 | 232.6 | 232.0 | 187.0 | 285.0 | 1 | 1 | ok |
| DuckDB | Q04 | Long And Expensive Yellow Taxi Trips | 5/5 | 690.6 | 680.0 | 649.0 | 731.0 | 1 | 1 | ok |
| DuckDB | Q05 | Monthly Yellow Taxi Amount And Distance Summary | 5/5 | 1,169.6 | 1,182.0 | 1,078.0 | 1,262.0 | 37 | 6 | ok |
| DuckDB | Q06 | Full High-Volume FHV Row Count | 5/5 | 92.4 | 94.0 | 87.0 | 98.0 | 1 | 1 | ok |
| DuckDB | Q07 | High-Volume FHV Count For One Partition Month | 5/5 | 26.4 | 27.0 | 22.0 | 28.0 | 1 | 1 | ok |
| DuckDB | Q08 | Long And Expensive High-Volume FHV Trips | 5/5 | 4,990.0 | 5,045.0 | 4,747.0 | 5,134.0 | 1 | 1 | ok |
| DuckDB | Q09 | Monthly High-Volume FHV Fare And Distance Summary | 5/5 | 9,216.8 | 9,118.0 | 9,026.0 | 9,451.0 | 37 | 7 | ok |
| DuckDB | Q10 | High-Volume FHV Shared-Ride Flag Distribution | 5/5 | 1,252.6 | 1,279.0 | 1,084.0 | 1,461.0 | 4 | 3 | ok |
| Apache Spark | Q01 | Full Yellow Taxi Row Count | 5/5 | 1,408.2 | 1,460.0 | 1,148.0 | 1,595.0 | 1 | 1 | ok |
| Apache Spark | Q02 | Yellow Taxi Count For One Partition Month | 5/5 | 369.4 | 375.0 | 349.0 | 377.0 | 1 | 1 | ok |
| Apache Spark | Q03 | Yellow Taxi Trips On One Day | 5/5 | 5,150.2 | 4,990.0 | 4,887.0 | 5,861.0 | 1 | 1 | ok |
| Apache Spark | Q04 | Long And Expensive Yellow Taxi Trips | 5/5 | 3,190.8 | 3,217.0 | 3,009.0 | 3,348.0 | 1 | 1 | ok |
| Apache Spark | Q05 | Monthly Yellow Taxi Amount And Distance Summary | 5/5 | 4,950.0 | 4,831.0 | 4,737.0 | 5,338.0 | 37 | 6 | ok |
| Apache Spark | Q06 | Full High-Volume FHV Row Count | 5/5 | 1,705.2 | 1,787.0 | 1,452.0 | 1,807.0 | 1 | 1 | ok |
| Apache Spark | Q07 | High-Volume FHV Count For One Partition Month | 5/5 | 335.6 | 330.0 | 256.0 | 437.0 | 1 | 1 | ok |
| Apache Spark | Q08 | Long And Expensive High-Volume FHV Trips | 5/5 | 13,269.0 | 13,234.0 | 12,108.0 | 14,109.0 | 1 | 1 | ok |
| Apache Spark | Q09 | Monthly High-Volume FHV Fare And Distance Summary | 5/5 | 30,931.6 | 30,810.0 | 30,380.0 | 31,569.0 | 37 | 7 | ok |
| Apache Spark | Q10 | High-Volume FHV Shared-Ride Flag Distribution | 5/5 | 7,823.6 | 7,779.0 | 7,238.0 | 8,694.0 | 4 | 3 | ok |
