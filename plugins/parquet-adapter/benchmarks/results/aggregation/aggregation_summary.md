# Aggregation Summary

Phase summarized: `measured`.
Warmup rows are excluded. Mean, median, and standard deviation values use successful runs only.

## Source Files

| System                 | CSV                                                                                            |
|------------------------|------------------------------------------------------------------------------------------------|
| Polypheny Relational   | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_results.csv`     |
| Polypheny Document MQL | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_results.csv` |
| DuckDB                 | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_results.csv`        |
| Apache Spark           | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_results.csv`         |

## Mean Elapsed Time (ms)

| Query | Description                                      | Polypheny Relational | Polypheny Document MQL | DuckDB  | Apache Spark | Row counts |
|-------|--------------------------------------------------|----------------------|------------------------|---------|--------------|------------|
| Q01   | Full Yellow Taxi Row Count                       | 12.2                 | 179.6                  | 22.6    | 1,415.2      | 1          |
| Q02   | Yellow Taxi Count For One Partition Month        | 10.4                 | 209.0                  | 9.2     | 379.2        | 1          |
| Q03   | Yellow Taxi Trips On One Day                     | 1,304.8              | 954.0                  | 179.0   | 5,486.4      | 1          |
| Q04   | Long And Expensive Yellow Taxi Trips             | 2,468.8              | 3,223.2                | 765.4   | 3,825.8      | 1          |
| Q05   | Yearly Yellow Taxi Amount And Distance Summary   | 1,841.2              | 2,007.6                | 1,193.0 | 5,447.2      | 4          |
| Q06   | Full High-Volume FHV Row Count                   | 12.4                 | 177.6                  | 145.4   | 2,027.4      | 1          |
| Q07   | High-Volume FHV Count For One Partition Month    | 14.2                 | 205.4                  | 34.0    | 275.6        | 1          |
| Q08   | Long And Expensive High-Volume FHV Trips         | 16,174.2             | 13,654.2               | 4,584.6 | 14,250.8     | 1          |
| Q09   | Yearly High-Volume FHV Fare And Distance Summary | 15,590.2             | 13,427.4               | 8,844.2 | 34,324.6     | 4          |
| Q10   | High-Volume FHV Shared-Request Flag Distribution | 5,458.2              | 4,321.0                | 435.4   | 5,407.8      | 2          |

## Result Row Counts

| Query | Description                                      | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
|-------|--------------------------------------------------|----------------------|------------------------|--------|--------------|
| Q01   | Full Yellow Taxi Row Count                       | 1                    | 1                      | 1      | 1            |
| Q02   | Yellow Taxi Count For One Partition Month        | 1                    | 1                      | 1      | 1            |
| Q03   | Yellow Taxi Trips On One Day                     | 1                    | 1                      | 1      | 1            |
| Q04   | Long And Expensive Yellow Taxi Trips             | 1                    | 1                      | 1      | 1            |
| Q05   | Yearly Yellow Taxi Amount And Distance Summary   | 4                    | 4                      | 4      | 4            |
| Q06   | Full High-Volume FHV Row Count                   | 1                    | 1                      | 1      | 1            |
| Q07   | High-Volume FHV Count For One Partition Month    | 1                    | 1                      | 1      | 1            |
| Q08   | Long And Expensive High-Volume FHV Trips         | 1                    | 1                      | 1      | 1            |
| Q09   | Yearly High-Volume FHV Fare And Distance Summary | 4                    | 4                      | 4      | 4            |
| Q10   | High-Volume FHV Shared-Request Flag Distribution | 2                    | 2                      | 2      | 2            |

## Detailed Summary (ms)

| System                 | Query | Description                                      | Runs | Mean     | Median   | Std Dev | Min      | Max      | Rows | Columns | Status |
|------------------------|-------|--------------------------------------------------|------|----------|----------|---------|----------|----------|------|---------|--------|
| Polypheny Relational   | Q01   | Full Yellow Taxi Row Count                       | 5/5  | 12.2     | 12.0     | 3.2     | 8.0      | 17.0     | 1    | 1       | ok     |
| Polypheny Relational   | Q02   | Yellow Taxi Count For One Partition Month        | 5/5  | 10.4     | 11.0     | 1.5     | 8.0      | 12.0     | 1    | 1       | ok     |
| Polypheny Relational   | Q03   | Yellow Taxi Trips On One Day                     | 5/5  | 1,304.8  | 1,234.0  | 280.8   | 1,026.0  | 1,614.0  | 1    | 1       | ok     |
| Polypheny Relational   | Q04   | Long And Expensive Yellow Taxi Trips             | 5/5  | 2,468.8  | 2,446.0  | 293.9   | 2,164.0  | 2,812.0  | 1    | 1       | ok     |
| Polypheny Relational   | Q05   | Yearly Yellow Taxi Amount And Distance Summary   | 5/5  | 1,841.2  | 1,895.0  | 186.5   | 1,603.0  | 2,065.0  | 4    | 5       | ok     |
| Polypheny Relational   | Q06   | Full High-Volume FHV Row Count                   | 5/5  | 12.4     | 12.0     | 2.1     | 10.0     | 15.0     | 1    | 1       | ok     |
| Polypheny Relational   | Q07   | High-Volume FHV Count For One Partition Month    | 5/5  | 14.2     | 15.0     | 1.3     | 12.0     | 15.0     | 1    | 1       | ok     |
| Polypheny Relational   | Q08   | Long And Expensive High-Volume FHV Trips         | 5/5  | 16,174.2 | 16,362.0 | 475.3   | 15,539.0 | 16,633.0 | 1    | 1       | ok     |
| Polypheny Relational   | Q09   | Yearly High-Volume FHV Fare And Distance Summary | 5/5  | 15,590.2 | 15,278.0 | 841.6   | 14,999.0 | 17,074.0 | 4    | 6       | ok     |
| Polypheny Relational   | Q10   | High-Volume FHV Shared-Request Flag Distribution | 5/5  | 5,458.2  | 5,310.0  | 655.8   | 4,858.0  | 6,541.0  | 2    | 2       | ok     |
| Polypheny Document MQL | Q01   | Full Yellow Taxi Row Count                       | 5/5  | 179.6    | 174.0    | 13.7    | 168.0    | 202.0    | 1    | 1       | ok     |
| Polypheny Document MQL | Q02   | Yellow Taxi Count For One Partition Month        | 5/5  | 209.0    | 211.0    | 12.5    | 188.0    | 219.0    | 1    | 1       | ok     |
| Polypheny Document MQL | Q03   | Yellow Taxi Trips On One Day                     | 5/5  | 954.0    | 951.0    | 98.7    | 834.0    | 1,083.0  | 1    | 1       | ok     |
| Polypheny Document MQL | Q04   | Long And Expensive Yellow Taxi Trips             | 5/5  | 3,223.2  | 3,165.0  | 272.3   | 2,945.0  | 3,588.0  | 1    | 1       | ok     |
| Polypheny Document MQL | Q05   | Yearly Yellow Taxi Amount And Distance Summary   | 5/5  | 2,007.6  | 1,900.0  | 246.5   | 1,758.0  | 2,382.0  | 4    | 4       | ok     |
| Polypheny Document MQL | Q06   | Full High-Volume FHV Row Count                   | 5/5  | 177.6    | 173.0    | 18.0    | 162.0    | 204.0    | 1    | 1       | ok     |
| Polypheny Document MQL | Q07   | High-Volume FHV Count For One Partition Month    | 5/5  | 205.4    | 199.0    | 20.2    | 191.0    | 241.0    | 1    | 1       | ok     |
| Polypheny Document MQL | Q08   | Long And Expensive High-Volume FHV Trips         | 5/5  | 13,654.2 | 13,636.0 | 185.3   | 13,426.0 | 13,940.0 | 1    | 1       | ok     |
| Polypheny Document MQL | Q09   | Yearly High-Volume FHV Fare And Distance Summary | 5/5  | 13,427.4 | 13,367.0 | 230.6   | 13,161.0 | 13,783.0 | 4    | 5       | ok     |
| Polypheny Document MQL | Q10   | High-Volume FHV Shared-Request Flag Distribution | 5/5  | 4,321.0  | 4,405.0  | 223.5   | 3,932.0  | 4,499.0  | 2    | 1       | ok     |
| DuckDB                 | Q01   | Full Yellow Taxi Row Count                       | 5/5  | 22.6     | 23.0     | 1.7     | 21.0     | 25.0     | 1    | 1       | ok     |
| DuckDB                 | Q02   | Yellow Taxi Count For One Partition Month        | 5/5  | 9.2      | 9.0      | 0.8     | 8.0      | 10.0     | 1    | 1       | ok     |
| DuckDB                 | Q03   | Yellow Taxi Trips On One Day                     | 5/5  | 179.0    | 180.0    | 15.2    | 162.0    | 202.0    | 1    | 1       | ok     |
| DuckDB                 | Q04   | Long And Expensive Yellow Taxi Trips             | 5/5  | 765.4    | 793.0    | 73.3    | 655.0    | 837.0    | 1    | 1       | ok     |
| DuckDB                 | Q05   | Yearly Yellow Taxi Amount And Distance Summary   | 5/5  | 1,193.0  | 1,170.0  | 92.6    | 1,081.0  | 1,324.0  | 4    | 5       | ok     |
| DuckDB                 | Q06   | Full High-Volume FHV Row Count                   | 5/5  | 145.4    | 146.0    | 3.8     | 141.0    | 150.0    | 1    | 1       | ok     |
| DuckDB                 | Q07   | High-Volume FHV Count For One Partition Month    | 5/5  | 34.0     | 36.0     | 4.2     | 29.0     | 38.0     | 1    | 1       | ok     |
| DuckDB                 | Q08   | Long And Expensive High-Volume FHV Trips         | 5/5  | 4,584.6  | 4,512.0  | 305.2   | 4,216.0  | 5,036.0  | 1    | 1       | ok     |
| DuckDB                 | Q09   | Yearly High-Volume FHV Fare And Distance Summary | 5/5  | 8,844.2  | 9,076.0  | 369.4   | 8,342.0  | 9,145.0  | 4    | 6       | ok     |
| DuckDB                 | Q10   | High-Volume FHV Shared-Request Flag Distribution | 5/5  | 435.4    | 456.0    | 42.2    | 384.0    | 480.0    | 2    | 2       | ok     |
| Apache Spark           | Q01   | Full Yellow Taxi Row Count                       | 5/5  | 1,415.2  | 1,451.0  | 185.0   | 1,162.0  | 1,616.0  | 1    | 1       | ok     |
| Apache Spark           | Q02   | Yellow Taxi Count For One Partition Month        | 5/5  | 379.2    | 384.0    | 28.9    | 340.0    | 411.0    | 1    | 1       | ok     |
| Apache Spark           | Q03   | Yellow Taxi Trips On One Day                     | 5/5  | 5,486.4  | 5,474.0  | 536.1   | 4,752.0  | 6,196.0  | 1    | 1       | ok     |
| Apache Spark           | Q04   | Long And Expensive Yellow Taxi Trips             | 5/5  | 3,825.8  | 3,908.0  | 223.4   | 3,454.0  | 4,034.0  | 1    | 1       | ok     |
| Apache Spark           | Q05   | Yearly Yellow Taxi Amount And Distance Summary   | 5/5  | 5,447.2  | 5,561.0  | 306.1   | 5,050.0  | 5,721.0  | 4    | 5       | ok     |
| Apache Spark           | Q06   | Full High-Volume FHV Row Count                   | 5/5  | 2,027.4  | 2,055.0  | 175.4   | 1,822.0  | 2,249.0  | 1    | 1       | ok     |
| Apache Spark           | Q07   | High-Volume FHV Count For One Partition Month    | 5/5  | 275.6    | 275.0    | 8.3     | 266.0    | 284.0    | 1    | 1       | ok     |
| Apache Spark           | Q08   | Long And Expensive High-Volume FHV Trips         | 5/5  | 14,250.8 | 14,289.0 | 361.6   | 13,841.0 | 14,638.0 | 1    | 1       | ok     |
| Apache Spark           | Q09   | Yearly High-Volume FHV Fare And Distance Summary | 5/5  | 34,324.6 | 34,651.0 | 735.7   | 33,487.0 | 35,189.0 | 4    | 6       | ok     |
| Apache Spark           | Q10   | High-Volume FHV Shared-Request Flag Distribution | 5/5  | 5,407.8  | 5,510.0  | 309.7   | 4,945.0  | 5,713.0  | 2    | 2       | ok     |
