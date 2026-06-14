# Aggregation Summary

Phase summarized: `measured`.
Warmup rows are excluded. Mean and median values use successful runs only.

## Source Files

| System               | CSV                                                                                        |
|----------------------|--------------------------------------------------------------------------------------------|
| Polypheny Relational | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_results.csv` |
| DuckDB               | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_results.csv`    |
| Apache Spark         | `plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_results.csv`     |

## Mean Elapsed Time (ms)

| Query | Description                                       | Polypheny Relational | DuckDB  | Apache Spark | Row counts |
|-------|---------------------------------------------------|----------------------|---------|--------------|------------|
| Q01   | Full Yellow Taxi Row Count                        | 11.2                 | 33.8    | 3,541.6      | 1          |
| Q02   | Yellow Taxi Count For One Partition Month         | 9.6                  | 13.8    | 1,228.0      | 1          |
| Q03   | Yellow Taxi Trips On One Day                      | 1,097.4              | 341.4   | 12,329.8     | 1          |
| Q04   | Long And Expensive Yellow Taxi Trips              | 2,965.6              | 793.2   | 9,580.2      | 1          |
| Q05   | Monthly Yellow Taxi Amount And Distance Summary   | 2,452.2              | 1,072.4 | 12,750.6     | 37         |
| Q06   | Full High-Volume FHV Row Count                    | 16.0                 | 96.8    | 3,385.6      | 1          |
| Q07   | High-Volume FHV Count For One Partition Month     | 19.0                 | 26.4    | 285.4        | 1          |
| Q08   | Long And Expensive High-Volume FHV Trips          | 19,552.8             | 6,327.8 | 12,076.8     | 1          |
| Q09   | Monthly High-Volume FHV Fare And Distance Summary | 24,881.4             | 9,690.6 | 27,923.4     | 37         |
| Q10   | High-Volume FHV Shared-Ride Flag Distribution     | 8,162.6              | 1,559.4 | 7,446.2      | 4          |

## Result Row Counts

| Query | Description                                       | Polypheny Relational | DuckDB | Apache Spark |
|-------|---------------------------------------------------|----------------------|--------|--------------|
| Q01   | Full Yellow Taxi Row Count                        | 1                    | 1      | 1            |
| Q02   | Yellow Taxi Count For One Partition Month         | 1                    | 1      | 1            |
| Q03   | Yellow Taxi Trips On One Day                      | 1                    | 1      | 1            |
| Q04   | Long And Expensive Yellow Taxi Trips              | 1                    | 1      | 1            |
| Q05   | Monthly Yellow Taxi Amount And Distance Summary   | 37                   | 37     | 37           |
| Q06   | Full High-Volume FHV Row Count                    | 1                    | 1      | 1            |
| Q07   | High-Volume FHV Count For One Partition Month     | 1                    | 1      | 1            |
| Q08   | Long And Expensive High-Volume FHV Trips          | 1                    | 1      | 1            |
| Q09   | Monthly High-Volume FHV Fare And Distance Summary | 37                   | 37     | 37           |
| Q10   | High-Volume FHV Shared-Ride Flag Distribution     | 4                    | 4      | 4            |

## Detailed Summary (ms)

| System               | Query | Description                                       | Runs | Mean     | Median   | Min      | Max      | Rows | Columns | Status |
|----------------------|-------|---------------------------------------------------|------|----------|----------|----------|----------|------|---------|--------|
| Polypheny Relational | Q01   | Full Yellow Taxi Row Count                        | 5/5  | 11.2     | 10.0     | 9.0      | 18.0     | 1    | 1       | ok     |
| Polypheny Relational | Q02   | Yellow Taxi Count For One Partition Month         | 5/5  | 9.6      | 9.0      | 8.0      | 11.0     | 1    | 1       | ok     |
| Polypheny Relational | Q03   | Yellow Taxi Trips On One Day                      | 5/5  | 1,097.4  | 1,004.0  | 962.0    | 1,295.0  | 1    | 1       | ok     |
| Polypheny Relational | Q04   | Long And Expensive Yellow Taxi Trips              | 5/5  | 2,965.6  | 2,820.0  | 2,297.0  | 3,748.0  | 1    | 1       | ok     |
| Polypheny Relational | Q05   | Monthly Yellow Taxi Amount And Distance Summary   | 5/5  | 2,452.2  | 2,497.0  | 2,365.0  | 2,534.0  | 37   | 6       | ok     |
| Polypheny Relational | Q06   | Full High-Volume FHV Row Count                    | 5/5  | 16.0     | 16.0     | 15.0     | 17.0     | 1    | 1       | ok     |
| Polypheny Relational | Q07   | High-Volume FHV Count For One Partition Month     | 5/5  | 19.0     | 18.0     | 16.0     | 22.0     | 1    | 1       | ok     |
| Polypheny Relational | Q08   | Long And Expensive High-Volume FHV Trips          | 5/5  | 19,552.8 | 20,611.0 | 15,594.0 | 21,747.0 | 1    | 1       | ok     |
| Polypheny Relational | Q09   | Monthly High-Volume FHV Fare And Distance Summary | 5/5  | 24,881.4 | 22,647.0 | 20,897.0 | 30,554.0 | 37   | 7       | ok     |
| Polypheny Relational | Q10   | High-Volume FHV Shared-Ride Flag Distribution     | 5/5  | 8,162.6  | 8,380.0  | 7,451.0  | 8,616.0  | 4    | 3       | ok     |
| DuckDB               | Q01   | Full Yellow Taxi Row Count                        | 5/5  | 33.8     | 34.0     | 29.0     | 37.0     | 1    | 1       | ok     |
| DuckDB               | Q02   | Yellow Taxi Count For One Partition Month         | 5/5  | 13.8     | 13.0     | 13.0     | 16.0     | 1    | 1       | ok     |
| DuckDB               | Q03   | Yellow Taxi Trips On One Day                      | 5/5  | 341.4    | 344.0    | 329.0    | 356.0    | 1    | 1       | ok     |
| DuckDB               | Q04   | Long And Expensive Yellow Taxi Trips              | 5/5  | 793.2    | 815.0    | 709.0    | 843.0    | 1    | 1       | ok     |
| DuckDB               | Q05   | Monthly Yellow Taxi Amount And Distance Summary   | 5/5  | 1,072.4  | 1,097.0  | 1,007.0  | 1,132.0  | 37   | 6       | ok     |
| DuckDB               | Q06   | Full High-Volume FHV Row Count                    | 5/5  | 96.8     | 97.0     | 90.0     | 101.0    | 1    | 1       | ok     |
| DuckDB               | Q07   | High-Volume FHV Count For One Partition Month     | 5/5  | 26.4     | 25.0     | 23.0     | 30.0     | 1    | 1       | ok     |
| DuckDB               | Q08   | Long And Expensive High-Volume FHV Trips          | 5/5  | 6,327.8  | 5,598.0  | 5,089.0  | 8,269.0  | 1    | 1       | ok     |
| DuckDB               | Q09   | Monthly High-Volume FHV Fare And Distance Summary | 5/5  | 9,690.6  | 9,562.0  | 8,037.0  | 12,223.0 | 37   | 7       | ok     |
| DuckDB               | Q10   | High-Volume FHV Shared-Ride Flag Distribution     | 5/5  | 1,559.4  | 1,602.0  | 1,391.0  | 1,625.0  | 4    | 3       | ok     |
| Apache Spark         | Q01   | Full Yellow Taxi Row Count                        | 5/5  | 3,541.6  | 3,820.0  | 1,167.0  | 5,948.0  | 1    | 1       | ok     |
| Apache Spark         | Q02   | Yellow Taxi Count For One Partition Month         | 5/5  | 1,228.0  | 1,202.0  | 521.0    | 1,845.0  | 1    | 1       | ok     |
| Apache Spark         | Q03   | Yellow Taxi Trips On One Day                      | 5/5  | 12,329.8 | 14,493.0 | 6,741.0  | 14,919.0 | 1    | 1       | ok     |
| Apache Spark         | Q04   | Long And Expensive Yellow Taxi Trips              | 5/5  | 9,580.2  | 5,865.0  | 4,584.0  | 19,634.0 | 1    | 1       | ok     |
| Apache Spark         | Q05   | Monthly Yellow Taxi Amount And Distance Summary   | 5/5  | 12,750.6 | 13,174.0 | 7,059.0  | 19,037.0 | 37   | 6       | ok     |
| Apache Spark         | Q06   | Full High-Volume FHV Row Count                    | 5/5  | 3,385.6  | 2,000.0  | 1,865.0  | 9,053.0  | 1    | 1       | ok     |
| Apache Spark         | Q07   | High-Volume FHV Count For One Partition Month     | 5/5  | 285.4    | 284.0    | 259.0    | 325.0    | 1    | 1       | ok     |
| Apache Spark         | Q08   | Long And Expensive High-Volume FHV Trips          | 5/5  | 12,076.8 | 12,172.0 | 11,390.0 | 12,908.0 | 1    | 1       | ok     |
| Apache Spark         | Q09   | Monthly High-Volume FHV Fare And Distance Summary | 5/5  | 27,923.4 | 27,946.0 | 27,588.0 | 28,279.0 | 37   | 7       | ok     |
| Apache Spark         | Q10   | High-Volume FHV Shared-Ride Flag Distribution     | 5/5  | 7,446.2  | 7,257.0  | 7,145.0  | 8,063.0  | 4    | 3       | ok     |

