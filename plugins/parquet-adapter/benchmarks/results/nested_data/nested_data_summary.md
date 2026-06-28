# Nested Data Summary

Phase summarized: `measured`.
Warmup rows are excluded. Mean, median, and standard deviation values use successful runs only.

## Source Files

| System | CSV |
| --- | --- |
| Polypheny Relational Normalized | `plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_polypheny_normalized_results.csv` |
| Polypheny Document MQL | `plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_polypheny_mql_results.csv` |
| DuckDB | `plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_duckdb_results.csv` |
| Apache Spark | `plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_spark_results.csv` |

## Mean Elapsed Time (ms)

| Query | Description | Polypheny Relational Normalized | Polypheny Document MQL | DuckDB | Apache Spark | Row counts |
| --- | --- | --- | --- | --- | --- | --- |
| Q01 | Filtered full customer rows. | 2,463.6 | 233,059.4 | 504,359.8 | 77,967.0 | 100,000 |
| Q02 | Filtered explicit customer projection. | 1,176.8 | 122,618.4 | 640.4 | 1,752.0 | 100,000 |
| Q03 | One-join filtered order element projection. | 2,727.8 |  | 96,091.4 | 13,420.6 | differs |
| Q04 | One-join filtered lineitem element projection. | 10,845.6 |  | 1,575,230.8 | 16,749.2 | differs |
| Q05 | Nested lineitem MAX aggregation grouped by return flag. | 222,409.4 |  | 3,724,019.2 | 15,316.4 | differs |

## Result Row Counts

| Query | Description | Polypheny Relational Normalized | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- | --- |
| Q01 | Filtered full customer rows. | 100,000 | 100,000 | 100,000 | 100,000 |
| Q02 | Filtered explicit customer projection. | 100,000 | 100,000 | 100,000 | 100,000 |
| Q03 | One-join filtered order element projection. | 100,000 |  | 100,000 | 100,000 |
| Q04 | One-join filtered lineitem element projection. | 100,000 |  | 100,000 | 100,000 |
| Q05 | Nested lineitem MAX aggregation grouped by return flag. | 3 |  | 3 | 3 |

## Detailed Summary (ms)

| System | Query | Description | Runs | Mean | Median | Std Dev | Min | Max | Rows | Columns | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Polypheny Relational Normalized | Q01 | Filtered full customer rows. | 5/5 | 2,463.6 | 2,515.0 | 744.8 | 1,560.0 | 3,595.0 | 100,000 | 9 | ok |
| Polypheny Relational Normalized | Q02 | Filtered explicit customer projection. | 5/5 | 1,176.8 | 1,169.0 | 176.9 | 953.0 | 1,446.0 | 100,000 | 8 | ok |
| Polypheny Relational Normalized | Q03 | One-join filtered order element projection. | 5/5 | 2,727.8 | 2,599.0 | 300.1 | 2,400.0 | 3,132.0 | 100,000 | 8 | ok |
| Polypheny Relational Normalized | Q04 | One-join filtered lineitem element projection. | 5/5 | 10,845.6 | 10,887.0 | 281.1 | 10,373.0 | 11,094.0 | 100,000 | 10 | ok |
| Polypheny Relational Normalized | Q05 | Nested lineitem MAX aggregation grouped by return flag. | 5/5 | 222,409.4 | 220,801.0 | 12,599.5 | 206,076.0 | 240,681.0 | 3 | 2 | ok |
| Polypheny Document MQL | Q01 | Filtered full customer documents. | 5/5 | 233,059.4 | 233,429.0 | 2,193.3 | 230,546.0 | 235,220.0 | 100,000 | 10 | ok |
| Polypheny Document MQL | Q02 | Filtered explicit customer projection. | 5/5 | 122,618.4 | 124,246.0 | 2,871.3 | 118,223.0 | 125,141.0 | 100,000 | 9 | ok |
| Polypheny Document MQL | Q03 | Filtered order projection. | 0/5 |  |  |  |  |  |  |  | 0/5 ok; IOException: java.lang.IndexOutOfBoundsException: Index 0 out of bounds for length 0; IOException: java.lang.IndexOutOfBoundsException: Index 0 out of bounds for length 0; IOException: java.lang.IndexOutOfBoundsException: Index 0 out of bounds for length 0; IOException: java.lang.IndexOutOfBoundsException: Index 0 out of bounds for length 0; IOException: java.lang.IndexOutOfBoundsException: Index 0 out of bounds for length 0 |
| Polypheny Document MQL | Q04 | Filtered lineitem projection. | 0/5 |  |  |  |  |  |  |  | 0/5 ok; IOException: Index 0 out of bounds for length 0; IOException: Index 0 out of bounds for length 0; IOException: Index 0 out of bounds for length 0; IOException: Index 0 out of bounds for length 0; IOException: Index 0 out of bounds for length 0 |
| Polypheny Document MQL | Q05 | Nested lineitem MAX aggregation grouped by return flag. | 0/5 |  |  |  |  |  |  |  | 0/5 ok; IOException: Could not convert {} to PolyBigDecimal; IOException: Could not convert {} to PolyBigDecimal; IOException: Could not convert {} to PolyBigDecimal; IOException: Could not convert {} to PolyBigDecimal; IOException: Could not convert {} to PolyBigDecimal |
| DuckDB | Q01 | Filtered full customer rows. | 5/5 | 504,359.8 | 298,765.0 | 502,681.7 | 208,355.0 | 1,400,388.0 | 100,000 | 9 | ok |
| DuckDB | Q02 | Filtered explicit customer projection. | 5/5 | 640.4 | 646.0 | 82.2 | 557.0 | 745.0 | 100,000 | 8 | ok |
| DuckDB | Q03 | Filtered order projection. | 5/5 | 96,091.4 | 97,781.0 | 8,018.8 | 86,416.0 | 103,882.0 | 100,000 | 6 | ok |
| DuckDB | Q04 | Filtered lineitem projection. | 5/5 | 1,575,230.8 | 1,721,657.0 | 563,393.3 | 828,355.0 | 2,198,427.0 | 100,000 | 8 | ok |
| DuckDB | Q05 | Nested lineitem MAX aggregation grouped by return flag. | 5/5 | 3,724,019.2 | 3,103,441.0 | 1,343,512.3 | 3,050,067.0 | 6,123,072.0 | 3 | 2 | ok |
| Apache Spark | Q01 | Filtered full customer rows. | 5/5 | 77,967.0 | 79,575.0 | 3,057.8 | 72,722.0 | 80,006.0 | 100,000 | 9 | ok |
| Apache Spark | Q02 | Filtered explicit customer projection. | 5/5 | 1,752.0 | 1,706.0 | 137.2 | 1,590.0 | 1,928.0 | 100,000 | 8 | ok |
| Apache Spark | Q03 | Filtered order projection. | 5/5 | 13,420.6 | 13,513.0 | 950.1 | 12,002.0 | 14,677.0 | 100,000 | 6 | ok |
| Apache Spark | Q04 | Filtered lineitem projection. | 5/5 | 16,749.2 | 17,154.0 | 1,060.4 | 14,952.0 | 17,544.0 | 100,000 | 8 | ok |
| Apache Spark | Q05 | Nested lineitem MAX aggregation grouped by return flag. | 5/5 | 15,316.4 | 15,376.0 | 322.4 | 14,800.0 | 15,608.0 | 3 | 2 | ok |
