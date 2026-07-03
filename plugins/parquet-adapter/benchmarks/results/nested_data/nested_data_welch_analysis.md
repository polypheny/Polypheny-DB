# Nested Data - Exploratory Welch Analysis

> This is an exploratory analysis: each tested group contains only five measurements, and benchmark execution order was not randomized. The results do not provide definitive evidence of performance equivalence or difference.

## Method

- Only `measured` rows with `success=true` are included; warm-up and failed rows are excluded.
- Tests use the raw `elapsed_ms` measurements rather than summary means.
- Every comparison is a two-sided Welch t-test, which does not assume equal variances.
- Execution order was not randomized, so caching, time trends, or other order effects may influence comparisons.
- The reported difference and unadjusted 95% confidence interval are calculated as system A minus system B.
- Raw p-values are adjusted together across all 21 comparisons using the Holm method.
- `Significant` means Holm-adjusted `p <= 0.05`.
- A non-significant result means that these measurements do not establish a difference; it does not prove equal performance.

## Inputs

| System                          | Result file                                                                                           |
|---------------------------------|-------------------------------------------------------------------------------------------------------|
| Polypheny Relational Normalized | `plugins/parquet-adapter/benchmarks/results/nested_data/nested_data_polypheny_normalized_results.csv` |
| Polypheny Document MQL          | `plugins/parquet-adapter/benchmarks/results/nested_data/nested_data_polypheny_mql_results.csv`        |
| DuckDB                          | `plugins/parquet-adapter/benchmarks/results/nested_data/nested_data_duckdb_results.csv`               |
| Apache Spark                    | `plugins/parquet-adapter/benchmarks/results/nested_data/nested_data_spark_results.csv`                |

## Results

### Q01

| Comparison (A vs B)                                       | n (A/B) |      A mean +/- SD (ms) |      B mean +/- SD (ms) | Difference A-B (ms) |               95% CI (ms) |           t (df) |    Raw p |   Holm p | Significant |
|-----------------------------------------------------------|--------:|------------------------:|------------------------:|--------------------:|--------------------------:|-----------------:|---------:|---------:|:-----------:|
| Polypheny Relational Normalized vs Polypheny Document MQL |     5/5 |      2,463.6 +/- 744.81 |   233,059.4 +/- 2,193.3 |          -230,595.8 |  [-233,273.3, -227,918.3] | -222.605 (4.910) | 5.06e-11 | 1.01e-09 |     Yes     |
| Polypheny Relational Normalized vs DuckDB                 |     5/5 |      2,463.6 +/- 744.81 | 504,359.8 +/- 502,681.7 |          -501,896.2 | [-1,126,057.6, 122,265.2] |   -2.233 (4.000) |   0.0894 |   0.2681 |     No      |
| Polypheny Relational Normalized vs Apache Spark           |     5/5 |      2,463.6 +/- 744.81 |    77,967.0 +/- 3,057.8 |           -75,503.4 |    [-79,253.5, -71,753.3] |  -53.645 (4.473) | 1.86e-07 | 2.98e-06 |     Yes     |
| Polypheny Document MQL vs DuckDB                          |     5/5 |   233,059.4 +/- 2,193.3 | 504,359.8 +/- 502,681.7 |          -271,300.4 |   [-895,458.8, 352,858.0] |   -1.207 (4.000) |   0.2940 |   0.2940 |     No      |
| Polypheny Document MQL vs Apache Spark                    |     5/5 |   233,059.4 +/- 2,193.3 |    77,967.0 +/- 3,057.8 |           155,092.4 |    [151,141.1, 159,043.7] |   92.158 (7.255) | 2.12e-12 | 4.44e-11 |     Yes     |
| DuckDB vs Apache Spark                                    |     5/5 | 504,359.8 +/- 502,681.7 |    77,967.0 +/- 3,057.8 |           426,392.8 | [-197,762.3, 1,050,547.9] |    1.897 (4.000) |   0.1307 |   0.2681 |     No      |

### Q02

| Comparison (A vs B)                                       | n (A/B) |    A mean +/- SD (ms) |    B mean +/- SD (ms) | Difference A-B (ms) |              95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|-----------------------------------------------------------|--------:|----------------------:|----------------------:|--------------------:|-------------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational Normalized vs Polypheny Document MQL |     5/5 |    1,176.8 +/- 176.86 | 122,618.4 +/- 2,871.3 |          -121,441.6 | [-125,002.9, -117,880.3] | -94.396 (4.030) | 6.80e-08 | 1.29e-06 |     Yes     |
| Polypheny Relational Normalized vs DuckDB                 |     5/5 |    1,176.8 +/- 176.86 |      640.40 +/- 82.16 |              536.40 |         [319.75, 753.05] |   6.150 (5.649) |   0.0011 |   0.0085 |     Yes     |
| Polypheny Relational Normalized vs Apache Spark           |     5/5 |    1,176.8 +/- 176.86 |    1,752.0 +/- 137.24 |             -575.20 |       [-808.57, -341.83] |  -5.745 (7.535) |   0.0005 |   0.0048 |     Yes     |
| Polypheny Document MQL vs DuckDB                          |     5/5 | 122,618.4 +/- 2,871.3 |      640.40 +/- 82.16 |           121,978.0 |   [118,413.7, 125,542.3] |  94.954 (4.007) | 7.21e-08 | 1.30e-06 |     Yes     |
| Polypheny Document MQL vs Apache Spark                    |     5/5 | 122,618.4 +/- 2,871.3 |    1,752.0 +/- 137.24 |           120,866.4 |   [117,303.6, 124,429.2] |  94.020 (4.018) | 7.20e-08 | 1.30e-06 |     Yes     |
| DuckDB vs Apache Spark                                    |     5/5 |      640.40 +/- 82.16 |    1,752.0 +/- 137.24 |            -1,111.6 |      [-1,283.2, -940.02] | -15.540 (6.541) | 2.08e-06 | 3.12e-05 |     Yes     |

### Q03

| Comparison (A vs B)                             | n (A/B) |   A mean +/- SD (ms) |   B mean +/- SD (ms) | Difference A-B (ms) |             95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|-------------------------------------------------|--------:|---------------------:|---------------------:|--------------------:|------------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational Normalized vs DuckDB       |     5/5 |   2,727.8 +/- 300.10 | 96,091.4 +/- 8,018.8 |           -93,363.6 | [-103,316.3, -83,410.9] | -26.016 (4.011) | 1.27e-05 |   0.0002 |     Yes     |
| Polypheny Relational Normalized vs Apache Spark |     5/5 |   2,727.8 +/- 300.10 |  13,420.6 +/- 950.10 |           -10,692.8 |   [-11,853.5, -9,532.1] | -23.997 (4.790) | 3.55e-06 | 4.61e-05 |     Yes     |
| DuckDB vs Apache Spark                          |     5/5 | 96,091.4 +/- 8,018.8 |  13,420.6 +/- 950.10 |            82,670.8 |    [72,751.5, 92,590.1] |  22.893 (4.112) | 1.71e-05 |   0.0002 |     Yes     |

### Q04

| Comparison (A vs B)                             | n (A/B) |        A mean +/- SD (ms) |        B mean +/- SD (ms) | Difference A-B (ms) |                95% CI (ms) |          t (df) |  Raw p | Holm p | Significant |
|-------------------------------------------------|--------:|--------------------------:|--------------------------:|--------------------:|---------------------------:|----------------:|-------:|-------:|:-----------:|
| Polypheny Relational Normalized vs DuckDB       |     5/5 |       10,845.6 +/- 281.05 | 1,575,230.8 +/- 563,393.3 |        -1,564,385.2 | [-2,263,930.4, -864,840.0] |  -6.209 (4.000) | 0.0034 | 0.0240 |     Yes     |
| Polypheny Relational Normalized vs Apache Spark |     5/5 |       10,845.6 +/- 281.05 |      16,749.2 +/- 1,060.4 |            -5,903.6 |       [-7,202.2, -4,605.0] | -12.034 (4.559) | 0.0001 | 0.0013 |     Yes     |
| DuckDB vs Apache Spark                          |     5/5 | 1,575,230.8 +/- 563,393.3 |      16,749.2 +/- 1,060.4 |         1,558,481.6 |   [858,937.1, 2,258,026.1] |   6.185 (4.000) | 0.0035 | 0.0240 |     Yes     |

### Q05

| Comparison (A vs B)                             | n (A/B) |          A mean +/- SD (ms) |          B mean +/- SD (ms) | Difference A-B (ms) |                  95% CI (ms) |         t (df) |    Raw p |   Holm p | Significant |
|-------------------------------------------------|--------:|----------------------------:|----------------------------:|--------------------:|-----------------------------:|---------------:|---------:|---------:|:-----------:|
| Polypheny Relational Normalized vs DuckDB       |     5/5 |      222,409.4 +/- 12,599.5 | 3,724,019.2 +/- 1,343,512.3 |        -3,501,609.8 | [-5,169,758.3, -1,833,461.3] | -5.828 (4.001) |   0.0043 |   0.0240 |     Yes     |
| Polypheny Relational Normalized vs Apache Spark |     5/5 |      222,409.4 +/- 12,599.5 |         15,316.4 +/- 322.38 |           207,093.0 |       [191,451.6, 222,734.4] | 36.741 (4.005) | 3.23e-06 | 4.53e-05 |     Yes     |
| DuckDB vs Apache Spark                          |     5/5 | 3,724,019.2 +/- 1,343,512.3 |         15,316.4 +/- 322.38 |         3,708,702.8 |   [2,040,512.0, 5,376,893.6] |  6.173 (4.000) |   0.0035 |   0.0240 |     Yes     |

## Skipped Data

The following system/query groups had fewer than two successful measured runs and were not used in same-query pairwise tests:

| Query | System                 | Successful runs |
|-------|------------------------|----------------:|
| Q03   | Polypheny Document MQL |               0 |
| Q04   | Polypheny Document MQL |               0 |
| Q05   | Polypheny Document MQL |               0 |
