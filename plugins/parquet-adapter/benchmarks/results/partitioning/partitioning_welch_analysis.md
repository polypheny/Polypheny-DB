# Partitioning - Exploratory Welch Analysis

> This is an exploratory analysis: each tested group contains only five measurements, and benchmark execution order was not randomized. The results do not provide definitive evidence of performance equivalence or difference.

## Method

- Only `measured` rows with `success=true` are included; warm-up and failed rows are excluded.
- Tests use the raw `elapsed_ms` measurements rather than summary means.
- Every comparison is a two-sided Welch t-test, which does not assume equal variances.
- Execution order was not randomized, so caching, time trends, or other order effects may influence comparisons.
- The reported difference and unadjusted 95% confidence interval are calculated as system A minus system B.
- Raw p-values are adjusted together across all 45 comparisons using the Holm method.
- `Significant` means Holm-adjusted `p <= 0.05`.
- A non-significant result means that these measurements do not establish a difference; it does not prove equal performance.

## Inputs

| System                     | Result file                                                                                             |
|----------------------------|---------------------------------------------------------------------------------------------------------|
| Polypheny Relational       | `plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_polypheny_results.csv`            |
| DuckDB Repartitioned       | `plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_duckdb_repartitioned_results.csv` |
| DuckDB Unpartitioned       | `plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_duckdb_unpartitioned_results.csv` |
| Apache Spark Repartitioned | `plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_spark_repartitioned_results.csv`  |
| Apache Spark Unpartitioned | `plugins/parquet-adapter/benchmarks/results/partitioning/partitioning_spark_unpartitioned_results.csv`  |

## Results

### Q1_NP

| Comparison (A vs B)                                | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |         95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|----------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|--------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs DuckDB Unpartitioned       |     5/5 |    9.600 +/- 1.140 |    59.60 +/- 2.408 |              -50.00 |    [-52.95, -47.05] | -41.959 (5.707) | 2.50e-08 | 1.10e-06 |     Yes     |
| Polypheny Relational vs Apache Spark Unpartitioned |     5/5 |    9.600 +/- 1.140 | 1,189.6 +/- 190.40 |            -1,180.0 | [-1,416.4, -943.59] | -13.858 (4.000) |   0.0002 |   0.0027 |     Yes     |
| DuckDB Unpartitioned vs Apache Spark Unpartitioned |     5/5 |    59.60 +/- 2.408 | 1,189.6 +/- 190.40 |            -1,130.0 | [-1,366.4, -893.60] | -13.270 (4.001) |   0.0002 |   0.0027 |     Yes     |

### Q1_P

| Comparison (A vs B)                                | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p | Holm p | Significant |
|----------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|-------:|:-----------:|
| Polypheny Relational vs DuckDB Repartitioned       |     5/5 |    7.200 +/- 0.837 |    78.80 +/- 11.28 |              -71.60 |     [-85.58, -57.62] | -14.157 (4.044) |   0.0001 | 0.0026 |     Yes     |
| Polypheny Relational vs Apache Spark Repartitioned |     5/5 |    7.200 +/- 0.837 | 1,681.8 +/- 204.77 |            -1,674.6 | [-1,928.9, -1,420.3] | -18.286 (4.000) | 5.26e-05 | 0.0013 |     Yes     |
| DuckDB Repartitioned vs Apache Spark Repartitioned |     5/5 |    78.80 +/- 11.28 | 1,681.8 +/- 204.77 |            -1,603.0 | [-1,857.0, -1,349.0] | -17.478 (4.024) | 6.02e-05 | 0.0014 |     Yes     |

### Q2_NP

| Comparison (A vs B)                                | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|----------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs DuckDB Unpartitioned       |     5/5 |    9.600 +/- 1.342 |    88.40 +/- 5.413 |              -78.80 |     [-85.44, -72.16] | -31.596 (4.490) | 1.90e-06 | 7.23e-05 |     Yes     |
| Polypheny Relational vs Apache Spark Unpartitioned |     5/5 |    9.600 +/- 1.342 |  2,123.2 +/- 94.05 |            -2,113.6 | [-2,230.4, -1,996.8] | -50.244 (4.002) | 9.35e-07 | 3.83e-05 |     Yes     |
| DuckDB Unpartitioned vs Apache Spark Unpartitioned |     5/5 |    88.40 +/- 5.413 |  2,123.2 +/- 94.05 |            -2,034.8 | [-2,151.5, -1,918.1] | -48.296 (4.026) | 1.02e-06 | 4.08e-05 |     Yes     |

### Q2_P

| Comparison (A vs B)                                | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |        95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|----------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|-------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs DuckDB Repartitioned       |     5/5 |    6.600 +/- 0.894 |    35.20 +/- 1.789 |              -28.60 |   [-30.80, -26.40] | -31.976 (5.882) | 8.01e-08 | 3.45e-06 |     Yes     |
| Polypheny Relational vs Apache Spark Repartitioned |     5/5 |    6.600 +/- 0.894 |   641.80 +/- 81.33 |             -635.20 | [-736.19, -534.21] | -17.462 (4.001) | 6.30e-05 |   0.0014 |     Yes     |
| DuckDB Repartitioned vs Apache Spark Repartitioned |     5/5 |    35.20 +/- 1.789 |   641.80 +/- 81.33 |             -606.60 | [-707.58, -505.62] | -16.673 (4.004) | 7.53e-05 |   0.0015 |     Yes     |

### Q3_NP

| Comparison (A vs B)                                | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p | Holm p | Significant |
|----------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|-------:|:-----------:|
| Polypheny Relational vs DuckDB Unpartitioned       |     5/5 |    9.800 +/- 1.924 |    72.20 +/- 5.630 |              -62.40 |     [-69.27, -55.53] | -23.451 (4.921) | 3.06e-06 | 0.0001 |     Yes     |
| Polypheny Relational vs Apache Spark Unpartitioned |     5/5 |    9.800 +/- 1.924 | 1,326.2 +/- 101.33 |            -1,316.4 | [-1,442.2, -1,190.6] | -29.045 (4.003) | 8.31e-06 | 0.0003 |     Yes     |
| DuckDB Unpartitioned vs Apache Spark Unpartitioned |     5/5 |    72.20 +/- 5.630 | 1,326.2 +/- 101.33 |            -1,254.0 | [-1,379.7, -1,128.3] | -27.630 (4.025) | 9.66e-06 | 0.0003 |     Yes     |

### Q3_P

| Comparison (A vs B)                                | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |        95% CI (ms) |          t (df) |  Raw p | Holm p | Significant |
|----------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|-------------------:|----------------:|-------:|-------:|:-----------:|
| Polypheny Relational vs DuckDB Repartitioned       |     5/5 |    8.600 +/- 1.140 |    17.00 +/- 2.000 |              -8.400 |   [-10.89, -5.914] |  -8.159 (6.352) | 0.0001 | 0.0026 |     Yes     |
| Polypheny Relational vs Apache Spark Repartitioned |     5/5 |    8.600 +/- 1.140 |   450.60 +/- 80.23 |             -442.00 | [-541.61, -342.39] | -12.318 (4.002) | 0.0002 | 0.0030 |     Yes     |
| DuckDB Repartitioned vs Apache Spark Repartitioned |     5/5 |    17.00 +/- 2.000 |   450.60 +/- 80.23 |             -433.60 | [-533.20, -334.00] | -12.082 (4.005) | 0.0003 | 0.0030 |     Yes     |

### Q4_NP

| Comparison (A vs B)                                | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |         95% CI (ms) |          t (df) |    Raw p | Holm p | Significant |
|----------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|--------------------:|----------------:|---------:|-------:|:-----------:|
| Polypheny Relational vs DuckDB Unpartitioned       |     5/5 |   131.20 +/- 14.89 |    20.60 +/- 1.342 |              110.60 |     [92.15, 129.05] |  16.543 (4.065) | 6.99e-05 | 0.0015 |     Yes     |
| Polypheny Relational vs Apache Spark Unpartitioned |     5/5 |   131.20 +/- 14.89 |  902.60 +/- 145.16 |             -771.40 |  [-951.12, -591.68] | -11.821 (4.084) |   0.0003 | 0.0030 |     Yes     |
| DuckDB Unpartitioned vs Apache Spark Unpartitioned |     5/5 |    20.60 +/- 1.342 |  902.60 +/- 145.16 |             -882.00 | [-1,062.2, -701.77] | -13.586 (4.001) |   0.0002 | 0.0027 |     Yes     |

### Q4_P

| Comparison (A vs B)                                | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p | Holm p | Significant |
|----------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|-------:|:-----------:|
| Polypheny Relational vs DuckDB Repartitioned       |     5/5 |   150.00 +/- 14.63 |    30.20 +/- 1.304 |              119.80 |     [101.68, 137.92] |  18.240 (4.064) | 4.73e-05 | 0.0012 |     Yes     |
| Polypheny Relational vs Apache Spark Repartitioned |     5/5 |   150.00 +/- 14.63 | 1,693.0 +/- 144.26 |            -1,543.0 | [-1,721.6, -1,364.4] | -23.795 (4.082) | 1.56e-05 | 0.0005 |     Yes     |
| DuckDB Repartitioned vs Apache Spark Repartitioned |     5/5 |    30.20 +/- 1.304 | 1,693.0 +/- 144.26 |            -1,662.8 | [-1,841.9, -1,483.7] | -25.772 (4.001) | 1.34e-05 | 0.0004 |     Yes     |

### Q5_NP

| Comparison (A vs B)                                | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |         95% CI (ms) |          t (df) |    Raw p | Holm p | Significant |
|----------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|--------------------:|----------------:|---------:|-------:|:-----------:|
| Polypheny Relational vs DuckDB Unpartitioned       |     5/5 |    61.60 +/- 6.025 |    13.60 +/- 0.548 |               48.00 |      [40.54, 55.46] |  17.741 (4.066) | 5.27e-05 | 0.0013 |     Yes     |
| Polypheny Relational vs Apache Spark Unpartitioned |     5/5 |    61.60 +/- 6.025 | 1,035.4 +/- 101.68 |             -973.80 | [-1,099.9, -847.67] | -21.378 (4.028) | 2.68e-05 | 0.0007 |     Yes     |
| DuckDB Unpartitioned vs Apache Spark Unpartitioned |     5/5 |    13.60 +/- 0.548 | 1,035.4 +/- 101.68 |            -1,021.8 | [-1,148.1, -895.55] | -22.470 (4.000) | 2.32e-05 | 0.0007 |     Yes     |

### Q5_P

| Comparison (A vs B)                                | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |        95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|----------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|-------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs DuckDB Repartitioned       |     5/5 |    54.60 +/- 3.050 |    17.40 +/- 1.140 |               37.20 |     [33.48, 40.92] |  25.549 (5.097) | 1.41e-06 | 5.50e-05 |     Yes     |
| Polypheny Relational vs Apache Spark Repartitioned |     5/5 |    54.60 +/- 3.050 |  637.80 +/- 101.51 |             -583.20 | [-709.21, -457.19] | -12.841 (4.007) |   0.0002 |   0.0027 |     Yes     |
| DuckDB Repartitioned vs Apache Spark Repartitioned |     5/5 |    17.40 +/- 1.140 |  637.80 +/- 101.51 |             -620.40 | [-746.44, -494.36] | -13.665 (4.001) |   0.0002 |   0.0027 |     Yes     |

### Q1_P vs Q1_NP

| Comparison (A vs B)                                                     | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |      95% CI (ms) |         t (df) |  Raw p | Holm p | Significant |
|-------------------------------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|-----------------:|---------------:|-------:|-------:|:-----------:|
| Polypheny Relational [Q1_P] vs Polypheny Relational [Q1_NP]             |     5/5 |    7.200 +/- 0.837 |    9.600 +/- 1.140 |              -2.400 | [-3.882, -0.918] | -3.795 (7.339) | 0.0062 | 0.0309 |     Yes     |
| DuckDB Repartitioned [Q1_P] vs DuckDB Unpartitioned [Q1_NP]             |     5/5 |    78.80 +/- 11.28 |    59.60 +/- 2.408 |               19.20 |   [5.339, 33.06] |  3.723 (4.364) | 0.0175 | 0.0699 |     No      |
| Apache Spark Repartitioned [Q1_P] vs Apache Spark Unpartitioned [Q1_NP] |     5/5 | 1,681.8 +/- 204.77 | 1,189.6 +/- 190.40 |              492.20 | [203.58, 780.82] |  3.936 (7.958) | 0.0044 | 0.0300 |     Yes     |

### Q2_P vs Q2_NP

| Comparison (A vs B)                                                     | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|-------------------------------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational [Q2_P] vs Polypheny Relational [Q2_NP]             |     5/5 |    6.600 +/- 0.894 |    9.600 +/- 1.342 |              -3.000 |     [-4.707, -1.293] |  -4.160 (6.969) |   0.0043 |   0.0300 |     Yes     |
| DuckDB Repartitioned [Q2_P] vs DuckDB Unpartitioned [Q2_NP]             |     5/5 |    35.20 +/- 1.789 |    88.40 +/- 5.413 |              -53.20 |     [-59.81, -46.59] | -20.867 (4.863) | 6.02e-06 |   0.0002 |     Yes     |
| Apache Spark Repartitioned [Q2_P] vs Apache Spark Unpartitioned [Q2_NP] |     5/5 |   641.80 +/- 81.33 |  2,123.2 +/- 94.05 |            -1,481.4 | [-1,610.1, -1,352.7] | -26.640 (7.837) | 5.70e-09 | 2.57e-07 |     Yes     |

### Q3_P vs Q3_NP

| Comparison (A vs B)                                                     | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |         95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|-------------------------------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|--------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational [Q3_P] vs Polypheny Relational [Q3_NP]             |     5/5 |    8.600 +/- 1.140 |    9.800 +/- 1.924 |              -1.200 |     [-3.602, 1.202] |  -1.200 (6.502) |   0.2720 |   0.2720 |     No      |
| DuckDB Repartitioned [Q3_P] vs DuckDB Unpartitioned [Q3_NP]             |     5/5 |    17.00 +/- 2.000 |    72.20 +/- 5.630 |              -55.20 |    [-62.07, -48.33] | -20.658 (4.994) | 4.98e-06 |   0.0002 |     Yes     |
| Apache Spark Repartitioned [Q3_P] vs Apache Spark Unpartitioned [Q3_NP] |     5/5 |   450.60 +/- 80.23 | 1,326.2 +/- 101.33 |             -875.60 | [-1,010.1, -741.09] | -15.149 (7.600) | 5.96e-07 | 2.50e-05 |     Yes     |

### Q4_P vs Q4_NP

| Comparison (A vs B)                                                     | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |       95% CI (ms) |         t (df) |    Raw p | Holm p | Significant |
|-------------------------------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|------------------:|---------------:|---------:|-------:|:-----------:|
| Polypheny Relational [Q4_P] vs Polypheny Relational [Q4_NP]             |     5/5 |   150.00 +/- 14.63 |   131.20 +/- 14.89 |               18.80 |   [-2.727, 40.33] |  2.014 (7.998) |   0.0788 | 0.1805 |     No      |
| DuckDB Repartitioned [Q4_P] vs DuckDB Unpartitioned [Q4_NP]             |     5/5 |    30.20 +/- 1.304 |    20.60 +/- 1.342 |               9.600 |    [7.670, 11.53] | 11.474 (7.993) | 3.03e-06 | 0.0001 |     Yes     |
| Apache Spark Repartitioned [Q4_P] vs Apache Spark Unpartitioned [Q4_NP] |     5/5 | 1,693.0 +/- 144.26 |  902.60 +/- 145.16 |              790.40 | [579.35, 1,001.5] |  8.636 (8.000) | 2.51e-05 | 0.0007 |     Yes     |

### Q5_P vs Q5_NP

| Comparison (A vs B)                                                     | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |        95% CI (ms) |         t (df) |  Raw p | Holm p | Significant |
|-------------------------------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|-------------------:|---------------:|-------:|-------:|:-----------:|
| Polypheny Relational [Q5_P] vs Polypheny Relational [Q5_NP]             |     5/5 |    54.60 +/- 3.050 |    61.60 +/- 6.025 |              -7.000 |    [-14.41, 0.413] | -2.318 (5.923) | 0.0602 | 0.1805 |     No      |
| DuckDB Repartitioned [Q5_P] vs DuckDB Unpartitioned [Q5_NP]             |     5/5 |    17.40 +/- 1.140 |    13.60 +/- 0.548 |               3.800 |     [2.401, 5.199] |  6.718 (5.753) | 0.0006 | 0.0050 |     Yes     |
| Apache Spark Repartitioned [Q5_P] vs Apache Spark Unpartitioned [Q5_NP] |     5/5 |  637.80 +/- 101.51 | 1,035.4 +/- 101.68 |             -397.60 | [-545.77, -249.43] | -6.188 (8.000) | 0.0003 | 0.0030 |     Yes     |
