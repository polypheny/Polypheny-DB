# Aggregation - Exploratory Welch Analysis

> This is an exploratory analysis: each tested group contains only five measurements, and benchmark execution order was not randomized. The results do not provide definitive evidence of performance equivalence or difference.

## Method

- Only `measured` rows with `success=true` are included; warm-up and failed rows are excluded.
- Tests use the raw `elapsed_ms` measurements rather than summary means.
- Every comparison is a two-sided Welch t-test, which does not assume equal variances.
- Execution order was not randomized, so caching, time trends, or other order effects may influence comparisons.
- The reported difference and unadjusted 95% confidence interval are calculated as system A minus system B.
- Raw p-values are adjusted together across all 60 comparisons using the Holm method.
- `Significant` means Holm-adjusted `p <= 0.05`.
- A non-significant result means that these measurements do not establish a difference; it does not prove equal performance.

## Inputs

| System                 | Result file                                                                                    |
|------------------------|------------------------------------------------------------------------------------------------|
| Polypheny Relational   | `plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_polypheny_results.csv`     |
| Polypheny Document MQL | `plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_polypheny_mql_results.csv` |
| DuckDB                 | `plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_duckdb_results.csv`        |
| Apache Spark           | `plugins/parquet-adapter/benchmarks/results/aggregation/aggregation_spark_results.csv`         |

## Results

### Q01

| Comparison (A vs B)                            | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p | Holm p | Significant |
|------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|-------:|:-----------:|
| Polypheny Relational vs Polypheny Document MQL |     5/5 |    12.20 +/- 3.194 |    40.40 +/- 12.01 |              -28.20 |     [-42.91, -13.49] |  -5.073 (4.563) |   0.0050 | 0.0398 |     Yes     |
| Polypheny Relational vs DuckDB                 |     5/5 |    12.20 +/- 3.194 |    22.60 +/- 1.673 |              -10.40 |     [-14.34, -6.461] |  -6.450 (6.042) |   0.0006 | 0.0096 |     Yes     |
| Polypheny Relational vs Apache Spark           |     5/5 |    12.20 +/- 3.194 | 1,415.2 +/- 184.97 |            -1,403.0 | [-1,632.6, -1,173.4] | -16.958 (4.002) | 7.06e-05 | 0.0018 |     Yes     |
| Polypheny Document MQL vs DuckDB               |     5/5 |    40.40 +/- 12.01 |    22.60 +/- 1.673 |               17.80 |       [2.960, 32.64] |   3.282 (4.155) |   0.0288 | 0.1152 |     No      |
| Polypheny Document MQL vs Apache Spark         |     5/5 |    40.40 +/- 12.01 | 1,415.2 +/- 184.97 |            -1,374.8 | [-1,604.2, -1,145.4] | -16.585 (4.034) | 7.30e-05 | 0.0018 |     Yes     |
| DuckDB vs Apache Spark                         |     5/5 |    22.60 +/- 1.673 | 1,415.2 +/- 184.97 |            -1,392.6 | [-1,622.3, -1,162.9] | -16.834 (4.001) | 7.29e-05 | 0.0018 |     Yes     |

### Q02

| Comparison (A vs B)                            | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |        95% CI (ms) |          t (df) |    Raw p | Holm p | Significant |
|------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|-------------------:|----------------:|---------:|-------:|:-----------:|
| Polypheny Relational vs Polypheny Document MQL |     5/5 |    10.40 +/- 1.517 |    41.80 +/- 8.729 |              -31.40 |   [-42.16, -20.64] |  -7.925 (4.241) |   0.0011 | 0.0137 |     Yes     |
| Polypheny Relational vs DuckDB                 |     5/5 |    10.40 +/- 1.517 |    9.200 +/- 0.837 |               1.200 |    [-0.679, 3.079] |   1.549 (6.228) |   0.1705 | 0.5115 |     No      |
| Polypheny Relational vs Apache Spark           |     5/5 |    10.40 +/- 1.517 |   379.20 +/- 28.87 |             -368.80 | [-404.62, -332.98] | -28.522 (4.022) | 8.55e-06 | 0.0003 |     Yes     |
| Polypheny Document MQL vs DuckDB               |     5/5 |    41.80 +/- 8.729 |    9.200 +/- 0.837 |               32.60 |     [21.79, 43.41] |   8.313 (4.073) |   0.0011 | 0.0137 |     Yes     |
| Polypheny Document MQL vs Apache Spark         |     5/5 |    41.80 +/- 8.729 |   379.20 +/- 28.87 |             -337.40 | [-372.69, -302.11] | -25.011 (4.725) | 3.33e-06 | 0.0001 |     Yes     |
| DuckDB vs Apache Spark                         |     5/5 |    9.200 +/- 0.837 |   379.20 +/- 28.87 |             -370.00 | [-405.84, -334.16] | -28.642 (4.007) | 8.71e-06 | 0.0003 |     Yes     |

### Q03

| Comparison (A vs B)                            | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs Polypheny Document MQL |     5/5 | 1,304.8 +/- 280.80 |   523.60 +/- 29.88 |              781.20 |    [433.61, 1,128.8] |   6.186 (4.091) |   0.0032 |   0.0290 |     Yes     |
| Polypheny Relational vs DuckDB                 |     5/5 | 1,304.8 +/- 280.80 |   179.00 +/- 15.23 |             1,125.8 |    [777.43, 1,474.2] |   8.952 (4.024) |   0.0008 |   0.0117 |     Yes     |
| Polypheny Relational vs Apache Spark           |     5/5 | 1,304.8 +/- 280.80 | 5,486.4 +/- 536.08 |            -4,181.6 | [-4,842.7, -3,520.5] | -15.451 (6.041) | 4.38e-06 |   0.0002 |     Yes     |
| Polypheny Document MQL vs DuckDB               |     5/5 |   523.60 +/- 29.88 |   179.00 +/- 15.23 |              344.60 |     [307.82, 381.38] |  22.975 (5.947) | 4.90e-07 | 2.31e-05 |     Yes     |
| Polypheny Document MQL vs Apache Spark         |     5/5 |   523.60 +/- 29.88 | 5,486.4 +/- 536.08 |            -4,962.8 | [-5,627.8, -4,297.8] | -20.669 (4.025) | 3.08e-05 |   0.0009 |     Yes     |
| DuckDB vs Apache Spark                         |     5/5 |   179.00 +/- 15.23 | 5,486.4 +/- 536.08 |            -5,307.4 | [-5,972.9, -4,641.9] | -22.129 (4.006) | 2.44e-05 |   0.0007 |     Yes     |

### Q04

| Comparison (A vs B)                            | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs Polypheny Document MQL |     5/5 | 2,468.8 +/- 293.89 | 2,249.4 +/- 136.58 |              219.40 |    [-140.62, 579.42] |   1.514 (5.651) |   0.1838 |   0.5115 |     No      |
| Polypheny Relational vs DuckDB                 |     5/5 | 2,468.8 +/- 293.89 |   765.40 +/- 73.32 |             1,703.4 |   [1,343.1, 2,063.7] |  12.575 (4.496) |   0.0001 |   0.0024 |     Yes     |
| Polypheny Relational vs Apache Spark           |     5/5 | 2,468.8 +/- 293.89 | 3,825.8 +/- 223.45 |            -1,357.0 |  [-1,742.5, -971.47] |  -8.219 (7.466) | 5.34e-05 |   0.0014 |     Yes     |
| Polypheny Document MQL vs DuckDB               |     5/5 | 2,249.4 +/- 136.58 |   765.40 +/- 73.32 |             1,484.0 |   [1,315.2, 1,652.8] |  21.407 (6.129) | 5.41e-07 | 2.49e-05 |     Yes     |
| Polypheny Document MQL vs Apache Spark         |     5/5 | 2,249.4 +/- 136.58 | 3,825.8 +/- 223.45 |            -1,576.4 | [-1,856.6, -1,296.2] | -13.460 (6.623) | 4.69e-06 |   0.0002 |     Yes     |
| DuckDB vs Apache Spark                         |     5/5 |   765.40 +/- 73.32 | 3,825.8 +/- 223.45 |            -3,060.4 | [-3,333.3, -2,787.5] | -29.099 (4.851) | 1.24e-06 | 5.58e-05 |     Yes     |

### Q05

| Comparison (A vs B)                            | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs Polypheny Document MQL |     5/5 | 1,841.2 +/- 186.45 |  1,468.8 +/- 43.87 |              372.40 |     [143.61, 601.19] |   4.347 (4.442) |   0.0097 |   0.0676 |     No      |
| Polypheny Relational vs DuckDB                 |     5/5 | 1,841.2 +/- 186.45 |  1,193.0 +/- 92.59 |              648.20 |     [419.07, 877.33] |   6.962 (5.860) |   0.0005 |   0.0077 |     Yes     |
| Polypheny Relational vs Apache Spark           |     5/5 | 1,841.2 +/- 186.45 | 5,447.2 +/- 306.09 |            -3,606.0 | [-3,989.6, -3,222.4] | -22.498 (6.609) | 1.71e-07 | 8.37e-06 |     Yes     |
| Polypheny Document MQL vs DuckDB               |     5/5 |  1,468.8 +/- 43.87 |  1,193.0 +/- 92.59 |              275.80 |     [162.29, 389.31] |   6.019 (5.710) |   0.0011 |   0.0137 |     Yes     |
| Polypheny Document MQL vs Apache Spark         |     5/5 |  1,468.8 +/- 43.87 | 5,447.2 +/- 306.09 |            -3,978.4 | [-4,356.4, -3,600.4] | -28.769 (4.164) | 5.99e-06 |   0.0002 |     Yes     |
| DuckDB vs Apache Spark                         |     5/5 |  1,193.0 +/- 92.59 | 5,447.2 +/- 306.09 |            -4,254.2 | [-4,628.3, -3,880.1] | -29.747 (4.726) | 1.47e-06 | 6.47e-05 |     Yes     |

### Q06

| Comparison (A vs B)                            | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs Polypheny Document MQL |     5/5 |    12.40 +/- 2.074 |    45.40 +/- 3.847 |              -33.00 |     [-37.76, -28.24] | -16.884 (6.143) | 2.22e-06 | 9.54e-05 |     Yes     |
| Polypheny Relational vs DuckDB                 |     5/5 |    12.40 +/- 2.074 |   145.40 +/- 3.847 |             -133.00 |   [-137.76, -128.24] | -68.049 (6.143) | 4.47e-10 | 2.55e-08 |     Yes     |
| Polypheny Relational vs Apache Spark           |     5/5 |    12.40 +/- 2.074 | 2,027.4 +/- 175.38 |            -2,015.0 | [-2,232.8, -1,797.2] | -25.689 (4.001) | 1.36e-05 |   0.0004 |     Yes     |
| Polypheny Document MQL vs DuckDB               |     5/5 |    45.40 +/- 3.847 |   145.40 +/- 3.847 |             -100.00 |    [-105.61, -94.39] | -41.100 (8.000) | 1.35e-10 | 7.98e-09 |     Yes     |
| Polypheny Document MQL vs Apache Spark         |     5/5 |    45.40 +/- 3.847 | 2,027.4 +/- 175.38 |            -1,982.0 | [-2,199.7, -1,764.3] | -25.264 (4.004) | 1.45e-05 |   0.0005 |     Yes     |
| DuckDB vs Apache Spark                         |     5/5 |   145.40 +/- 3.847 | 2,027.4 +/- 175.38 |            -1,882.0 | [-2,099.7, -1,664.3] | -23.990 (4.004) | 1.78e-05 |   0.0006 |     Yes     |

### Q07

| Comparison (A vs B)                            | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |        95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|-------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs Polypheny Document MQL |     5/5 |    14.20 +/- 1.304 |    57.60 +/- 5.320 |              -43.40 |   [-49.92, -36.88] | -17.718 (4.479) | 2.56e-05 |   0.0007 |     Yes     |
| Polypheny Relational vs DuckDB                 |     5/5 |    14.20 +/- 1.304 |    34.00 +/- 4.183 |              -19.80 |   [-24.91, -14.69] | -10.104 (4.770) |   0.0002 |   0.0040 |     Yes     |
| Polypheny Relational vs Apache Spark           |     5/5 |    14.20 +/- 1.304 |   275.60 +/- 8.325 |             -261.40 | [-271.67, -251.13] | -69.368 (4.196) | 1.40e-07 | 6.99e-06 |     Yes     |
| Polypheny Document MQL vs DuckDB               |     5/5 |    57.60 +/- 5.320 |    34.00 +/- 4.183 |               23.60 |     [16.55, 30.65] |   7.798 (7.579) | 7.04e-05 |   0.0018 |     Yes     |
| Polypheny Document MQL vs Apache Spark         |     5/5 |    57.60 +/- 5.320 |   275.60 +/- 8.325 |             -218.00 | [-228.51, -207.49] | -49.342 (6.800) | 6.06e-10 | 3.40e-08 |     Yes     |
| DuckDB vs Apache Spark                         |     5/5 |    34.00 +/- 4.183 |   275.60 +/- 8.325 |             -241.60 | [-251.84, -231.36] | -57.986 (5.899) | 2.33e-09 | 1.24e-07 |     Yes     |

### Q08

| Comparison (A vs B)                            | n (A/B) |  A mean +/- SD (ms) |  B mean +/- SD (ms) | Difference A-B (ms) |           95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|------------------------------------------------|--------:|--------------------:|--------------------:|--------------------:|----------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs Polypheny Document MQL |     5/5 | 16,174.2 +/- 475.26 | 12,680.8 +/- 941.70 |             3,493.4 |    [2,335.0, 4,651.8] |   7.405 (5.914) |   0.0003 |   0.0057 |     Yes     |
| Polypheny Relational vs DuckDB                 |     5/5 | 16,174.2 +/- 475.26 |  4,584.6 +/- 305.19 |            11,589.6 |  [10,989.1, 12,190.1] |  45.883 (6.819) | 9.47e-10 | 5.11e-08 |     Yes     |
| Polypheny Relational vs Apache Spark           |     5/5 | 16,174.2 +/- 475.26 | 14,250.8 +/- 361.58 |             1,923.4 |    [1,299.8, 2,547.0] |   7.202 (7.469) |   0.0001 |   0.0026 |     Yes     |
| Polypheny Document MQL vs DuckDB               |     5/5 | 12,680.8 +/- 941.70 |  4,584.6 +/- 305.19 |             8,096.2 |    [6,946.1, 9,246.3] |  18.288 (4.831) | 1.20e-05 |   0.0004 |     Yes     |
| Polypheny Document MQL vs Apache Spark         |     5/5 | 12,680.8 +/- 941.70 | 14,250.8 +/- 361.58 |            -1,570.0 |   [-2,719.3, -420.73] |  -3.480 (5.154) |   0.0168 |   0.0840 |     No      |
| DuckDB vs Apache Spark                         |     5/5 |  4,584.6 +/- 305.19 | 14,250.8 +/- 361.58 |            -9,666.2 | [-10,156.6, -9,175.8] | -45.681 (7.781) | 9.76e-11 | 5.86e-09 |     Yes     |

### Q09

| Comparison (A vs B)                            | n (A/B) |  A mean +/- SD (ms) |  B mean +/- SD (ms) | Difference A-B (ms) |            95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|------------------------------------------------|--------:|--------------------:|--------------------:|--------------------:|-----------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs Polypheny Document MQL |     5/5 | 15,590.2 +/- 841.62 | 13,218.0 +/- 279.78 |             2,372.2 |     [1,344.6, 3,399.8] |   5.981 (4.873) |   0.0020 |   0.0205 |     Yes     |
| Polypheny Relational vs DuckDB                 |     5/5 | 15,590.2 +/- 841.62 |  8,844.2 +/- 369.44 |             6,746.0 |     [5,716.9, 7,775.1] |  16.412 (5.486) | 7.13e-06 |   0.0003 |     Yes     |
| Polypheny Relational vs Apache Spark           |     5/5 | 15,590.2 +/- 841.62 | 34,324.6 +/- 735.65 |           -18,734.4 | [-19,890.8, -17,578.0] | -37.476 (7.859) | 3.82e-10 | 2.21e-08 |     Yes     |
| Polypheny Document MQL vs DuckDB               |     5/5 | 13,218.0 +/- 279.78 |  8,844.2 +/- 369.44 |             4,373.8 |     [3,889.7, 4,857.9] |  21.104 (7.453) | 6.42e-08 | 3.28e-06 |     Yes     |
| Polypheny Document MQL vs Apache Spark         |     5/5 | 13,218.0 +/- 279.78 | 34,324.6 +/- 735.65 |           -21,106.6 | [-22,004.4, -20,208.8] | -59.965 (5.133) | 1.66e-08 | 8.65e-07 |     Yes     |
| DuckDB vs Apache Spark                         |     5/5 |  8,844.2 +/- 369.44 | 34,324.6 +/- 735.65 |           -25,480.4 | [-26,385.1, -24,575.7] | -69.212 (5.897) | 8.27e-10 | 4.55e-08 |     Yes     |

### Q10

| Comparison (A vs B)                            | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|---------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational vs Polypheny Document MQL |     5/5 | 5,458.2 +/- 655.76 | 4,283.4 +/- 176.21 |             1,174.8 |    [371.85, 1,977.7] |   3.869 (4.575) |   0.0140 |   0.0838 |     No      |
| Polypheny Relational vs DuckDB                 |     5/5 | 5,458.2 +/- 655.76 |   435.40 +/- 42.24 |             5,022.8 |   [4,209.5, 5,836.1] |  17.092 (4.033) | 6.48e-05 |   0.0017 |     Yes     |
| Polypheny Relational vs Apache Spark           |     5/5 | 5,458.2 +/- 655.76 | 5,407.8 +/- 309.74 |               50.40 |    [-753.44, 854.24] |   0.155 (5.700) |   0.8819 |   0.8819 |     No      |
| Polypheny Document MQL vs DuckDB               |     5/5 | 4,283.4 +/- 176.21 |   435.40 +/- 42.24 |             3,848.0 |   [3,631.8, 4,064.2] |  47.487 (4.458) | 3.34e-07 | 1.61e-05 |     Yes     |
| Polypheny Document MQL vs Apache Spark         |     5/5 | 4,283.4 +/- 176.21 | 5,407.8 +/- 309.74 |            -1,124.4 |  [-1,509.3, -739.51] |  -7.056 (6.344) |   0.0003 |   0.0057 |     Yes     |
| DuckDB vs Apache Spark                         |     5/5 |   435.40 +/- 42.24 | 5,407.8 +/- 309.74 |            -4,972.4 | [-5,355.1, -4,589.7] | -35.568 (4.149) | 2.58e-06 |   0.0001 |     Yes     |
