# Access Model Comparison - Exploratory Welch Analysis

> This is an exploratory analysis: each tested group contains only five measurements, and benchmark execution order was not randomized. The results do not provide definitive evidence of performance equivalence or difference.

## Method

- Only `measured` rows with `success=true` are included; warm-up and failed rows are excluded.
- Tests use the raw `elapsed_ms` measurements rather than summary means.
- Every comparison is a two-sided Welch t-test, which does not assume equal variances.
- Execution order was not randomized, so caching, time trends, or other order effects may influence comparisons.
- The reported difference and unadjusted 95% confidence interval are calculated as system A minus system B.
- Raw p-values are adjusted together across all 50 comparisons using the Holm method.
- `Significant` means Holm-adjusted `p <= 0.05`.
- A non-significant result means that these measurements do not establish a difference; it does not prove equal performance.

## Inputs

| System                          | Result file                                                                                                                 |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| Polypheny Relational Flat       | `plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_polypheny_rf_tlcp_results.csv`  |
| Polypheny Relational Normalized | `plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_polypheny_rn_tlcpn_results.csv` |
| Polypheny Document MQL          | `plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_polypheny_mql_tlcp_results.csv` |
| DuckDB                          | `plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_duckdb_tlcp_results.csv`        |
| Apache Spark                    | `plugins/parquet-adapter/benchmarks/results/access_model_comparison/access_model_comparison_spark_tlcp_results.csv`         |

## Results

### Q01

| Comparison (A vs B)                                          | n (A/B) |    A mean +/- SD (ms) |    B mean +/- SD (ms) | Difference A-B (ms) |              95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|--------------------------------------------------------------|--------:|----------------------:|----------------------:|--------------------:|-------------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational Flat vs Polypheny Relational Normalized |     5/5 |  29,741.8 +/- 1,684.3 |   36,049.6 +/- 280.35 |            -6,307.8 |     [-8,384.8, -4,230.8] |  -8.260 (4.221) |   0.0009 |   0.0065 |     Yes     |
| Polypheny Relational Flat vs Polypheny Document MQL          |     5/5 |  29,741.8 +/- 1,684.3 | 142,440.8 +/- 7,815.9 |          -112,699.0 | [-122,303.1, -103,094.9] | -31.519 (4.371) | 2.53e-06 | 7.34e-05 |     Yes     |
| Polypheny Relational Flat vs DuckDB                          |     5/5 |  29,741.8 +/- 1,684.3 |    3,672.4 +/- 281.27 |            26,069.4 |     [23,992.5, 28,146.3] |  34.137 (4.223) | 2.55e-06 | 7.34e-05 |     Yes     |
| Polypheny Relational Flat vs Apache Spark                    |     5/5 |  29,741.8 +/- 1,684.3 |   21,738.2 +/- 305.76 |             8,003.6 |      [5,928.9, 10,078.3] |  10.455 (4.263) |   0.0003 |   0.0027 |     Yes     |
| Polypheny Relational Normalized vs Polypheny Document MQL    |     5/5 |   36,049.6 +/- 280.35 | 142,440.8 +/- 7,815.9 |          -106,391.2 |  [-116,092.4, -96,690.0] | -30.418 (4.010) | 6.79e-06 |   0.0002 |     Yes     |
| Polypheny Relational Normalized vs DuckDB                    |     5/5 |   36,049.6 +/- 280.35 |    3,672.4 +/- 281.27 |            32,377.2 |     [31,967.7, 32,786.7] | 182.304 (8.000) | 9.18e-16 | 4.59e-14 |     Yes     |
| Polypheny Relational Normalized vs Apache Spark              |     5/5 |   36,049.6 +/- 280.35 |   21,738.2 +/- 305.76 |            14,311.4 |     [13,883.0, 14,739.8] |  77.144 (7.941) | 1.05e-12 | 5.06e-11 |     Yes     |
| Polypheny Document MQL vs DuckDB                             |     5/5 | 142,440.8 +/- 7,815.9 |    3,672.4 +/- 281.27 |           138,768.4 |   [129,067.3, 148,469.5] |  39.675 (4.010) | 2.35e-06 | 7.04e-05 |     Yes     |
| Polypheny Document MQL vs Apache Spark                       |     5/5 | 142,440.8 +/- 7,815.9 |   21,738.2 +/- 305.76 |           120,702.6 |   [111,002.1, 130,403.1] |  34.506 (4.012) | 4.08e-06 |   0.0001 |     Yes     |
| DuckDB vs Apache Spark                                       |     5/5 |    3,672.4 +/- 281.27 |   21,738.2 +/- 305.76 |           -18,065.8 |   [-18,494.8, -17,636.8] | -97.234 (7.945) | 1.66e-13 | 8.12e-12 |     Yes     |

### Q02

| Comparison (A vs B)                                          | n (A/B) |     A mean +/- SD (ms) |     B mean +/- SD (ms) | Difference A-B (ms) |             95% CI (ms) |           t (df) |    Raw p |   Holm p | Significant |
|--------------------------------------------------------------|--------:|-----------------------:|-----------------------:|--------------------:|------------------------:|-----------------:|---------:|---------:|:-----------:|
| Polypheny Relational Flat vs Polypheny Relational Normalized |     5/5 |     9,650.6 +/- 395.42 |   10,555.8 +/- 1,123.7 |             -905.20 |      [-2,276.7, 466.33] |   -1.699 (4.976) |   0.1503 |   0.3950 |     No      |
| Polypheny Relational Flat vs Polypheny Document MQL          |     5/5 |     9,650.6 +/- 395.42 | 106,694.4 +/- 10,386.4 |           -97,043.8 | [-109,934.9, -84,152.7] |  -20.877 (4.012) | 3.04e-05 |   0.0006 |     Yes     |
| Polypheny Relational Flat vs DuckDB                          |     5/5 |     9,650.6 +/- 395.42 |       218.80 +/- 12.66 |             9,431.8 |      [8,941.0, 9,922.6] |   53.308 (4.008) | 7.24e-07 | 2.39e-05 |     Yes     |
| Polypheny Relational Flat vs Apache Spark                    |     5/5 |     9,650.6 +/- 395.42 |    13,667.6 +/- 247.62 |            -4,017.0 |    [-4,514.6, -3,519.4] |  -19.252 (6.719) | 3.96e-07 | 1.35e-05 |     Yes     |
| Polypheny Relational Normalized vs Polypheny Document MQL    |     5/5 |   10,555.8 +/- 1,123.7 | 106,694.4 +/- 10,386.4 |           -96,138.6 | [-108,994.2, -83,283.0] |  -20.577 (4.094) | 2.75e-05 |   0.0005 |     Yes     |
| Polypheny Relational Normalized vs DuckDB                    |     5/5 |   10,555.8 +/- 1,123.7 |       218.80 +/- 12.66 |            10,337.0 |     [8,941.7, 11,732.3] |   20.568 (4.001) | 3.29e-05 |   0.0006 |     Yes     |
| Polypheny Relational Normalized vs Apache Spark              |     5/5 |   10,555.8 +/- 1,123.7 |    13,667.6 +/- 247.62 |            -3,111.8 |    [-4,492.2, -1,731.4] |   -6.047 (4.388) |   0.0028 |   0.0139 |     Yes     |
| Polypheny Document MQL vs DuckDB                             |     5/5 | 106,694.4 +/- 10,386.4 |       218.80 +/- 12.66 |           106,475.6 |   [93,579.1, 119,372.1] |   22.923 (4.000) | 2.15e-05 |   0.0005 |     Yes     |
| Polypheny Document MQL vs Apache Spark                       |     5/5 | 106,694.4 +/- 10,386.4 |    13,667.6 +/- 247.62 |            93,026.8 |   [80,132.4, 105,921.2] |   20.022 (4.005) | 3.64e-05 |   0.0006 |     Yes     |
| DuckDB vs Apache Spark                                       |     5/5 |       218.80 +/- 12.66 |    13,667.6 +/- 247.62 |           -13,448.8 |  [-13,756.0, -13,141.6] | -121.288 (4.021) | 2.56e-08 | 1.05e-06 |     Yes     |

### Q03

| Comparison (A vs B)                                          | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) |         95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|--------------------------------------------------------------|--------:|-------------------:|-------------------:|--------------------:|--------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational Flat vs Polypheny Relational Normalized |     5/5 |   157.40 +/- 13.32 |   170.80 +/- 11.84 |              -13.40 |     [-31.82, 5.020] |  -1.682 (7.892) |   0.1317 |   0.3950 |     No      |
| Polypheny Relational Flat vs Polypheny Document MQL          |     5/5 |   157.40 +/- 13.32 |   424.80 +/- 23.72 |             -267.40 |  [-296.84, -237.96] | -21.980 (6.293) | 3.45e-07 | 1.21e-05 |     Yes     |
| Polypheny Relational Flat vs DuckDB                          |     5/5 |   157.40 +/- 13.32 |    35.40 +/- 2.302 |              122.00 |    [105.59, 138.41] |  20.188 (4.239) | 2.25e-05 |   0.0005 |     Yes     |
| Polypheny Relational Flat vs Apache Spark                    |     5/5 |   157.40 +/- 13.32 | 1,182.4 +/- 142.80 |            -1,025.0 | [-1,201.9, -848.11] | -15.981 (4.070) | 7.97e-05 |   0.0010 |     Yes     |
| Polypheny Relational Normalized vs Polypheny Document MQL    |     5/5 |   170.80 +/- 11.84 |   424.80 +/- 23.72 |             -254.00 |  [-283.16, -224.84] | -21.423 (5.877) | 8.39e-07 | 2.68e-05 |     Yes     |
| Polypheny Relational Normalized vs DuckDB                    |     5/5 |   170.80 +/- 11.84 |    35.40 +/- 2.302 |              135.40 |    [120.83, 149.97] |  25.100 (4.302) | 7.88e-06 |   0.0002 |     Yes     |
| Polypheny Relational Normalized vs Apache Spark              |     5/5 |   170.80 +/- 11.84 | 1,182.4 +/- 142.80 |            -1,011.6 | [-1,188.6, -834.63] | -15.786 (4.055) | 8.58e-05 |   0.0010 |     Yes     |
| Polypheny Document MQL vs DuckDB                             |     5/5 |   424.80 +/- 23.72 |    35.40 +/- 2.302 |              389.40 |    [360.02, 418.78] |  36.535 (4.075) | 2.77e-06 | 7.49e-05 |     Yes     |
| Polypheny Document MQL vs Apache Spark                       |     5/5 |   424.80 +/- 23.72 | 1,182.4 +/- 142.80 |             -757.60 |  [-933.70, -581.50] | -11.703 (4.221) |   0.0002 |   0.0020 |     Yes     |
| DuckDB vs Apache Spark                                       |     5/5 |    35.40 +/- 2.302 | 1,182.4 +/- 142.80 |            -1,147.0 | [-1,324.3, -969.70] | -17.958 (4.002) | 5.63e-05 |   0.0009 |     Yes     |

### Q04

| Comparison (A vs B)                                          | n (A/B) |  A mean +/- SD (ms) |  B mean +/- SD (ms) | Difference A-B (ms) |          95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|--------------------------------------------------------------|--------:|--------------------:|--------------------:|--------------------:|---------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational Flat vs Polypheny Relational Normalized |     5/5 |  6,073.4 +/- 172.93 |  7,594.4 +/- 341.79 |            -1,521.0 | [-1,941.5, -1,100.5] |  -8.879 (5.922) |   0.0001 |   0.0012 |     Yes     |
| Polypheny Relational Flat vs Polypheny Document MQL          |     5/5 |  6,073.4 +/- 172.93 | 14,752.6 +/- 405.70 |            -8,679.2 | [-9,174.9, -8,183.5] | -44.005 (5.407) | 4.05e-08 | 1.62e-06 |     Yes     |
| Polypheny Relational Flat vs DuckDB                          |     5/5 |  6,073.4 +/- 172.93 |    480.00 +/- 42.63 |             5,593.4 |   [5,381.4, 5,805.4] |  70.224 (4.484) | 5.40e-08 | 2.10e-06 |     Yes     |
| Polypheny Relational Flat vs Apache Spark                    |     5/5 |  6,073.4 +/- 172.93 |  2,666.6 +/- 300.58 |             3,406.8 |   [3,032.8, 3,780.8] |  21.968 (6.386) | 2.94e-07 | 1.06e-05 |     Yes     |
| Polypheny Relational Normalized vs Polypheny Document MQL    |     5/5 |  7,594.4 +/- 341.79 | 14,752.6 +/- 405.70 |            -7,158.2 | [-7,708.0, -6,608.4] | -30.173 (7.776) | 2.44e-09 | 1.05e-07 |     Yes     |
| Polypheny Relational Normalized vs DuckDB                    |     5/5 |  7,594.4 +/- 341.79 |    480.00 +/- 42.63 |             7,114.4 |   [6,691.8, 7,537.0] |  46.186 (4.124) | 9.35e-07 | 2.90e-05 |     Yes     |
| Polypheny Relational Normalized vs Apache Spark              |     5/5 |  7,594.4 +/- 341.79 |  2,666.6 +/- 300.58 |             4,927.8 |   [4,457.1, 5,398.5] |  24.209 (7.871) | 1.13e-08 | 4.74e-07 |     Yes     |
| Polypheny Document MQL vs DuckDB                             |     5/5 | 14,752.6 +/- 405.70 |    480.00 +/- 42.63 |            14,272.6 | [13,770.4, 14,774.8] |  78.234 (4.088) | 1.20e-07 | 4.43e-06 |     Yes     |
| Polypheny Document MQL vs Apache Spark                       |     5/5 | 14,752.6 +/- 405.70 |  2,666.6 +/- 300.58 |            12,086.0 | [11,557.5, 12,614.5] |  53.523 (7.375) | 7.98e-11 | 3.51e-09 |     Yes     |
| DuckDB vs Apache Spark                                       |     5/5 |    480.00 +/- 42.63 |  2,666.6 +/- 300.58 |            -2,186.6 | [-2,557.9, -1,815.3] | -16.105 (4.161) | 6.62e-05 |   0.0009 |     Yes     |

### Q05

| Comparison (A vs B)                                          | n (A/B) |  A mean +/- SD (ms) |  B mean +/- SD (ms) | Difference A-B (ms) |           95% CI (ms) |          t (df) |    Raw p |   Holm p | Significant |
|--------------------------------------------------------------|--------:|--------------------:|--------------------:|--------------------:|----------------------:|----------------:|---------:|---------:|:-----------:|
| Polypheny Relational Flat vs Polypheny Relational Normalized |     5/5 |  2,528.8 +/- 181.63 |  2,303.0 +/- 286.00 |              225.80 |     [-134.90, 586.50] |   1.490 (6.775) |   0.1812 |   0.3950 |     No      |
| Polypheny Relational Flat vs Polypheny Document MQL          |     5/5 |  2,528.8 +/- 181.63 | 12,683.2 +/- 274.62 |           -10,154.4 | [-10,503.2, -9,805.6] | -68.963 (6.937) | 4.23e-11 | 1.90e-09 |     Yes     |
| Polypheny Relational Flat vs DuckDB                          |     5/5 |  2,528.8 +/- 181.63 |    115.20 +/- 3.962 |             2,413.6 |    [2,188.1, 2,639.1] |  29.707 (4.004) | 7.58e-06 |   0.0002 |     Yes     |
| Polypheny Relational Flat vs Apache Spark                    |     5/5 |  2,528.8 +/- 181.63 |  1,941.4 +/- 224.30 |              587.40 |      [287.50, 887.30] |   4.551 (7.668) |   0.0021 |   0.0125 |     Yes     |
| Polypheny Relational Normalized vs Polypheny Document MQL    |     5/5 |  2,303.0 +/- 286.00 | 12,683.2 +/- 274.62 |           -10,380.2 | [-10,789.2, -9,971.2] | -58.540 (7.987) | 8.33e-12 | 3.83e-10 |     Yes     |
| Polypheny Relational Normalized vs DuckDB                    |     5/5 |  2,303.0 +/- 286.00 |    115.20 +/- 3.962 |             2,187.8 |    [1,832.7, 2,542.9] |  17.104 (4.002) | 6.84e-05 |   0.0009 |     Yes     |
| Polypheny Relational Normalized vs Apache Spark              |     5/5 |  2,303.0 +/- 286.00 |  1,941.4 +/- 224.30 |              361.60 |      [-16.97, 740.17] |   2.225 (7.570) |   0.0586 |   0.2345 |     No      |
| Polypheny Document MQL vs DuckDB                             |     5/5 | 12,683.2 +/- 274.62 |    115.20 +/- 3.962 |            12,568.0 |  [12,227.0, 12,909.0] | 102.323 (4.002) | 5.44e-08 | 2.10e-06 |     Yes     |
| Polypheny Document MQL vs Apache Spark                       |     5/5 | 12,683.2 +/- 274.62 |  1,941.4 +/- 224.30 |            10,741.8 |  [10,373.6, 11,110.0] |  67.740 (7.693) | 5.83e-12 | 2.74e-10 |     Yes     |
| DuckDB vs Apache Spark                                       |     5/5 |    115.20 +/- 3.962 |  1,941.4 +/- 224.30 |            -1,826.2 |  [-2,104.7, -1,547.7] | -18.203 (4.002) | 5.33e-05 |   0.0009 |     Yes     |
