# Welch’s test

Welch’s t-test compares the mean runtimes of two systems without assuming that their runtime variances are equal.

Its null hypothesis is:
- The two systems have the same mean runtime.

Interpretation:
- Holm-adjusted p ≤ 0.05: the measured difference is statistically significant.
- Holm-adjusted p > 0.05: the five runs do not provide enough evidence of a difference.
- A non-significant result does not prove that performance is equal.
- Statistical significance does not necessarily mean the difference is practically important.

Because only five measurements are available per group and execution order was not randomized, the results are explicitly described as exploratory. Caching, time trends, or other order effects may influence the comparisons.


## benchmark_welch_analysis.py
`plugins/parquet-adapter/benchmarks/scripts/benchmark_welch_analysis.py`

- Reads the raw benchmark CSV files.

- Selects only successful measured runs, excluding warm-ups and failures.

- Compares systems pairwise for each query using their raw elapsed_ms values.

- Calculates:
  - number of runs;
  - mean and sample standard deviation;
  - mean difference, calculated as system A minus system B;
  - unadjusted 95% confidence interval;
  - Welch t-statistic and degrees of freedom;
  - raw p-value;
  - Holm-adjusted p-value;
  - statistical-significance status.

- Applies Holm correction across all comparisons in a benchmark, reducing the risk of false significant results caused by testing many pairs.

- Generates a Markdown report for each benchmark
  - benchmarks/results/access_model_comparison/access_model_comparison_welch_analysis.md
  - benchmarks/results/aggregation/aggregation_welch_analysis.md
  - benchmarks/results/nested_data/nested_data_welch_analysis.md
  - benchmarks/results/partitioning/partitioning_welch_analysis.md

- For partitioning, additional explicit comparisons test partitioned versus unpartitioned measurements within each system.

The standard-deviation plots and Welch reports answer different questions:
- standard-deviation whiskers show runtime variability.
- Welch tests evaluate whether an observed difference between two mean runtimes is statistically supported.


Holm correction is used to reduce the risk of false-positive results when many statistical comparisons are performed.
Holm correction:
- Sorts all raw p-values from smallest to largest.
- Adjusts them according to the number of comparisons.
- Produces the stricter Holm p values.
- Uses Holm p ≤ 0.05 to determine significance.
