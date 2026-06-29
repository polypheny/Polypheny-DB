# Parquet Adapter Benchmarks

`plugins/parquet-adapter/benchmarks` folder contains benchmark definitions, execution pipelines, runner code,
and result artifacts for the Polypheny Parquet adapter comparison.

## Folder Structure and Files

| Path                       | Contents                                                                                               |
|----------------------------|--------------------------------------------------------------------------------------------------------|
| `benchmark-plan.md`        | Benchmark methodology and suite overview: goals, datasets, query files, result locations, interpretation files, and pipeline links. |
| `technology-comparison.md` | Technical comparison of Polypheny, DuckDB, and Apache Spark for Parquet workloads.                     |
| `datasets/`                | Dataset descriptions.                                                                                  |
| `query_lists/`             | Runnable SQL/MQL query files and query specifications.                                                 |
| `run_pipeline/`            | Step-by-step benchmark execution commands. Use these files to run each full suite pipeline.            |
| `scripts/`                 | Benchmark clients, PowerShell runners, preprocessing scripts, summary generation, and plot generation. |
| `results/`                 | Raw CSV/JSONL results, generated statistical and correctness summaries, manual result interpretations, and plots. |

## Query Lists

| Path                                   | Meaning                                                                                                                                            |
|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `query_lists/data_configuration.md`    | Local data paths and Polypheny adapter names used by the query files.                                                                              |
| `query_lists/access_model_comparison/` | Access-model comparison benchmark queries for Polypheny relational flat, Polypheny relational normalized, Polypheny MQL, DuckDB, and Spark.        |
| `query_lists/nested_data/`             | Nested Customer benchmark queries. Engine-specific files are used because nested access syntax differs.                                            |
| `query_lists/aggregation/`             | Aggregation benchmark query files for Polypheny relational, Polypheny MQL, DuckDB, and Spark.                                                       |
| `query_lists/partitioning/`            | Partitioning benchmark queries for repartitioned and unpartitioned TLC layouts.                                                                    |

File naming:

- `*_rf.sql`: Polypheny relational flat SQL with explicit adapter-prefixed table names.
- `*_rn.sql`: Polypheny relational normalized SQL with explicit adapter-prefixed table names.
- `*_polypheny.sql` and `*_polypheny_normalized.sql`: other Polypheny relational SQL with explicit adapter-prefixed table names.
- `*_polypheny_mql.sql` and `*.mql`: Polypheny document adapter queries written in MQL.
- `*_sql.sql`: SQL shared by DuckDB and Spark where both expose the same logical temp views.
- `*_query_specification.md`: logical query intent, query groups, and textual query descriptions.

## Run Pipelines

Run suite pipelines from:

```text
run_pipeline/
```

| Pipeline file                                | Suite                   |
|----------------------------------------------|-------------------------|
| `run_access_model_comparison_bm_pipeline.md` | Access model comparison |
| `run_nested_data_bm_pipeline.md`             | Nested data             |
| `run_aggregation_bm_pipeline.md`             | Aggregation             |
| `run_partitioning_bm_pipeline.md`            | Partitioning            |

Each pipeline file contains the commands to build/start Polypheny where needed,
run the benchmark clients, and generate the suite summary and plot. The
aggregation pipeline additionally captures result values and generates a
correctness comparison. Result interpretation documents are updated manually
after the generated artifacts have been reviewed.

## Scripts

| Path                                     | Purpose                                                                      |
|------------------------------------------|------------------------------------------------------------------------------|
| `scripts/runners/`                       | PowerShell entry points for Polypheny SQL, Polypheny MQL, DuckDB, and Spark. |
| `scripts/runners/nested/`                | Nested-data-specific runner wrappers.                                        |
| `scripts/implementation/`                | Java/Python benchmark clients called by the runners.                         |
| `scripts/ds_preprocessing/`              | Dataset materialization and preprocessing utilities.                         |
| `scripts/summarize_benchmark_results.py` | Creates markdown summaries with per-query runtime statistics from benchmark CSV files. |
| `scripts/compare_aggregation_results.py` | Compares captured aggregation result values across systems.                  |
| `scripts/plot_generation/`               | Plot generation scripts, including the generic SVG/PDF/PNG benchmark CSV plot generator. |

PNG plot output requires Pillow. SVG and PDF output use only the Python
standard library.

### Plot Color Convention

Benchmark plots use a fixed color convention so that the same system or access
path has the same color across all result figures.

| System or access path | Color |
|-----------------------|-------|
| Polypheny Relational Flat | Green (`#4f9d55`) |
| Polypheny Relational Normalized | Red (`#c94c4c`) |
| Polypheny Document | Purple (`#7b61b9`) |
| DuckDB | Orange (`#f0a22e`) |
| Apache Spark | Blue (`#2f6fbb`) |

## Results

Raw benchmark outputs are stored as CSV files under:

```text
results/
```

Result conventions:

- `*_results.csv`: raw benchmark output from a runner.
- `*_values.jsonl`: captured query result values used for correctness checks where applicable.
- `*_summary.md`: generated comparison summary from `scripts/summarize_benchmark_results.py`.
- `*_correctness_summary.md`: generated comparison of captured result values where applicable.
- `*_result_analysis.md`: manually maintained result interpretation combining the generated plot, findings, and comparability notes.
- `results/plots/`: generated plot artifacts such as SVG, PDF, and PNG files.

### Artifact Workflow

1. Benchmark runners write raw timing results to CSV files and, where required,
   captured result values to JSONL files.
2. Summary scripts calculate per-query mean, median, sample standard deviation,
   minimum, and maximum runtimes. The aggregation comparison also checks
   captured grouping keys and aggregate values.
3. The plot generator creates SVG, PDF, and PNG figures from the raw timing
   results.
4. The corresponding `*_result_analysis.md` file is updated manually with the
   interpretation of the summary and plot.

Current result folders:

| Path                               | Contents                                                                    |
|------------------------------------|-----------------------------------------------------------------------------|
| `results/access_model_comparison/` | Access-model raw CSVs, generated summary, and manual result interpretation. |
| `results/aggregation/`             | Aggregation raw CSV/JSONL files, timing and correctness summaries, and manual result interpretation. |
| `results/nested_data/`             | Nested-data raw CSVs, generated summary, and manual result interpretation.  |
| `results/partitioning/`            | Partitioning raw CSVs, generated summary, and manual result interpretation. |
| `results/plots/`                   | Generated SVG, PDF, and PNG benchmark plots.                                |
