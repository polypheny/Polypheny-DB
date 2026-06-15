# Parquet Adapter Benchmarks

`plugins/parquet-adapter/benchmarks` folder contains benchmark definitions, execution pipelines, runner code,
and result artifacts for the Polypheny Parquet adapter comparison.

## Main Files

| Path                          | Purpose                                                                                       |
|-------------------------------|-----------------------------------------------------------------------------------------------|
| `benchmark-plan.md`           | Benchmark suite overview: goals, datasets, query files, result locations, and pipeline links. |
| `compatitive-technologies.md` | Technical comparison of Polypheny, DuckDB, and Apache Spark for Parquet workloads.            |
| `README.md`                   | Folder structure and file map.                                                                |

## Folder Structure

| Folder          | Contents                                                                                               |
|-----------------|--------------------------------------------------------------------------------------------------------|
| `datasets/`     | Dataset descriptions.                                                                                  |
| `query_lists/`  | Runnable SQL/MQL query files and query specifications.                                                 |
| `run_pipeline/` | Step-by-step benchmark execution commands. Use these files to run each full suite pipeline.            |
| `scripts/`      | Benchmark clients, PowerShell runners, preprocessing scripts, summary generation, and plot generation. |
| `results/`      | Raw CSV results, generated summaries, short analysis notes, and plots.                                 |

## Query Lists

| Path                                   | Meaning                                                                                                                                            |
|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `query_lists/data_configuration.md`    | Local data paths and Polypheny adapter names used by the query files.                                                                              |
| `query_lists/access_model_comparison/` | Access-model comparison benchmark queries for Polypheny relational flat, Polypheny relational normalized, Polypheny MQL, DuckDB, and Spark.        |
| `query_lists/nested_data/`             | Nested Customer benchmark queries. Engine-specific files are used because nested access syntax differs.                                            |
| `query_lists/arrregation/`             | Aggregation benchmark query files for Polypheny relational, Polypheny MQL, DuckDB, and Spark. The directory name is kept as-is for existing paths. | 
| `query_lists/partitioning/`            | Partitioning benchmark queries for repartitioned and unpartitioned TLC layouts.                                                                    |

File naming:

- `*_rf.sql`: Polypheny relational flat SQL with explicit adapter-prefixed table names.
- `*_rn.sql`: Polypheny relational normalized SQL with explicit adapter-prefixed table names.
- `*_polypheny*.sql`: other Polypheny relational SQL with explicit adapter-prefixed table names.
- `*_sql.sql`: SQL shared by DuckDB and Spark where both expose the same logical temp views.
- `*.mql`: Polypheny document adapter queries.
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
run the benchmark clients, and generate the suite summary and plot.

## Scripts

| Path                                     | Purpose                                                                      |
|------------------------------------------|------------------------------------------------------------------------------|
| `scripts/runners/`                       | PowerShell entry points for Polypheny SQL, Polypheny MQL, DuckDB, and Spark. |
| `scripts/runners/nested/`                | Nested-data-specific runner wrappers.                                        |
| `scripts/implementation/`                | Java/Python benchmark clients called by the runners.                         |
| `scripts/ds_preprocessing/`              | Dataset materialization and preprocessing utilities.                         |
| `scripts/summarize_benchmark_results.py` | Creates markdown summaries from benchmark CSV files.                         |
| `scripts/plot_generation/`               | Plot generation scripts, including the generic SVG/PDF/PNG benchmark CSV plot generator. |

PNG plot output requires Pillow. SVG and PDF output use only the Python
standard library.

## Results

Raw benchmark outputs are stored as CSV files under:

```text
results/
```

Result conventions:

- `*_results.csv`: raw benchmark output from a runner.
- `*_summary.md`: generated comparison summary from `scripts/summarize_benchmark_results.py`.
- `*_results_analysis.md`: short manual interpretation of a generated summary.
- `results/plots/`: generated plot artifacts such as SVG, PDF, and PNG files.

Current result folders:

| Path                               | Contents                                      |
|------------------------------------|-----------------------------------------------|
| `results/access_model_comparison/` | Access-model raw CSVs, summary, and analysis. |
| `results/aggregation/`             | Aggregation raw CSVs, summary, and analysis.  |
| `results/nested_data/`             | Nested-data raw CSVs.                         |
| `results/plots/`                   | Generated benchmark plots.                    |
