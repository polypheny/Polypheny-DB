# Aggregation benchmark pipeline

## Dataset

The pipeline uses the partitioned TLC dataset:

```text
C:\PolyData\tlc_partitioned
```

The current aggregation query files run Q01-Q05 on `yellow_tripdata` and
Q06-Q10 on `fhvhv_tripdata`.

## Run benchmark

```powershell
cd C:\github\Polypheny-DB
```

Build jar:

```powershell
.\gradlew.bat :dbms:shadowJar --no-daemon
```

Run jar:

```powershell
java -Xms8g -Xmx16g "-Dfile.encoding=UTF-8" -jar .\dbms\build\libs\dbms-0.10.1-SNAPSHOT.jar
```

The Polypheny relational run assumes a Parquet relational adapter named `tlcp`,
which exposes `tlcp__yellow_tripdata` and `tlcp__fhvhv_tripdata`.

The Polypheny document run assumes a Parquet document adapter named `tlcpd`,
exposed through namespace `tlcpd_document`.

PR

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_polypheny_benchmark.ps1 `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\aggregation\aggregation_polypheny.sql `
  -Output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_results.csv `
  -ResultValuesOutput plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_values.jsonl `
  -Warmups 1 `
  -Runs 5 `
  -NoTableNameMapping
```

PD

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_polypheny_mql_benchmark.ps1 `
  -Namespace tlcpd_document `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\aggregation\aggregation_polypheny_mql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_results.csv `
  -ResultValuesOutput plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_values.jsonl `
  -Warmups 1 `
  -Runs 5
```

DuckDB

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_duckdb_benchmark.ps1 `
  -DataDir C:\PolyData\tlc_partitioned `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\aggregation\aggregation_sql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_results.csv `
  -ResultValuesOutput plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_values.jsonl `
  -Warmups 1 `
  -Runs 5
```

Spark

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_spark_benchmark.ps1 `
  -DataDir C:\PolyData\tlc_partitioned `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\aggregation\aggregation_sql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_results.csv `
  -ResultValuesOutput plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_values.jsonl `
  -Warmups 1 `
  -Runs 5
```

## Create correctness summary

```powershell
python plugins\parquet-adapter\benchmarks\scripts\compare_aggregation_results.py `
  --title "Aggregation Correctness Summary" `
  --output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_correctness_summary.md `
  --reference DuckDB `
  "Polypheny Relational=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_values.jsonl" `
  "Polypheny Document MQL=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_values.jsonl" `
  "DuckDB=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_values.jsonl" `
  "Apache Spark=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_values.jsonl"
```

## Create summary

```powershell
python plugins\parquet-adapter\benchmarks\scripts\summarize_benchmark_results.py `
  --title "Aggregation Summary" `
  --output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_summary.md `
  "Polypheny Relational=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_results.csv" `
  "Polypheny Document MQL=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_results.csv" `
  "DuckDB=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_results.csv" `
  "Apache Spark=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_results.csv"
```

## Create exploratory Welch analysis

```powershell
python plugins\parquet-adapter\benchmarks\scripts\benchmark_welch_analysis.py `
  --title "Aggregation - Exploratory Welch Analysis" `
  --output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_welch_analysis.md `
  "Polypheny Relational=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_results.csv" `
  "Polypheny Document MQL=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_results.csv" `
  "DuckDB=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_results.csv" `
  "Apache Spark=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_results.csv"
```

## Create plot

```powershell
python plugins\parquet-adapter\benchmarks\scripts\plot_generation\benchmark_result_plot_generator.py `
  --title "Aggregation on Partitioned TLC Data" `
  --name aggregation_plot `
  --query-order "Q01,Q02,Q03,Q04,Q05,Q06,Q07,Q08,Q09,Q10" `
  --query-descriptions "Q01=Yellow total row count;Q02=Yellow one-month count;Q03=Yellow one-day count;Q04=Yellow filtered row count;Q05=Yellow summary by year;Q06=FHV total row count;Q07=FHV one-month count;Q08=FHV filtered row count;Q09=FHV summary by year;Q10=FHV count by shared-request flag" `
  --query-description-wrap-chars 14 `
  --query-description-max-lines 3 `
  "Polypheny Relational=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_results.csv" `
  "Polypheny Document MQL=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_results.csv" `
  "DuckDB=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_results.csv" `
  "Apache Spark=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_results.csv"
```

## Create plot with standard-deviation whiskers

This creates additional `_std` files and leaves the mean-only plot files unchanged.

```powershell
python plugins\parquet-adapter\benchmarks\scripts\plot_generation\benchmark_result_std_plot_generator.py `
  --title "Aggregation on Partitioned TLC Data" `
  --name aggregation_plot_std `
  --query-order "Q01,Q02,Q03,Q04,Q05,Q06,Q07,Q08,Q09,Q10" `
  --query-descriptions "Q01=Yellow total row count;Q02=Yellow one-month count;Q03=Yellow one-day count;Q04=Yellow filtered row count;Q05=Yellow summary by year;Q06=FHV total row count;Q07=FHV one-month count;Q08=FHV filtered row count;Q09=FHV summary by year;Q10=FHV count by shared-request flag" `
  --query-description-wrap-chars 14 `
  --query-description-max-lines 3 `
  "Polypheny Relational=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_results.csv" `
  "Polypheny Document MQL=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_results.csv" `
  "DuckDB=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_results.csv" `
  "Apache Spark=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_results.csv"
```
