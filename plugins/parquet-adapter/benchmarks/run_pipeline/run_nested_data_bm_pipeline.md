# Nested data benchmark pipeline

## Dataset

The pipeline uses the Nested Customer Parquet file:

```text
C:\PolyData\nested_customer\nestedcustomer.parquet
```

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

Polypheny relational normalized mode assumes a Parquet relational adapter named
`ncp`, as defined in `benchmarks\query_lists\data_configuration.md`. The document run assumes a Parquet document adapter named
`ncpd`, exposed through namespace `ncpd_document`. If the nested document adapter is
too heavy to load, skip the PD command and omit it from the summary command.

PR normalized

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\nested\run_polypheny_nested_benchmark.ps1 `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\nested_data\nested_data_polypheny_normalized.sql `
  -Output plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_polypheny_normalized_results.csv `
  -Warmups 1 `
  -Runs 5
```

PD

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\nested\run_polypheny_mql_nested_benchmark.ps1 `
  -Namespace ncpd_document `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\nested_data\nested_data_mql.mql `
  -Output plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_polypheny_mql_results.csv `
  -Warmups 1 `
  -Runs 5
```

DuckDB

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\nested\run_duckdb_nested_benchmark.ps1 `
  -DataFile C:\PolyData\nested_customer\nestedcustomer.parquet `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\nested_data\nested_data_duckdb.sql `
  -Output plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_duckdb_results.csv `
  -Warmups 1 `
  -Runs 5
```

Spark

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\nested\run_spark_nested_benchmark.ps1 `
  -DataFile C:\PolyData\nested_customer\nestedcustomer.parquet `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\nested_data\nested_data_spark.sql `
  -Output plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_spark_results.csv `
  -Warmups 1 `
  -Runs 5
```

## Create summary

```powershell
python plugins\parquet-adapter\benchmarks\scripts\summarize_benchmark_results.py `
  --title "Nested Data Summary" `
  --output plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_summary.md `
  "Polypheny Relational Normalized=plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_polypheny_normalized_results.csv" `
  "Polypheny Document MQL=plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_polypheny_mql_results.csv" `
  "DuckDB=plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_duckdb_results.csv" `
  "Apache Spark=plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_spark_results.csv"
```

## Create plot

```powershell
python plugins\parquet-adapter\benchmarks\scripts\plot_generation\benchmark_result_plot_generator.py `
  --title "Nested Data" `
  --name nested_data_plot `
  "Polypheny Relational Normalized=plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_polypheny_normalized_results.csv" `
  "Polypheny Document MQL=plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_polypheny_mql_results.csv" `
  "DuckDB=plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_duckdb_results.csv" `
  "Apache Spark=plugins\parquet-adapter\benchmarks\results\nested_data\nested_data_spark_results.csv"
```
