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
  -Queries plugins\parquet-adapter\benchmarks\query_lists\arrregation\aggregation_polypheny.sql `
  -Output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_results.csv `
  -Warmups 1 `
  -Runs 5 `
  -NoTableNameMapping
```

PD

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_polypheny_mql_benchmark.ps1 `
  -Namespace tlcpd_document `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\arrregation\aggregation_polypheny_mql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_results.csv `
  -Warmups 1 `
  -Runs 5
```

DuckDB

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_duckdb_benchmark.ps1 `
  -DataDir C:\PolyData\tlc_partitioned `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\arrregation\aggregation_sql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_results.csv `
  -Warmups 1 `
  -Runs 5
```

Spark

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_spark_benchmark.ps1 `
  -DataDir C:\PolyData\tlc_partitioned `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\arrregation\aggregation_sql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_results.csv `
  -Warmups 1 `
  -Runs 5
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

## Create plot

```powershell
python plugins\parquet-adapter\benchmarks\scripts\plot_generation\benchmark_result_plot_generator.py `
  --title "Aggregation" `
  --name aggregation_plot `
  "Polypheny Relational=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_results.csv" `
  "Polypheny Document MQL=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_polypheny_mql_results.csv" `
  "DuckDB=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_duckdb_results.csv" `
  "Apache Spark=plugins\parquet-adapter\benchmarks\results\aggregation\aggregation_spark_results.csv"
```
