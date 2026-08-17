# Partitioning benchmark pipeline

## Dataset

The pipeline compares two TLC layouts:

```text
C:\PolyData\tlc_repartitioned
C:\PolyData\tlc_unpartitioned
```

The Polypheny run assumes two Parquet relational adapters:

- `tlcr` for repartitioned data;
- `tlcu` for unpartitioned data.

DuckDB and Spark bind one data directory per run, so they are executed once for
the repartitioned layout and once for the unpartitioned layout.

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

PR

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_polypheny_benchmark.ps1 `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\partitioning\partitioning_repartitioned_unpartitioned_polypheny.sql `
  -Output plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_polypheny_results.csv `
  -Warmups 1 `
  -Runs 5 `
  -NoTableNameMapping
```

DuckDB repartitioned

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_duckdb_benchmark.ps1 `
  -DataDir C:\PolyData\tlc_repartitioned `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\partitioning\partitioning_repartitioned_sql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_repartitioned_results.csv `
  -Warmups 1 `
  -Runs 5
```

DuckDB unpartitioned

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_duckdb_benchmark.ps1 `
  -DataDir C:\PolyData\tlc_unpartitioned `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\partitioning\partitioning_unpartitioned_sql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_unpartitioned_results.csv `
  -Warmups 1 `
  -Runs 5
```

Spark repartitioned

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_spark_benchmark.ps1 `
  -DataDir C:\PolyData\tlc_repartitioned `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\partitioning\partitioning_repartitioned_sql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_repartitioned_results.csv `
  -Warmups 1 `
  -Runs 5
```

Spark unpartitioned

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_spark_benchmark.ps1 `
  -DataDir C:\PolyData\tlc_unpartitioned `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\partitioning\partitioning_unpartitioned_sql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_unpartitioned_results.csv `
  -Warmups 1 `
  -Runs 5
```

## Create summary

```powershell
python plugins\parquet-adapter\benchmarks\scripts\summarize_benchmark_results.py `
  --title "Partitioning Summary" `
  --output plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_summary.md `
  "Polypheny Relational=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_polypheny_results.csv" `
  "DuckDB Repartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_repartitioned_results.csv" `
  "DuckDB Unpartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_unpartitioned_results.csv" `
  "Apache Spark Repartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_repartitioned_results.csv" `
  "Apache Spark Unpartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_unpartitioned_results.csv"
```

## Create exploratory Welch analysis

The explicit comparisons evaluate the partitioned and unpartitioned variants of each logical query within each system.

```powershell
python plugins\parquet-adapter\benchmarks\scripts\benchmark_welch_analysis.py `
  --title "Partitioning - Exploratory Welch Analysis" `
  --output plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_welch_analysis.md `
  --comparison "Polypheny Relational@Q1_P=Polypheny Relational@Q1_NP" `
  --comparison "DuckDB Repartitioned@Q1_P=DuckDB Unpartitioned@Q1_NP" `
  --comparison "Apache Spark Repartitioned@Q1_P=Apache Spark Unpartitioned@Q1_NP" `
  --comparison "Polypheny Relational@Q2_P=Polypheny Relational@Q2_NP" `
  --comparison "DuckDB Repartitioned@Q2_P=DuckDB Unpartitioned@Q2_NP" `
  --comparison "Apache Spark Repartitioned@Q2_P=Apache Spark Unpartitioned@Q2_NP" `
  --comparison "Polypheny Relational@Q3_P=Polypheny Relational@Q3_NP" `
  --comparison "DuckDB Repartitioned@Q3_P=DuckDB Unpartitioned@Q3_NP" `
  --comparison "Apache Spark Repartitioned@Q3_P=Apache Spark Unpartitioned@Q3_NP" `
  --comparison "Polypheny Relational@Q4_P=Polypheny Relational@Q4_NP" `
  --comparison "DuckDB Repartitioned@Q4_P=DuckDB Unpartitioned@Q4_NP" `
  --comparison "Apache Spark Repartitioned@Q4_P=Apache Spark Unpartitioned@Q4_NP" `
  --comparison "Polypheny Relational@Q5_P=Polypheny Relational@Q5_NP" `
  --comparison "DuckDB Repartitioned@Q5_P=DuckDB Unpartitioned@Q5_NP" `
  --comparison "Apache Spark Repartitioned@Q5_P=Apache Spark Unpartitioned@Q5_NP" `
  "Polypheny Relational=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_polypheny_results.csv" `
  "DuckDB Repartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_repartitioned_results.csv" `
  "DuckDB Unpartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_unpartitioned_results.csv" `
  "Apache Spark Repartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_repartitioned_results.csv" `
  "Apache Spark Unpartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_unpartitioned_results.csv"
```

## Create plot

```powershell
python plugins\parquet-adapter\benchmarks\scripts\plot_generation\benchmark_result_plot_generator.py `
  --title "Partitioning by Year and Month" `
  --name partitioning_plot `
  --query-order "Q1_P,Q1_NP,Q2_P,Q2_NP,Q3_P,Q3_NP,Q4_P,Q4_NP,Q5_P,Q5_NP" `
  --query-descriptions "Q1=Full-count baseline;Q2=Year-filtered count;Q3=Year-and-month-filtered count;Q4=Data-column filtering, no partition restriction;Q5=Year and data-column filtering" `
  --query-description-wrap-chars 28 `
  --query-description-max-lines 2 `
  --side-note "P - partitioned;NP - not partitioned" `
  "Polypheny Relational=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_polypheny_results.csv" `
  "DuckDB Repartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_repartitioned_results.csv" `
  "DuckDB Unpartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_unpartitioned_results.csv" `
  "Apache Spark Repartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_repartitioned_results.csv" `
  "Apache Spark Unpartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_unpartitioned_results.csv"
```

## Create plot with standard-deviation whiskers

This creates additional `_std` files and leaves the mean-only plot files unchanged.

```powershell
python plugins\parquet-adapter\benchmarks\scripts\plot_generation\benchmark_result_std_plot_generator.py `
  --title "Partitioning by Year and Month" `
  --name partitioning_plot_std `
  --query-order "Q1_P,Q1_NP,Q2_P,Q2_NP,Q3_P,Q3_NP,Q4_P,Q4_NP,Q5_P,Q5_NP" `
  --query-descriptions "Q1=Full-count baseline;Q2=Year-filtered count;Q3=Year-and-month-filtered count;Q4=Data-column filtering, no partition restriction;Q5=Year and data-column filtering" `
  --query-description-wrap-chars 28 `
  --query-description-max-lines 2 `
  --side-note "P - partitioned;NP - not partitioned" `
  "Polypheny Relational=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_polypheny_results.csv" `
  "DuckDB Repartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_repartitioned_results.csv" `
  "DuckDB Unpartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_duckdb_unpartitioned_results.csv" `
  "Apache Spark Repartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_repartitioned_results.csv" `
  "Apache Spark Unpartitioned=plugins\parquet-adapter\benchmarks\results\partitioning\partitioning_spark_unpartitioned_results.csv"
```
