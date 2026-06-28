# Benchmark pipeline

## Run benchmark:

cd C:\github\Polypheny-DB

build jar:
.\gradlew.bat :dbms:shadowJar --no-daemon

run jar:
java -Xms8g -Xmx16g "-Dfile.encoding=UTF-8" -jar .\dbms\build\libs\dbms-0.10.1-SNAPSHOT.jar

PR flat

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_polypheny_benchmark.ps1 `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\access_model_comparison\access_model_comparison_rf.sql `
  -Output plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_rf_tlcp_results.csv `
  -Warmups 1 `
  -Runs 5 `
  -NoTableNameMapping
```

PR normalized

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_polypheny_benchmark.ps1 `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\access_model_comparison\access_model_comparison_rn.sql `
  -Output plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_rn_tlcpn_results.csv `
  -Warmups 1 `
  -Runs 5 `
  -NoTableNameMapping
```

PD

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_polypheny_mql_benchmark.ps1 `
  -Namespace tlcpd_document `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\access_model_comparison\access_model_comparison_mql.mql `
  -Output plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_mql_tlcp_results.csv `
  -Warmups 1 `
  -Runs 5
```

DuckDB

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_duckdb_benchmark.ps1 `
  -DataDir C:\PolyData\tlc_partitioned `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\access_model_comparison\access_model_comparison_sql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_duckdb_tlcp_results.csv `
  -Warmups 1 `
  -Runs 5
```

Spark

```powershell
powershell -ExecutionPolicy Bypass `
  -File plugins\parquet-adapter\benchmarks\scripts\runners\run_spark_benchmark.ps1 `
  -DataDir C:\PolyData\tlc_partitioned `
  -Queries plugins\parquet-adapter\benchmarks\query_lists\access_model_comparison\access_model_comparison_sql.sql `
  -Output plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_spark_tlcp_results.csv `
  -Warmups 1 `
  -Runs 5
```

## Create summary


```powershell

python plugins\parquet-adapter\benchmarks\scripts\summarize_benchmark_results.py `
  --title "Access Model Comparison TLCP Summary" `
  --output plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_tlcp_summary.md `
  "Polypheny Relational Flat=plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_rf_tlcp_results.csv" `
  "Polypheny Relational Normalized=plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_rn_tlcpn_results.csv" `
  "Polypheny Document MQL=plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_mql_tlcp_results.csv" `
  "DuckDB=plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_duckdb_tlcp_results.csv" `
  "Apache Spark=plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_spark_tlcp_results.csv"

```

## Create plot

```powershell
python plugins\parquet-adapter\benchmarks\scripts\plot_generation\benchmark_result_plot_generator.py `
  --title "Access Model Comparison" `
  --name access_model_comparison_tlcp_plot `
  --query-order "Q01,Q02,Q03,Q04,Q05" `
  --query-descriptions "Q01=Full access;Q02=Projection;Q03=Filtered count;Q04=Filtered full access;Q05=Filtered projection" `
  --query-description-wrap-chars 30 `
  --query-description-max-lines 1 `
  "Polypheny Relational Flat=plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_rf_tlcp_results.csv" `
  "Polypheny Relational Normalized=plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_rn_tlcpn_results.csv" `
  "Polypheny Document MQL=plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_polypheny_mql_tlcp_results.csv" `
  "DuckDB=plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_duckdb_tlcp_results.csv" `
  "Apache Spark=plugins\parquet-adapter\benchmarks\results\access_model_comparison\access_model_comparison_spark_tlcp_results.csv"
```
