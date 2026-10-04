

# NYC TLC nested benchmark dataset

`prepare_tlc_benchmark.py` converts the official NYC TLC monthly Parquet files
into a Hive-style partitioned dataset with nested Parquet fields.

Default input:

```text
C:\tmp\tlc\original
```

Default output:

```text
E:\tmp\tlc\benchmark_nested
```

The generated layout is:

```text
benchmark_nested/
  yellow_trips_nested/year=2021/month=01/part-yellow-2021-01.parquet
  green_trips_nested/year=2021/month=01/part-green-2021-01.parquet
  fhv_trips_nested/year=2021/month=01/part-fhv-2021-01.parquet
  fhvhv_trips_nested/year=2021/month=01/part-fhvhv-2021-01.parquet
```

Each table contains nested `struct` columns. Taxi and HVFHV tables also contain a
repeated `fare.components` list of structs; FHV contains a repeated `events`
list of structs.

Run the full conversion:

```powershell
& "C:\Users\igorc\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe" `
  plugins\parquet-adapter\benchmarks\scripts\ds_preprocessing\prepare_tlc_benchmark.py `
  --output-dir E:\tmp\tlc\benchmark_nested
```

Run a small sample:

```powershell
& "C:\Users\igorc\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe" `
  plugins\parquet-adapter\benchmarks\scripts\ds_preprocessing\prepare_tlc_benchmark.py `
  --output-dir E:\tmp\tlc\benchmark_nested_sample `
  --years 2021 `
  --limit-per-file 1000 `
  --overwrite
```

The script requires DuckDB in the local dependency folder:

```powershell
& "C:\Users\igorc\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe" `
  -m pip install --target C:\tmp\tlc\.pydeps duckdb
```
