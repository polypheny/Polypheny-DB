## DuckDB

DuckDB runs as an embedded database inside the Java benchmark process.

At a high level:

- The PowerShell runner compiles the Java benchmark client.
- The client loads DuckDB’s JDBC driver.
- DuckDB creates views over the Parquet folders.
- The client executes each query and drains the full result set.
- Timings are written to a CSV file.

## Apache Spark
Spark runs inside a Docker container, so no local Spark installation is required.

At a high level:

- The PowerShell runner starts a Spark Docker image.
- The repository and Parquet dataset are mounted into the container.
- A Python script creates Spark temporary views over the Parquet folders.
- Spark executes the same SQL queries.
- Results are fully materialized and timings are written to CSV.

## Polypheny
Polypheny runs as a separate database server, outside the benchmark client.

At a high level:

- Start Polypheny normally.
- Add the Parquet adapter and point it to relevant data
- Run the PowerShell benchmark wrapper
- The wrapper launches the Java benchmark client, which connects to Polypheny through the Prism JDBC interface
- The client maps query table names, executes each query, drains the full result set, and records elapsed time.

Unlike Spark, the benchmark wrapper does not start Polypheny automatically. The DBMS must already be running with the Parquet adapter configured.
