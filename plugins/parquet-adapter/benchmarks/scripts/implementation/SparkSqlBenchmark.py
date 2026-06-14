#!/usr/bin/env python3

import argparse
import csv
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession


TABLES = (
    "yellow_tripdata",
    "green_tripdata",
    "fhv_tripdata",
    "fhvhv_tripdata",
)


class Query:

    def __init__(self, query_id, description, sql):
        self.query_id = query_id
        self.description = description
        self.sql = sql


def main():
    args = parse_args()
    ad_hoc_sql = read_ad_hoc_sql(args)
    queries = (
        [Query("SQL", "Ad hoc SQL", ad_hoc_sql)]
        if ad_hoc_sql.strip()
        else load_queries(Path(args.queries))
    )

    if args.only:
        allowed = {part.strip().upper() for part in args.only.split(",") if part.strip()}
        queries = [query for query in queries if query.query_id.upper() in allowed]

    if not queries:
        raise ValueError(f"No queries found in {args.queries}")

    output = Path(args.output)
    if output.parent:
        output.parent.mkdir(parents=True, exist_ok=True)

    spark = create_spark(args)
    try:
        setup_views(spark, args.data_dir)
        run_queries(spark, queries, args, output)
    finally:
        spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the Parquet adapter benchmark query set with Apache Spark SQL."
    )
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument("--nested-customer-file", default="")
    parser.add_argument(
        "--queries",
        default="/repo/plugins/parquet-adapter/benchmarks/query_lists/access_model_comparison/access_model_comparison_sql.sql",
    )
    parser.add_argument(
        "--output",
        default="/repo/plugins/parquet-adapter/benchmarks/results/access_model_comparison/spark_results.csv",
    )
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--only", default="")
    parser.add_argument("--sql", default="")
    parser.add_argument("--sql-file", default="")
    parser.add_argument("--app-name", default="ParquetAdapterSparkBenchmark")
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--console-prefix", default="")
    parser.add_argument(
        "--drain-mode",
        choices=("executor", "driver"),
        default="executor",
        help=(
            "executor drains result rows in Spark tasks and returns row counts only; "
            "driver streams all rows back through PySpark's local iterator."
        ),
    )
    parser.add_argument("--print-rows", action="store_true")
    return parser.parse_args()


def create_spark(args):
    return (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.caseSensitive", "false")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.parquetBenchmark.consolePrefix", args.console_prefix)
        .config("spark.parquetBenchmark.nestedCustomerFile", args.nested_customer_file)
        .getOrCreate()
    )


def setup_views(spark, data_dir):
    data_dir = data_dir.rstrip("/\\")
    spark.sparkContext.setLogLevel("ERROR")
    log(spark, f"Data directory {data_dir}")
    nested_customer_file = spark.conf.get("spark.parquetBenchmark.nestedCustomerFile", "")
    if nested_customer_file:
        if not path_exists(nested_customer_file):
            raise FileNotFoundError(f"Missing nested customer Parquet file: {nested_customer_file}")
        dataframe = spark.read.parquet(nested_customer_file)
        dataframe.createOrReplaceTempView("nested_customer")
        log(spark, f"Created Spark temp view nested_customer from {nested_customer_file}")
        return
    for table in TABLES:
        table_path = join_path(data_dir, table)
        if not path_exists(table_path):
            raise FileNotFoundError(f"Missing Spark benchmark table path: {table_path}")
        dataframe = spark.read.option("basePath", table_path).parquet(table_path)
        dataframe.createOrReplaceTempView(table)
        log(spark, f"Created Spark temp view {table} from {table_path}")


def run_queries(spark, queries, args, output):
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "timestamp",
                "query_id",
                "description",
                "phase",
                "run",
                "elapsed_ms",
                "rows",
                "columns",
                "success",
                "error",
            ]
        )
        handle.flush()

        log(spark, f"Loaded {len(queries)} queries")
        log(spark, f"Writing CSV to {output}")

        for query in queries:
            log(spark)
            log(spark, f"{query.query_id} {query.description}")
            for run in range(1, args.warmups + 1):
                execute_and_record(spark, writer, handle, query, "warmup", run, args)
            for run in range(1, args.runs + 1):
                execute_and_record(spark, writer, handle, query, "measured", run, args)


def execute_and_record(spark, writer, handle, query, phase, run, args):
    start = time.perf_counter()
    rows = 0
    columns = 0
    success = False
    error = ""

    try:
        result = spark.sql(prepare_sql_for_spark(query.sql))
        columns = len(result.columns)
        if args.drain_mode == "driver" or args.print_rows:
            for row in result.toLocalIterator():
                rows += 1
                if args.print_rows:
                    log(spark, "\t".join(format_cell(value) for value in row))
        else:
            rows = sum(result.rdd.mapPartitions(count_partition_rows).collect())
        success = True
    except Exception as exc:
        error = f"{exc.__class__.__name__}: {exc}".replace("\r", " ").replace("\n", " ")
        log(spark, f"  {phase} {run} failed: {error}")

    elapsed_ms = round((time.perf_counter() - start) * 1000)
    if success:
        log(spark, f"  {phase} {run}: {elapsed_ms} ms, rows={rows}")

    writer.writerow(
        [
            timestamp_utc(),
            query.query_id,
            query.description,
            phase,
            run,
            elapsed_ms,
            rows,
            columns,
            str(success).lower(),
            error,
        ]
    )
    handle.flush()


def load_queries(path):
    lines = path.read_text(encoding="utf-8").splitlines()
    queries = []
    current_id = None
    current_description = None
    sql_lines = []

    for line in lines:
        trimmed = line.strip()
        if trimmed.startswith("-- Q") and ":" in trimmed:
            add_query(queries, current_id, current_description, sql_lines)
            colon = trimmed.index(":")
            current_id = trimmed[3:colon].strip().upper()
            current_description = trimmed[colon + 1 :].strip()
            sql_lines = []
            continue
        if trimmed.startswith("--") or not trimmed:
            continue
        sql_lines.append(line)
        if trimmed.endswith(";"):
            add_query(queries, current_id, current_description, sql_lines)
            current_id = None
            current_description = None
            sql_lines = []

    add_query(queries, current_id, current_description, sql_lines)
    return queries


def add_query(queries, query_id, description, sql_lines):
    statement = "\n".join(sql_lines).strip()
    if not statement:
        return
    if statement.endswith(";"):
        statement = statement[:-1]
    queries.append(Query(query_id or "SQL", description or "", statement))


def read_ad_hoc_sql(args):
    if args.sql_file:
        return Path(args.sql_file).read_text(encoding="utf-8")
    return args.sql


def prepare_sql_for_spark(sql):
    return re.sub(r'"([A-Za-z_][A-Za-z0-9_]*)"', r"`\1`", sql).rstrip(";")


def count_partition_rows(iterator):
    count = 0
    for _ in iterator:
        count += 1
    return [count]


def log(spark, message=""):
    prefix = spark.conf.get("spark.parquetBenchmark.consolePrefix", "")
    print(f"{prefix}{message}", flush=True)


def join_path(parent, child):
    if parent.startswith("/") or parent.startswith("file:"):
        return f"{parent.rstrip('/')}/{child}"
    return str(Path(parent) / child)


def path_exists(path):
    if path.startswith("file:"):
        return Path(path[5:]).exists()
    return Path(path).exists()


def timestamp_utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def format_cell(value):
    return "NULL" if value is None else str(value)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Spark benchmark failed: {exc}", file=sys.stderr)
        raise
