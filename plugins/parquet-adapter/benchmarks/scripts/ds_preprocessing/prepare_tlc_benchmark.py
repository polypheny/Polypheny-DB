#!/usr/bin/env python3
"""Prepare NYC TLC Parquet files for Parquet adapter benchmarks.

The script reads the official flat TLC monthly Parquet files and writes a
Hive-style partitioned dataset with nested structs and a small repeated field.
Raw input files are never modified.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


DEFAULT_INPUT_DIR = Path(r"C:\tmp\tlc\original")
DEFAULT_OUTPUT_DIR = Path(r"E:\tmp\tlc\benchmark_nested")
DEFAULT_DEPS_DIR = Path(r"C:\tmp\tlc\.pydeps")

FILE_RE = re.compile(r"(?P<trip_type>.+)_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.parquet$")


def load_duckdb(deps_dir: Path):
    if deps_dir.exists():
        sys.path.insert(0, str(deps_dir))
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "duckdb is required. Install it with:\n"
            f'  "{sys.executable}" -m pip install --target "{deps_dir}" duckdb'
        ) from exc
    return duckdb


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("\\", "/").replace("'", "''") + "'"


def sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def find_col(columns: set[str], *candidates: str) -> str | None:
    by_lower = {col.lower(): col for col in columns}
    for candidate in candidates:
        found = by_lower.get(candidate.lower())
        if found is not None:
            return found
    return None


def col(columns: set[str], name: str, fallback: str = "NULL") -> str:
    found = find_col(columns, name)
    return sql_ident(found) if found else fallback


def typed_col(columns: set[str], name: str, sql_type: str) -> str:
    found = find_col(columns, name)
    if found is None:
        return f"NULL::{sql_type}"
    return f"try_cast({sql_ident(found)} AS {sql_type})"


def typed_col_any(columns: set[str], names: tuple[str, ...], sql_type: str) -> str:
    for name in names:
        found = find_col(columns, name)
        if found is not None:
            return f"try_cast({sql_ident(found)} AS {sql_type})"
    return f"NULL::{sql_type}"


def duration_expr(pickup: str, dropoff: str) -> str:
    return f"date_diff('second', {pickup}, {dropoff})"


def trip_id_expr(year: str, month: str) -> str:
    return f"({int(year)}::BIGINT * 10000000000 + {int(month)}::BIGINT * 100000000 + row_number() OVER ())"


def taxi_select(trip_type: str, year: str, month: str, source_file: Path, columns: set[str], limit: int | None) -> str:
    pickup_ts = typed_col(columns, "tpep_pickup_datetime" if trip_type == "yellow" else "lpep_pickup_datetime", "TIMESTAMP")
    dropoff_ts = typed_col(columns, "tpep_dropoff_datetime" if trip_type == "yellow" else "lpep_dropoff_datetime", "TIMESTAMP")
    airport_fee = typed_col_any(columns, ("airport_fee", "Airport_fee"), "DOUBLE")

    limit_sql = f"\n  LIMIT {limit}" if limit else ""
    return f"""
SELECT
  {trip_id_expr(year, month)} AS trip_id,
  struct_pack(
    trip_type := {sql_string(trip_type)},
    source_file := {sql_string(source_file.name)}
  ) AS source,
  struct_pack(
    vendor_id := {typed_col(columns, "VendorID", "BIGINT")},
    rate_code_id := {typed_col(columns, "RatecodeID", "DOUBLE")},
    store_and_fwd_flag := {typed_col(columns, "store_and_fwd_flag", "VARCHAR")}
  ) AS vendor,
  struct_pack(
    ts := {pickup_ts},
    location_id := {typed_col(columns, "PULocationID", "BIGINT")}
  ) AS pickup,
  struct_pack(
    ts := {dropoff_ts},
    location_id := {typed_col(columns, "DOLocationID", "BIGINT")}
  ) AS dropoff,
  struct_pack(
    count := {typed_col(columns, "passenger_count", "DOUBLE")}
  ) AS passenger,
  struct_pack(
    distance := {typed_col(columns, "trip_distance", "DOUBLE")},
    duration_seconds := {duration_expr(pickup_ts, dropoff_ts)}
  ) AS trip,
  struct_pack(
    payment_type := {typed_col(columns, "payment_type", "DOUBLE")},
    trip_type := {typed_col(columns, "trip_type", "DOUBLE")}
  ) AS payment,
  struct_pack(
    base := {typed_col(columns, "fare_amount", "DOUBLE")},
    extra := {typed_col(columns, "extra", "DOUBLE")},
    mta_tax := {typed_col(columns, "mta_tax", "DOUBLE")},
    tip := {typed_col(columns, "tip_amount", "DOUBLE")},
    tolls := {typed_col(columns, "tolls_amount", "DOUBLE")},
    improvement := {typed_col(columns, "improvement_surcharge", "DOUBLE")},
    congestion := {typed_col(columns, "congestion_surcharge", "DOUBLE")},
    airport := {airport_fee},
    total := {typed_col(columns, "total_amount", "DOUBLE")},
    components := [
      struct_pack(name := 'tip', amount := {typed_col(columns, "tip_amount", "DOUBLE")})
    ]
  ) AS fare
FROM read_parquet({sql_string(source_file)}){limit_sql}
"""


def hvfhv_select(year: str, month: str, source_file: Path, columns: set[str], limit: int | None) -> str:
    pickup_ts = typed_col(columns, "pickup_datetime", "TIMESTAMP")
    dropoff_ts = typed_col(columns, "dropoff_datetime", "TIMESTAMP")
    limit_sql = f"\n  LIMIT {limit}" if limit else ""
    return f"""
SELECT
  {trip_id_expr(year, month)} AS trip_id,
  struct_pack(
    trip_type := 'fhvhv',
    source_file := {sql_string(source_file.name)}
  ) AS source,
  struct_pack(
    hvfhs_license_num := {typed_col(columns, "hvfhs_license_num", "VARCHAR")},
    dispatching_base_num := {typed_col(columns, "dispatching_base_num", "VARCHAR")},
    originating_base_num := {typed_col(columns, "originating_base_num", "VARCHAR")}
  ) AS operator,
  struct_pack(
    request_ts := {typed_col(columns, "request_datetime", "TIMESTAMP")},
    on_scene_ts := {typed_col(columns, "on_scene_datetime", "TIMESTAMP")},
    pickup_ts := {pickup_ts},
    dropoff_ts := {dropoff_ts}
  ) AS lifecycle,
  struct_pack(
    ts := {pickup_ts},
    location_id := {typed_col(columns, "PULocationID", "BIGINT")}
  ) AS pickup,
  struct_pack(
    ts := {dropoff_ts},
    location_id := {typed_col(columns, "DOLocationID", "BIGINT")}
  ) AS dropoff,
  struct_pack(
    distance := {typed_col(columns, "trip_miles", "DOUBLE")},
    duration_seconds := {typed_col(columns, "trip_time", "BIGINT")}
  ) AS trip,
  struct_pack(
    shared_request_flag := {typed_col(columns, "shared_request_flag", "VARCHAR")},
    shared_match_flag := {typed_col(columns, "shared_match_flag", "VARCHAR")},
    access_a_ride_flag := {typed_col(columns, "access_a_ride_flag", "VARCHAR")},
    wav_request_flag := {typed_col(columns, "wav_request_flag", "VARCHAR")},
    wav_match_flag := {typed_col(columns, "wav_match_flag", "VARCHAR")}
  ) AS flags,
  struct_pack(
    base := {typed_col(columns, "base_passenger_fare", "DOUBLE")},
    tolls := {typed_col(columns, "tolls", "DOUBLE")},
    bcf := {typed_col(columns, "bcf", "DOUBLE")},
    sales_tax := {typed_col(columns, "sales_tax", "DOUBLE")},
    congestion := {typed_col(columns, "congestion_surcharge", "DOUBLE")},
    airport := {typed_col(columns, "airport_fee", "DOUBLE")},
    tip := {typed_col(columns, "tips", "DOUBLE")},
    driver_pay := {typed_col(columns, "driver_pay", "DOUBLE")},
    components := [
      struct_pack(name := 'tip', amount := {typed_col(columns, "tips", "DOUBLE")})
    ]
  ) AS fare
FROM read_parquet({sql_string(source_file)}){limit_sql}
"""


def fhv_select(year: str, month: str, source_file: Path, columns: set[str], limit: int | None) -> str:
    pickup_ts = typed_col(columns, "pickup_datetime", "TIMESTAMP")
    dropoff_ts = typed_col(columns, "dropOff_datetime", "TIMESTAMP")
    limit_sql = f"\n  LIMIT {limit}" if limit else ""
    return f"""
SELECT
  {trip_id_expr(year, month)} AS trip_id,
  struct_pack(
    trip_type := 'fhv',
    source_file := {sql_string(source_file.name)}
  ) AS source,
  struct_pack(
    dispatching_base_num := {typed_col(columns, "dispatching_base_num", "VARCHAR")},
    affiliated_base_number := {typed_col(columns, "Affiliated_base_number", "VARCHAR")}
  ) AS operator,
  struct_pack(
    ts := {pickup_ts},
    location_id := {typed_col(columns, "PUlocationID", "BIGINT")}
  ) AS pickup,
  struct_pack(
    ts := {dropoff_ts},
    location_id := {typed_col(columns, "DOlocationID", "BIGINT")}
  ) AS dropoff,
  struct_pack(
    duration_seconds := {duration_expr(pickup_ts, dropoff_ts)},
    sr_flag := {typed_col(columns, "SR_Flag", "VARCHAR")}
  ) AS trip,
  [
    struct_pack(name := 'pickup', ts := {pickup_ts}),
    struct_pack(name := 'dropoff', ts := {dropoff_ts})
  ] AS events
FROM read_parquet({sql_string(source_file)}){limit_sql}
"""


def build_select(trip_type: str, year: str, month: str, source_file: Path, columns: set[str], limit: int | None) -> str:
    if trip_type in {"yellow", "green"}:
        return taxi_select(trip_type, year, month, source_file, columns, limit)
    if trip_type == "fhvhv":
        return hvfhv_select(year, month, source_file, columns, limit)
    if trip_type == "fhv":
        return fhv_select(year, month, source_file, columns, limit)
    raise ValueError(f"Unsupported TLC trip type: {trip_type}")


def discover_files(input_dir: Path, trip_types: set[str], years: set[str]) -> list[tuple[Path, str, str, str]]:
    files: list[tuple[Path, str, str, str]] = []
    for path in sorted(input_dir.glob("*.parquet")):
        match = FILE_RE.fullmatch(path.name)
        if not match:
            continue
        trip_type = match.group("trip_type")
        year = match.group("year")
        month = match.group("month")
        if trip_types and trip_type not in trip_types:
            continue
        if years and year not in years:
            continue
        files.append((path, trip_type, year, month))
    return files


def parquet_columns(duckdb, source_file: Path) -> set[str]:
    con = duckdb.connect()
    try:
        rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_string(source_file)})").fetchall()
    finally:
        con.close()
    return {row[0] for row in rows}


def write_file(duckdb, select_sql: str, output_file: Path, compression: str, threads: int) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"PRAGMA threads={threads}")
        con.execute(
            f"COPY ({select_sql}) TO {sql_string(output_file)} "
            f"(FORMAT PARQUET, COMPRESSION {compression})"
        )
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--deps-dir", type=Path, default=DEFAULT_DEPS_DIR)
    parser.add_argument("--trip-types", nargs="*", default=["yellow", "green", "fhv", "fhvhv"])
    parser.add_argument("--years", nargs="*", default=["2021", "2022", "2023"])
    parser.add_argument("--compression", default="SNAPPY", choices=["SNAPPY", "ZSTD", "GZIP", "UNCOMPRESSED"])
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--limit-per-file", type=int, default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.input_dir.exists():
        raise SystemExit(f"Input directory does not exist: {args.input_dir}")

    duckdb = load_duckdb(args.deps_dir)
    trip_types = set(args.trip_types or [])
    years = set(args.years or [])
    files = discover_files(args.input_dir, trip_types, years)
    if not files:
        raise SystemExit("No matching TLC Parquet files found.")

    print(f"Input:  {args.input_dir}")
    print(f"Output: {args.output_dir}")
    print(f"Files:  {len(files)}")

    for index, (source_file, trip_type, year, month) in enumerate(files, start=1):
        table_dir = f"{trip_type}_trips_nested"
        output_file = args.output_dir / table_dir / f"year={year}" / f"month={month}" / f"part-{trip_type}-{year}-{month}.parquet"

        if output_file.exists() and not args.overwrite:
            print(f"[{index}/{len(files)}] skip existing {output_file}")
            continue
        if output_file.exists() and args.overwrite:
            output_file.unlink()

        columns = parquet_columns(duckdb, source_file)
        select_sql = build_select(trip_type, year, month, source_file, columns, args.limit_per_file)

        print(f"[{index}/{len(files)}] {source_file.name} -> {output_file}")
        if args.dry_run:
            continue
        write_file(duckdb, select_sql, output_file, args.compression, args.threads)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
