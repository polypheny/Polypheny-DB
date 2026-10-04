#!/usr/bin/env python3

import argparse
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path


def main():
    args = parse_args()
    inputs = [parse_input(value) for value in args.values]
    if len(inputs) < 2:
        raise ValueError("At least two value files are required for comparison.")

    records_by_system = {
        system: read_records(path)
        for system, path in inputs
    }
    reference_system = args.reference or inputs[0][0]
    if reference_system not in records_by_system:
        raise ValueError(f"Reference system not found: {reference_system}")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        render_markdown(records_by_system, inputs, reference_system, args),
        encoding="utf-8",
    )
    print(f"Wrote {output}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare captured aggregation result values across systems."
    )
    parser.add_argument(
        "values",
        nargs="+",
        help="JSONL value file. Use LABEL=path.jsonl to control the system name.",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Markdown output file.",
    )
    parser.add_argument(
        "--reference",
        default="",
        help="Reference system label. Defaults to the first input.",
    )
    parser.add_argument(
        "--abs-tolerance",
        default="0.000001",
        help="Absolute tolerance for numeric comparisons. Default: 0.000001.",
    )
    parser.add_argument(
        "--rel-tolerance",
        default="0.000001",
        help="Relative tolerance for numeric comparisons. Default: 0.000001.",
    )
    parser.add_argument(
        "--title",
        default="Aggregation Correctness Summary",
        help="Markdown title.",
    )
    return parser.parse_args()


def parse_input(value):
    if "=" in value:
        label, path = value.split("=", 1)
        return label.strip(), Path(path.strip())
    path = Path(value)
    return infer_label(path), path


def infer_label(path):
    name = path.stem
    for suffix in ("_values", "_results"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name.replace("_", " ").title()


def read_records(path):
    records = {}
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            query_id = str(record.get("query_id", "")).strip()
            if not query_id:
                raise ValueError(f"{path}:{line_number}: missing query_id")
            records[query_id] = record
    return records


def render_markdown(records_by_system, inputs, reference_system, args):
    systems = [system for system, _path in inputs]
    reference_records = records_by_system[reference_system]
    query_ids = sorted(
        {query_id for records in records_by_system.values() for query_id in records},
        key=natural_key,
    )

    lines = [
        f"# {args.title}",
        "",
        f"Reference system: `{reference_system}`.",
        (
            "Numeric values are compared with absolute tolerance "
            f"`{args.abs_tolerance}` and relative tolerance `{args.rel_tolerance}`."
        ),
        "",
        "## Source Files",
        "",
        "| System | Values file |",
        "| --- | --- |",
    ]
    for system, path in inputs:
        lines.append(f"| {escape_md(system)} | `{escape_md(str(path))}` |")

    lines.extend(
        [
            "",
            "## Comparison",
            "",
            "| Query | " + " | ".join(escape_md(system) for system in systems if system != reference_system) + " |",
            "| --- | " + " | ".join("---" for system in systems if system != reference_system) + " |",
        ]
    )

    abs_tolerance = Decimal(args.abs_tolerance)
    rel_tolerance = Decimal(args.rel_tolerance)

    for query_id in query_ids:
        row = [escape_md(query_id)]
        reference = reference_records.get(query_id)
        for system in systems:
            if system == reference_system:
                continue
            status, details = compare_records(
                reference,
                records_by_system[system].get(query_id),
                abs_tolerance,
                rel_tolerance,
            )
            row.append(escape_md(status if not details else f"{status}: {details}"))
        lines.append("| " + " | ".join(row) + " |")

    lines.extend(
        [
            "",
            "## Compared Values",
            "",
            "The following tables align captured result values by query.",
            "Each table includes the reference system so that mismatches can be inspected directly.",
        ]
    )
    for query_id in query_ids:
        lines.extend(render_query_values(
            query_id,
            systems,
            records_by_system,
            reference_system,
        ))

    lines.append("")
    return "\n".join(lines)


def compare_records(reference, candidate, abs_tolerance, rel_tolerance):
    if reference is None:
        return "missing reference", ""
    if candidate is None:
        return "missing", ""
    if not reference.get("success", False):
        return "reference failed", reference.get("error", "")
    if not candidate.get("success", False):
        return "failed", candidate.get("error", "")

    reference_columns = [str(column).lower() for column in reference.get("columns", [])]
    candidate_columns = [str(column).lower() for column in candidate.get("columns", [])]
    if reference_columns != candidate_columns:
        return "differs", "columns differ"

    reference_rows = sorted(reference.get("rows", []), key=canonical_row_key)
    candidate_rows = sorted(candidate.get("rows", []), key=canonical_row_key)
    if len(reference_rows) != len(candidate_rows):
        return "differs", f"row count {len(candidate_rows)} != {len(reference_rows)}"

    for row_index, (reference_row, candidate_row) in enumerate(zip(reference_rows, candidate_rows), start=1):
        if len(reference_row) != len(candidate_row):
            return "differs", f"row {row_index} column count differs"
        for column_index, (reference_value, candidate_value) in enumerate(zip(reference_row, candidate_row), start=1):
            if not values_match(reference_value, candidate_value, abs_tolerance, rel_tolerance):
                return "differs", f"row {row_index}, column {column_index}"

    return "ok", ""


def render_query_values(
    query_id,
    systems,
    records_by_system,
    reference_system,
):
    values_by_system = {
        system: flatten_record(records_by_system[system].get(query_id))
        for system in systems
    }
    labels = ordered_value_labels(values_by_system, systems, reference_system)
    lines = [
        "",
        f"### {escape_md(query_id)}",
        "",
        "| Result | " + " | ".join(escape_md(system) for system in systems) + " |",
        "| --- | " + " | ".join("---" for _system in systems) + " |",
    ]
    if not labels:
        lines.append("| No captured values | " + " | ".join("" for _system in systems) + " |")
        return lines

    for label in labels:
        row = [escape_md(label)]
        for system in systems:
            row.append(escape_md(values_by_system[system].get(label, "")))
        lines.append("| " + " | ".join(row) + " |")
    return lines


def ordered_value_labels(values_by_system, systems, reference_system):
    ordered = []
    seen = set()

    def add_labels(system):
        for label in values_by_system.get(system, {}):
            if label not in seen:
                ordered.append(label)
                seen.add(label)

    add_labels(reference_system)
    for system in systems:
        add_labels(system)
    return ordered


def flatten_record(record):
    if record is None:
        return {"status": "missing"}
    if not record.get("success", False):
        return {"status": "failed: " + str(record.get("error", ""))}

    rows = record.get("rows", [])
    if not rows:
        return {"status": "no rows"}

    columns = normalized_columns(record)
    if len(rows) == 1 and len(columns) == 1:
        return {columns[0]: rows[0][0]}

    key_indexes = infer_key_indexes(columns, rows)
    value_indexes = [index for index in range(len(columns)) if index not in key_indexes]
    flattened = {}

    if not value_indexes:
        for row_index, row in enumerate(rows, start=1):
            values = list(row) + [""] * max(0, len(columns) - len(row))
            label = f"row {row_index}"
            flattened[label] = ", ".join(
                f"{columns[index]}={values[index]}"
                for index in range(len(columns))
            )
        return flattened

    for row in rows:
        values = list(row) + [""] * max(0, len(columns) - len(row))
        key = format_key(columns, values, key_indexes)
        for index in value_indexes:
            label = columns[index] if not key else f"{key} / {columns[index]}"
            flattened[unique_label(flattened, label)] = values[index]
    return flattened


def infer_key_indexes(columns, rows):
    if len(columns) == 1:
        return []

    key_indexes = []
    for index, column in enumerate(columns):
        column_lower = str(column).lower()
        if is_key_column(column_lower):
            key_indexes.append(index)

    if key_indexes:
        return key_indexes

    # If the schema is unfamiliar, keep the first column as a row identity and
    # compare the remaining values as measures.
    return [0]


def is_key_column(column):
    if column in {"_id", "year", "month"}:
        return True
    if column.endswith("_flag"):
        return True
    return False


def format_key(columns, values, key_indexes):
    return ", ".join(
        f"{columns[index]}={values[index]}"
        for index in key_indexes
    )


def unique_label(values, label):
    if label not in values:
        return label
    counter = 2
    while f"{label} #{counter}" in values:
        counter += 1
    return f"{label} #{counter}"


def normalized_columns(record):
    columns = [str(column) for column in record.get("columns", [])]
    max_row_width = max((len(row) for row in record.get("rows", [])), default=0)
    if not columns and max_row_width == 1:
        columns = ["value"]
    while len(columns) < max_row_width:
        columns.append(f"value_{len(columns) + 1}")
    return columns


def values_match(reference, candidate, abs_tolerance, rel_tolerance):
    if reference is None or candidate is None:
        return reference is None and candidate is None

    reference_decimal = parse_decimal(reference)
    candidate_decimal = parse_decimal(candidate)
    if reference_decimal is not None and candidate_decimal is not None:
        difference = abs(reference_decimal - candidate_decimal)
        if difference <= abs_tolerance:
            return True
        largest = max(abs(reference_decimal), abs(candidate_decimal), Decimal("1"))
        return difference / largest <= rel_tolerance

    return str(reference) == str(candidate)


def parse_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def canonical_row_key(row):
    return tuple("" if value is None else str(value) for value in row)


def natural_key(value):
    parts = []
    current = ""
    for char in str(value):
        if not current or char.isdigit() == current[-1].isdigit():
            current += char
        else:
            parts.append(current)
            current = char
    if current:
        parts.append(current)
    return [int(part) if part.isdigit() else part for part in parts]


def escape_md(value):
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


if __name__ == "__main__":
    main()
