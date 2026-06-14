#!/usr/bin/env python3

import argparse
import csv
import re
import statistics
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Summary:
    system: str
    query_id: str
    description: str
    total_runs: int
    successful_runs: int
    elapsed_values: list[int]
    row_values: list[int]
    column_values: list[int]
    errors: list[str]


def main():
    args = parse_args()
    inputs = [parse_input(value) for value in args.csv]
    summaries = []

    for system, path in inputs:
        summaries.extend(read_summaries(system, path, args.phase))

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_markdown(summaries, inputs, args), encoding="utf-8")
    print(f"Wrote {output}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create a markdown comparison table from benchmark result CSV files."
    )
    parser.add_argument(
        "csv",
        nargs="+",
        help="CSV input. Use LABEL=path.csv to control the system column name; otherwise the label is inferred.",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Output markdown file.",
    )
    parser.add_argument(
        "--phase",
        default="measured",
        help="Benchmark phase to summarize. Default: measured.",
    )
    parser.add_argument(
        "--unit",
        choices=("ms", "s"),
        default="ms",
        help="Time unit used in comparison tables. Default: ms.",
    )
    parser.add_argument(
        "--title",
        default="Benchmark Result Summary",
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
    name = re.sub(r"^access_model_comparison_", "", name)
    name = re.sub(r"_results$", "", name)
    name = re.sub(r"_tlcp$", "", name)
    name = name.replace("_", " ")
    return name.title()


def read_summaries(system, path, phase):
    groups = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if row.get("phase") != phase:
                continue
            query_id = row.get("query_id", "")
            groups.setdefault(query_id, []).append(row)

    summaries = []
    for query_id, rows in groups.items():
        successful = [row for row in rows if is_success(row.get("success", ""))]
        summaries.append(
            Summary(
                system=system,
                query_id=query_id,
                description=first_non_empty(row.get("description", "") for row in rows),
                total_runs=len(rows),
                successful_runs=len(successful),
                elapsed_values=[parse_int(row.get("elapsed_ms", "")) for row in successful if is_int(row.get("elapsed_ms", ""))],
                row_values=sorted({parse_int(row.get("rows", "")) for row in successful if is_int(row.get("rows", ""))}),
                column_values=sorted({parse_int(row.get("columns", "")) for row in successful if is_int(row.get("columns", ""))}),
                errors=[row.get("error", "") for row in rows if row.get("error", "")],
            )
        )
    return summaries


def render_markdown(summaries, inputs, args):
    systems = [label for label, _ in inputs]
    by_system_query = {(summary.system, summary.query_id): summary for summary in summaries}
    query_ids = sorted({summary.query_id for summary in summaries}, key=natural_key)
    description_by_query = description_map(summaries)
    unit_label = "ms" if args.unit == "ms" else "s"

    lines = [
        f"# {args.title}",
        "",
        f"Phase summarized: `{args.phase}`.",
        "Warmup rows are excluded. Mean and median values use successful runs only.",
        "",
        "## Source Files",
        "",
        "| System | CSV |",
        "| --- | --- |",
    ]
    for system, path in inputs:
        lines.append(f"| {escape_md(system)} | `{escape_md(str(path))}` |")

    lines.extend(
        [
            "",
            f"## Mean Elapsed Time ({unit_label})",
            "",
            "| Query | Description | " + " | ".join(escape_md(system) for system in systems) + " | Row counts |",
            "| --- | --- | " + " | ".join("---" for _ in systems) + " | --- |",
        ]
    )
    for query_id in query_ids:
        row = [
            escape_md(query_id),
            escape_md(description_by_query.get(query_id, "")),
        ]
        for system in systems:
            row.append(format_mean(by_system_query.get((system, query_id)), args.unit))
        row.append(format_row_consistency([by_system_query.get((system, query_id)) for system in systems]))
        lines.append("| " + " | ".join(row) + " |")

    lines.extend(
        [
            "",
            "## Result Row Counts",
            "",
            "| Query | Description | " + " | ".join(escape_md(system) for system in systems) + " |",
            "| --- | --- | " + " | ".join("---" for _ in systems) + " |",
        ]
    )
    for query_id in query_ids:
        row = [
            escape_md(query_id),
            escape_md(description_by_query.get(query_id, "")),
        ]
        for system in systems:
            row.append(format_values(by_system_query.get((system, query_id)), "rows"))
        lines.append("| " + " | ".join(row) + " |")

    lines.extend(
        [
            "",
            f"## Detailed Summary ({unit_label})",
            "",
            "| System | Query | Description | Runs | Mean | Median | Min | Max | Rows | Columns | Status |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for system in systems:
        for query_id in query_ids:
            summary = by_system_query.get((system, query_id))
            if summary is None:
                continue
            lines.append(
                "| "
                + " | ".join(
                    [
                        escape_md(system),
                        escape_md(query_id),
                        escape_md(summary.description),
                        f"{summary.successful_runs}/{summary.total_runs}",
                        format_stat(summary, statistics.mean, args.unit),
                        format_stat(summary, statistics.median, args.unit),
                        format_stat(summary, min, args.unit),
                        format_stat(summary, max, args.unit),
                        format_values(summary, "rows"),
                        format_values(summary, "columns"),
                        escape_md(status(summary)),
                    ]
                )
                + " |"
            )

    lines.append("")
    return "\n".join(lines)


def description_map(summaries):
    result = {}
    for summary in sorted(summaries, key=lambda item: natural_key(item.query_id)):
        result.setdefault(summary.query_id, summary.description)
    return result


def format_mean(summary, unit):
    return format_stat(summary, statistics.mean, unit)


def format_stat(summary, fn, unit):
    if summary is None or not summary.elapsed_values:
        return ""
    value = fn(summary.elapsed_values)
    if unit == "s":
        return f"{value / 1000.0:,.3f}"
    return f"{value:,.1f}"


def format_row_consistency(summaries):
    values = []
    for summary in summaries:
        if summary is None:
            continue
        if len(summary.row_values) == 1:
            values.append(summary.row_values[0])
        else:
            return "differs"
    return format_int(values[0]) if values and all(value == values[0] for value in values) else "differs"


def format_values(summary, attr):
    if summary is None:
        return ""
    values = summary.row_values if attr == "rows" else summary.column_values
    if not values:
        return ""
    return ", ".join(format_int(value) for value in values)


def status(summary):
    if summary.successful_runs == summary.total_runs and not summary.errors:
        return "ok"
    if summary.errors:
        return f"{summary.successful_runs}/{summary.total_runs} ok; " + "; ".join(summary.errors)
    return f"{summary.successful_runs}/{summary.total_runs} ok"


def first_non_empty(values):
    for value in values:
        if value:
            return value
    return ""


def is_success(value):
    return str(value).strip().lower() == "true"


def is_int(value):
    try:
        int(value)
        return True
    except (TypeError, ValueError):
        return False


def parse_int(value):
    return int(value)


def format_int(value):
    return f"{value:,}"


def natural_key(value):
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", value)]


def escape_md(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


if __name__ == "__main__":
    main()
