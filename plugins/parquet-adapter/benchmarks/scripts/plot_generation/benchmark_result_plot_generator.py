#!/usr/bin/env python3

import argparse
import csv
import html
import math
import statistics
from dataclasses import dataclass
from pathlib import Path


COLORS = (
    "#2f6fbb",
    "#c94c4c",
    "#4f9d55",
    "#f0a22e",
    "#7b61b9",
    "#4aa3a1",
)


@dataclass
class Series:
    label: str
    color: str
    values: dict[str, float]


def main():
    args = parse_args()
    inputs = [parse_input(value) for value in args.csv]
    series = [
        Series(label, COLORS[index % len(COLORS)], read_means(path, args.phase))
        for index, (label, path) in enumerate(inputs)
    ]
    query_ids = sorted({query_id for item in series for query_id in item.values}, key=natural_key)
    if not query_ids:
        raise ValueError("No successful benchmark rows found.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / f"{args.name}.svg"
    output.write_text(render_svg(series, query_ids, args.title), encoding="utf-8")
    print(f"Wrote {output}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate an SVG runtime plot from benchmark result CSV files."
    )
    parser.add_argument(
        "csv",
        nargs="+",
        help="CSV input. Use LABEL=path.csv to control the legend label.",
    )
    parser.add_argument("--title", required=True, help="Plot title.")
    parser.add_argument("--name", required=True, help="Output file name without extension.")
    parser.add_argument(
        "--output-dir",
        default="plugins/parquet-adapter/benchmarks/results/plots",
        help="Directory where the SVG plot is written.",
    )
    parser.add_argument(
        "--phase",
        default="measured",
        help="Benchmark phase to plot. Default: measured.",
    )
    return parser.parse_args()


def parse_input(value):
    if "=" in value:
        label, path = value.split("=", 1)
        return label.strip(), Path(path.strip())
    path = Path(value)
    return path.stem, path


def read_means(path, phase):
    elapsed_by_query = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if row.get("phase") != phase:
                continue
            if row.get("success", "").strip().lower() != "true":
                continue
            query_id = row.get("query_id", "").strip()
            elapsed_ms = row.get("elapsed_ms", "").strip()
            if not query_id or not elapsed_ms:
                continue
            elapsed_by_query.setdefault(query_id, []).append(float(elapsed_ms) / 1000.0)
    return {
        query_id: statistics.mean(values)
        for query_id, values in elapsed_by_query.items()
        if values
    }


def render_svg(series, query_ids, title):
    width = 1180
    height = 700
    left = 90
    right = 260
    top = 80
    bottom = 120
    plot_width = width - left - right
    plot_height = height - top - bottom
    lower, upper = plot_bounds(series)
    ticks = nice_log_ticks(lower, upper)

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="24" font-weight="700">{escape(title)}</text>',
        f'<text x="{left}" y="62" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#333">Mean execution time (s, log scale)</text>',
    ]

    for tick in ticks:
        y = y_for(tick, lower, upper, top, plot_height)
        lines.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_width}" y2="{y:.2f}" stroke="#e2e2e2" stroke-width="1"/>')
        lines.append(f'<text x="{left - 12}" y="{y + 4:.2f}" text-anchor="end" font-family="Helvetica, Arial, sans-serif" font-size="11" fill="#333">{format_tick(tick)}</text>')

    lines.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#333" stroke-width="1.2"/>')
    lines.append(f'<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#333" stroke-width="1.2"/>')

    group_width = plot_width / len(query_ids)
    bar_gap = 3
    bar_width = min(24, (group_width - 20) / len(series) - bar_gap)

    for query_index, query_id in enumerate(query_ids):
        group_x = left + query_index * group_width
        total_bar_width = len(series) * bar_width + (len(series) - 1) * bar_gap
        start_x = group_x + (group_width - total_bar_width) / 2
        for series_index, item in enumerate(series):
            value = item.values.get(query_id)
            if value is None:
                continue
            x = start_x + series_index * (bar_width + bar_gap)
            y = y_for(value, lower, upper, top, plot_height)
            h = top + plot_height - y
            lines.append(
                f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width:.2f}" height="{h:.2f}" fill="{item.color}">'
                f'<title>{escape(item.label)} {escape(query_id)}: {value:.3f} s</title></rect>'
            )
        center = group_x + group_width / 2
        lines.append(f'<text x="{center:.2f}" y="{top + plot_height + 30}" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="12" font-weight="700">{escape(query_id)}</text>')

    legend_x = left + plot_width + 35
    legend_y = top + 15
    lines.append(f'<text x="{legend_x}" y="{legend_y - 18}" font-family="Helvetica, Arial, sans-serif" font-size="13" font-weight="700">System</text>')
    for index, item in enumerate(series):
        y = legend_y + index * 28
        lines.append(f'<rect x="{legend_x}" y="{y - 12}" width="16" height="16" fill="{item.color}"/>')
        lines.append(f'<text x="{legend_x + 24}" y="{y + 1}" font-family="Helvetica, Arial, sans-serif" font-size="12">{escape(item.label)}</text>')

    lines.append("</svg>")
    return "\n".join(lines)


def plot_bounds(series):
    values = [value for item in series for value in item.values.values() if value > 0]
    if not values:
        raise ValueError("Log-scale plot requires positive elapsed values.")
    lower = 10 ** math.floor(math.log10(min(values)))
    upper = 10 ** math.ceil(math.log10(max(values)))
    return lower, upper


def nice_log_ticks(lower, upper):
    ticks = []
    lower_exp = math.floor(math.log10(lower))
    upper_exp = math.ceil(math.log10(upper))
    for exp in range(lower_exp, upper_exp + 1):
        for multiplier in (1, 2, 5):
            value = multiplier * (10 ** exp)
            if lower <= value <= upper:
                ticks.append(value)
    return ticks


def y_for(value, lower, upper, top, height):
    lower_log = math.log10(lower)
    upper_log = math.log10(upper)
    value_log = math.log10(value)
    ratio = (value_log - lower_log) / (upper_log - lower_log)
    return top + height - ratio * height


def format_tick(value):
    if value >= 1:
        return f"{value:g}"
    return f"{value:.3f}".rstrip("0").rstrip(".")


def natural_key(value):
    parts = []
    current = ""
    for char in value:
        if char.isdigit() == (current[-1:].isdigit() if current else char.isdigit()):
            current += char
        else:
            parts.append(current)
            current = char
    if current:
        parts.append(current)
    return [int(part) if part.isdigit() else part for part in parts]


def escape(value):
    return html.escape(str(value), quote=True)


if __name__ == "__main__":
    main()
