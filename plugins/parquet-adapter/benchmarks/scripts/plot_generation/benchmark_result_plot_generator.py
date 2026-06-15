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


@dataclass
class PlotGeometry:
    width: int
    height: int
    left: int
    right: int
    top: int
    bottom: int
    plot_width: int
    plot_height: int
    lower: float
    upper: float
    ticks: list[float]
    group_width: float
    bar_gap: int
    bar_width: float


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
    for file_format in parse_formats(args.formats):
        output = output_dir / f"{args.name}.{file_format}"
        if file_format == "svg":
            output.write_text(render_svg(series, query_ids, args.title), encoding="utf-8")
        elif file_format == "png":
            write_png(output, series, query_ids, args.title)
        elif file_format == "pdf":
            write_pdf(output, series, query_ids, args.title)
        else:
            raise ValueError(f"Unsupported plot format: {file_format}")
        print(f"Wrote {output}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate runtime plots from benchmark result CSV files."
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
        help="Directory where plot files are written.",
    )
    parser.add_argument(
        "--formats",
        default="svg,pdf,png",
        help="Comma-separated output formats to write. Default: svg,pdf,png.",
    )
    parser.add_argument(
        "--phase",
        default="measured",
        help="Benchmark phase to plot. Default: measured.",
    )
    return parser.parse_args()


def parse_formats(value):
    valid = {"svg", "pdf", "png"}
    formats = []
    for part in value.split(","):
        file_format = part.strip().lower()
        if not file_format:
            continue
        if file_format not in valid:
            raise ValueError(f"Unsupported plot format: {file_format}")
        formats.append(file_format)
    if not formats:
        raise ValueError("At least one plot format must be selected.")
    return formats


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
    geometry = make_geometry(series, query_ids)

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{geometry.width}" height="{geometry.height}" viewBox="0 0 {geometry.width} {geometry.height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{geometry.width / 2}" y="34" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="24" font-weight="700">{escape(title)}</text>',
        f'<text x="{geometry.left}" y="62" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#333">Mean execution time (s, log scale)</text>',
    ]

    for tick in geometry.ticks:
        y = y_for(tick, geometry)
        lines.append(f'<line x1="{geometry.left}" y1="{y:.2f}" x2="{geometry.left + geometry.plot_width}" y2="{y:.2f}" stroke="#e2e2e2" stroke-width="1"/>')
        lines.append(f'<text x="{geometry.left - 12}" y="{y + 4:.2f}" text-anchor="end" font-family="Helvetica, Arial, sans-serif" font-size="11" fill="#333">{format_tick(tick)}</text>')

    lines.append(f'<line x1="{geometry.left}" y1="{geometry.top}" x2="{geometry.left}" y2="{geometry.top + geometry.plot_height}" stroke="#333" stroke-width="1.2"/>')
    lines.append(f'<line x1="{geometry.left}" y1="{geometry.top + geometry.plot_height}" x2="{geometry.left + geometry.plot_width}" y2="{geometry.top + geometry.plot_height}" stroke="#333" stroke-width="1.2"/>')

    for bar in bars(series, query_ids, geometry):
        x, y, width, height, item, query_id, value = bar
        lines.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{width:.2f}" height="{height:.2f}" fill="{item.color}">'
            f'<title>{escape(item.label)} {escape(query_id)}: {value:.3f} s</title></rect>'
        )

    for query_index, query_id in enumerate(query_ids):
        center = geometry.left + query_index * geometry.group_width + geometry.group_width / 2
        lines.append(f'<text x="{center:.2f}" y="{geometry.top + geometry.plot_height + 30}" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="12" font-weight="700">{escape(query_id)}</text>')

    legend_x = geometry.left + geometry.plot_width + 35
    legend_y = geometry.top + 15
    lines.append(f'<text x="{legend_x}" y="{legend_y - 18}" font-family="Helvetica, Arial, sans-serif" font-size="13" font-weight="700">System</text>')
    for index, item in enumerate(series):
        y = legend_y + index * 28
        lines.append(f'<rect x="{legend_x}" y="{y - 12}" width="16" height="16" fill="{item.color}"/>')
        lines.append(f'<text x="{legend_x + 24}" y="{y + 1}" font-family="Helvetica, Arial, sans-serif" font-size="12">{escape(item.label)}</text>')

    lines.append("</svg>")
    return "\n".join(lines)


def write_png(output, series, query_ids, title):
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as error:
        raise RuntimeError("PNG output requires Pillow.") from error

    geometry = make_geometry(series, query_ids)
    image = Image.new("RGB", (geometry.width, geometry.height), "white")
    draw = ImageDraw.Draw(image)
    fonts = {
        "title": load_font(ImageFont, 24, bold=True),
        "subtitle": load_font(ImageFont, 13),
        "tick": load_font(ImageFont, 11),
        "axis": load_font(ImageFont, 12, bold=True),
        "legend": load_font(ImageFont, 12),
        "legend_title": load_font(ImageFont, 13, bold=True),
    }

    draw.text((geometry.width / 2, 34), title, anchor="mm", font=fonts["title"], fill="#000000")
    draw.text((geometry.left, 62), "Mean execution time (s, log scale)", anchor="la", font=fonts["subtitle"], fill="#333333")

    for tick in geometry.ticks:
        y = y_for(tick, geometry)
        draw.line((geometry.left, y, geometry.left + geometry.plot_width, y), fill="#e2e2e2", width=1)
        draw.text((geometry.left - 12, y), format_tick(tick), anchor="rm", font=fonts["tick"], fill="#333333")

    draw.line((geometry.left, geometry.top, geometry.left, geometry.top + geometry.plot_height), fill="#333333", width=1)
    draw.line((geometry.left, geometry.top + geometry.plot_height, geometry.left + geometry.plot_width, geometry.top + geometry.plot_height), fill="#333333", width=1)

    for x, y, width, height, item, _query_id, _value in bars(series, query_ids, geometry):
        draw.rectangle((x, y, x + width, y + height), fill=item.color)

    for query_index, query_id in enumerate(query_ids):
        center = geometry.left + query_index * geometry.group_width + geometry.group_width / 2
        draw.text((center, geometry.top + geometry.plot_height + 30), query_id, anchor="mm", font=fonts["axis"], fill="#000000")

    legend_x = geometry.left + geometry.plot_width + 35
    legend_y = geometry.top + 15
    draw.text((legend_x, legend_y - 18), "System", anchor="la", font=fonts["legend_title"], fill="#000000")
    for index, item in enumerate(series):
        y = legend_y + index * 28
        draw.rectangle((legend_x, y - 12, legend_x + 16, y + 4), fill=item.color)
        draw.text((legend_x + 24, y), item.label, anchor="lm", font=fonts["legend"], fill="#000000")

    image.save(output)


def write_pdf(output, series, query_ids, title):
    geometry = make_geometry(series, query_ids)
    commands = []

    def py(y):
        return geometry.height - y

    def set_fill(color):
        r, g, b = color_parts(color)
        commands.append(f"{r:.3f} {g:.3f} {b:.3f} rg")

    def set_stroke(color):
        r, g, b = color_parts(color)
        commands.append(f"{r:.3f} {g:.3f} {b:.3f} RG")

    def rect(x, y, width, height, color):
        set_fill(color)
        commands.append(f"{x:.2f} {py(y + height):.2f} {width:.2f} {height:.2f} re f")

    def line(x1, y1, x2, y2, color, width=1):
        set_stroke(color)
        commands.append(f"{width:.2f} w")
        commands.append(f"{x1:.2f} {py(y1):.2f} m {x2:.2f} {py(y2):.2f} l S")

    def text(x, y, value, size, color="#000000", bold=False, anchor="left"):
        set_fill(color)
        font = "/F2" if bold else "/F1"
        adjusted_x = aligned_text_x(x, value, size, bold, anchor)
        commands.append(
            f"BT {font} {size} Tf 1 0 0 1 {adjusted_x:.2f} {py(y):.2f} Tm ({pdf_escape(value)}) Tj ET"
        )

    rect(0, 0, geometry.width, geometry.height, "#ffffff")
    text(geometry.width / 2, 34, title, 24, bold=True, anchor="center")
    text(geometry.left, 62, "Mean execution time (s, log scale)", 13, color="#333333")

    for tick in geometry.ticks:
        y = y_for(tick, geometry)
        line(geometry.left, y, geometry.left + geometry.plot_width, y, "#e2e2e2")
        text(geometry.left - 12, y + 4, format_tick(tick), 11, color="#333333", anchor="right")

    line(geometry.left, geometry.top, geometry.left, geometry.top + geometry.plot_height, "#333333", 1.2)
    line(
        geometry.left,
        geometry.top + geometry.plot_height,
        geometry.left + geometry.plot_width,
        geometry.top + geometry.plot_height,
        "#333333",
        1.2,
    )

    for x, y, width, height, item, _query_id, _value in bars(series, query_ids, geometry):
        rect(x, y, width, height, item.color)

    for query_index, query_id in enumerate(query_ids):
        center = geometry.left + query_index * geometry.group_width + geometry.group_width / 2
        text(center, geometry.top + geometry.plot_height + 30, query_id, 12, bold=True, anchor="center")

    legend_x = geometry.left + geometry.plot_width + 35
    legend_y = geometry.top + 15
    text(legend_x, legend_y - 18, "System", 13, bold=True)
    for index, item in enumerate(series):
        y = legend_y + index * 28
        rect(legend_x, y - 12, 16, 16, item.color)
        text(legend_x + 24, y + 1, item.label, 12)

    write_simple_pdf(output, geometry.width, geometry.height, "\n".join(commands))


def color_parts(color):
    value = color.lstrip("#")
    return tuple(int(value[index:index + 2], 16) / 255.0 for index in (0, 2, 4))


def aligned_text_x(x, value, size, bold, anchor):
    if anchor == "left":
        return x
    width = approximate_text_width(value, size, bold)
    if anchor == "center":
        return x - width / 2
    if anchor == "right":
        return x - width
    raise ValueError(f"Unsupported text anchor: {anchor}")


def approximate_text_width(value, size, bold):
    factor = 0.58 if bold else 0.52
    return len(str(value)) * size * factor


def pdf_escape(value):
    return str(value).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def write_simple_pdf(output, width, height, content):
    content_bytes = content.encode("latin-1", errors="replace")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {width} {height}] "
            f"/Resources << /Font << /F1 4 0 R /F2 5 0 R >> >> /Contents 6 0 R >>"
        ).encode("ascii"),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>",
        b"<< /Length " + str(len(content_bytes)).encode("ascii") + b" >>\nstream\n" + content_bytes + b"\nendstream",
    ]

    pdf = b"%PDF-1.4\n"
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf += f"{index} 0 obj\n".encode("ascii") + obj + b"\nendobj\n"

    xref_start = len(pdf)
    pdf += f"xref\n0 {len(objects) + 1}\n".encode("ascii")
    pdf += b"0000000000 65535 f \n"
    for offset in offsets[1:]:
        pdf += f"{offset:010d} 00000 n \n".encode("ascii")
    pdf += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_start}\n%%EOF\n"
    ).encode("ascii")

    output.write_bytes(pdf)


def load_font(image_font, size, bold=False):
    candidates = (
        ("arialbd.ttf", "DejaVuSans-Bold.ttf") if bold else ("arial.ttf", "DejaVuSans.ttf")
    )
    for candidate in candidates:
        try:
            return image_font.truetype(candidate, size)
        except OSError:
            continue
    return image_font.load_default()


def make_geometry(series, query_ids):
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
    group_width = plot_width / len(query_ids)
    bar_gap = 3
    bar_width = min(24, (group_width - 20) / len(series) - bar_gap)
    return PlotGeometry(
        width,
        height,
        left,
        right,
        top,
        bottom,
        plot_width,
        plot_height,
        lower,
        upper,
        ticks,
        group_width,
        bar_gap,
        max(1.0, bar_width),
    )


def bars(series, query_ids, geometry):
    for query_index, query_id in enumerate(query_ids):
        group_x = geometry.left + query_index * geometry.group_width
        total_bar_width = len(series) * geometry.bar_width + (len(series) - 1) * geometry.bar_gap
        start_x = group_x + (geometry.group_width - total_bar_width) / 2
        for series_index, item in enumerate(series):
            value = item.values.get(query_id)
            if value is None:
                continue
            x = start_x + series_index * (geometry.bar_width + geometry.bar_gap)
            y = y_for(value, geometry)
            height = geometry.top + geometry.plot_height - y
            yield x, y, geometry.bar_width, height, item, query_id, value


def plot_bounds(series):
    values = [value for item in series for value in item.values.values() if value > 0]
    if not values:
        raise ValueError("Log-scale plot requires positive elapsed values.")
    lower = 10 ** math.floor(math.log10(min(values)))
    upper = 10 ** math.ceil(math.log10(max(values)))
    if lower == upper:
        upper = lower * 10
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


def y_for(value, geometry):
    lower_log = math.log10(geometry.lower)
    upper_log = math.log10(geometry.upper)
    value_log = math.log10(value)
    ratio = (value_log - lower_log) / (upper_log - lower_log)
    return geometry.top + geometry.plot_height - ratio * geometry.plot_height


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
