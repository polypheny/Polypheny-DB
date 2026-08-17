#!/usr/bin/env python3

import argparse
import csv
import html
import math
import statistics
from dataclasses import dataclass
from pathlib import Path


COLORS = (
    "#4f9d55",
    "#c94c4c",
    "#7b61b9",
    "#f0a22e",
    "#2f6fbb",
    "#4aa3a1",
)

SYSTEM_COLORS = (
    ("Polypheny Relational Normalized", "#c94c4c"),
    ("Polypheny Relational Flat", "#4f9d55"),
    ("Polypheny Relational", "#4f9d55"),
    ("Polypheny Document", "#7b61b9"),
    ("DuckDB", "#f0a22e"),
    ("Apache Spark", "#2f6fbb"),
)


@dataclass
class Series:
    label: str
    visual_label: str
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
    bar_gap: int
    bar_width: float
    query_centers: dict[str, float]


def main():
    args = parse_args()
    inputs = [parse_input(value) for value in args.csv]
    query_descriptions = parse_query_descriptions(args.query_descriptions)
    color_by_label = {}
    fallback_color_index = 0
    series = []
    for label, path in inputs:
        visual_label = normalize_visual_label(label)
        if visual_label not in color_by_label:
            color, fallback_color_index = color_for_label(visual_label, fallback_color_index)
            color_by_label[visual_label] = color
        series.append(
            Series(
                label,
                visual_label,
                color_by_label[visual_label],
                read_means(path, args.phase),
            )
        )
    query_ids = ordered_query_ids(series, args.query_order)
    if not query_ids:
        raise ValueError("No successful benchmark rows found.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for file_format in parse_formats(args.formats):
        output = output_dir / f"{args.name}.{file_format}"
        if file_format == "svg":
            output.write_text(
                render_svg(
                    series,
                    query_ids,
                    args.title,
                    query_descriptions,
                    args.query_description_wrap_chars,
                    args.query_description_max_lines,
                    args.side_note,
                ),
                encoding="utf-8",
            )
        elif file_format == "png":
            write_png(
                output,
                series,
                query_ids,
                args.title,
                query_descriptions,
                args.query_description_wrap_chars,
                args.query_description_max_lines,
                args.side_note,
            )
        elif file_format == "pdf":
            write_pdf(
                output,
                series,
                query_ids,
                args.title,
                query_descriptions,
                args.query_description_wrap_chars,
                args.query_description_max_lines,
                args.side_note,
            )
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
    parser.add_argument(
        "--query-order",
        default="",
        help="Comma-separated query IDs that define the x-axis order. Missing IDs are appended using natural order.",
    )
    parser.add_argument(
        "--query-descriptions",
        default="",
        help="Semicolon-separated query descriptions, for example Q1=Full count;Q2=Year filter.",
    )
    parser.add_argument(
        "--query-description-wrap-chars",
        type=int,
        default=18,
        help="Approximate line length for query descriptions. Default: 18.",
    )
    parser.add_argument(
        "--query-description-max-lines",
        type=int,
        default=2,
        help="Maximum number of lines per query description. Default: 2.",
    )
    parser.add_argument(
        "--side-note",
        default="",
        help="Semicolon-separated note lines to render below the legend.",
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


def parse_query_descriptions(value):
    descriptions = {}
    for part in value.split(";"):
        if "=" not in part:
            continue
        query_id, description = part.split("=", 1)
        query_id = query_id.strip()
        description = description.strip()
        if query_id and description:
            descriptions[query_id] = description
    return descriptions


def parse_side_note(value):
    return [part.strip() for part in value.split(";") if part.strip()]


def side_note_parts(note):
    if " - " not in note:
        return note, ""
    prefix, rest = note.split(" - ", 1)
    return prefix, " - " + rest


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


def ordered_query_ids(series, query_order):
    available = {query_id for item in series for query_id in item.values}
    if not query_order:
        return sorted(available, key=natural_key)

    requested = [part.strip() for part in query_order.split(",") if part.strip()]
    ordered = []
    seen = set()
    for query_id in requested:
        if query_id in available and query_id not in seen:
            ordered.append(query_id)
            seen.add(query_id)

    ordered.extend(sorted(available - seen, key=natural_key))
    return ordered


def render_svg(series, query_ids, title, query_descriptions, description_wrap_chars, description_max_lines, side_note=""):
    geometry = make_geometry(
        series,
        query_ids,
        query_descriptions,
        description_wrap_chars,
        description_max_lines,
    )

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
        center = geometry.query_centers[query_id]
        lines.append(f'<text x="{center:.2f}" y="{geometry.top + geometry.plot_height + 30}" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="12" font-weight="700">{escape(query_id)}</text>')

    for center, description_lines in query_description_items(query_ids, geometry, query_descriptions, description_wrap_chars, description_max_lines):
        for line_index, description in enumerate(description_lines):
            y = geometry.top + geometry.plot_height + 55 + line_index * 17
            lines.append(f'<text x="{center:.2f}" y="{y}" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="13" fill="#333">{escape(description)}</text>')

    legend_x = geometry.left + geometry.plot_width + 35
    legend_y = geometry.top + 32
    lines.append(f'<text x="{legend_x}" y="{legend_y - 32}" font-family="Helvetica, Arial, sans-serif" font-size="13" font-weight="700">System</text>')
    for index, item in enumerate(legend_items(series)):
        y = legend_y + index * 28
        label, color = item
        lines.append(f'<rect x="{legend_x}" y="{y - 12}" width="16" height="16" fill="{color}"/>')
        lines.append(f'<text x="{legend_x + 24}" y="{y + 1}" font-family="Helvetica, Arial, sans-serif" font-size="12">{escape(label)}</text>')

    for index, note in enumerate(parse_side_note(side_note)):
        y = legend_y + len(legend_items(series)) * 28 + 18 + index * 18
        prefix, rest = side_note_parts(note)
        lines.append(f'<text x="{legend_x}" y="{y}" font-family="Helvetica, Arial, sans-serif" font-size="12" font-weight="700" fill="#333">{escape(prefix)}</text>')
        lines.append(f'<text x="{legend_x + approximate_text_width(prefix, 12, True)}" y="{y}" font-family="Helvetica, Arial, sans-serif" font-size="12" fill="#333">{escape(rest)}</text>')

    lines.append("</svg>")
    return "\n".join(lines)


def write_png(output, series, query_ids, title, query_descriptions, description_wrap_chars, description_max_lines, side_note=""):
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as error:
        raise RuntimeError("PNG output requires Pillow.") from error

    geometry = make_geometry(
        series,
        query_ids,
        query_descriptions,
        description_wrap_chars,
        description_max_lines,
    )
    image = Image.new("RGB", (geometry.width, geometry.height), "white")
    draw = ImageDraw.Draw(image)
    fonts = {
        "title": load_font(ImageFont, 24, bold=True),
        "subtitle": load_font(ImageFont, 13),
        "tick": load_font(ImageFont, 11),
        "axis": load_font(ImageFont, 12, bold=True),
        "description": load_font(ImageFont, 13),
        "legend": load_font(ImageFont, 12),
        "legend_title": load_font(ImageFont, 13, bold=True),
        "note_bold": load_font(ImageFont, 12, bold=True),
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
        center = geometry.query_centers[query_id]
        draw.text((center, geometry.top + geometry.plot_height + 30), query_id, anchor="mm", font=fonts["axis"], fill="#000000")

    for center, description_lines in query_description_items(query_ids, geometry, query_descriptions, description_wrap_chars, description_max_lines):
        for line_index, description in enumerate(description_lines):
            y = geometry.top + geometry.plot_height + 55 + line_index * 17
            draw.text((center, y), description, anchor="mm", font=fonts["description"], fill="#333333")

    legend_x = geometry.left + geometry.plot_width + 35
    legend_y = geometry.top + 32
    draw.text((legend_x, legend_y - 32), "System", anchor="la", font=fonts["legend_title"], fill="#000000")
    for index, item in enumerate(legend_items(series)):
        y = legend_y + index * 28
        label, color = item
        draw.rectangle((legend_x, y - 12, legend_x + 16, y + 4), fill=color)
        draw.text((legend_x + 24, y), label, anchor="lm", font=fonts["legend"], fill="#000000")

    for index, note in enumerate(parse_side_note(side_note)):
        y = legend_y + len(legend_items(series)) * 28 + 18 + index * 18
        prefix, rest = side_note_parts(note)
        draw.text((legend_x, y), prefix, anchor="la", font=fonts["note_bold"], fill="#333333")
        draw.text((legend_x + draw.textlength(prefix, font=fonts["note_bold"]), y), rest, anchor="la", font=fonts["legend"], fill="#333333")

    image.save(output)


def write_pdf(output, series, query_ids, title, query_descriptions, description_wrap_chars, description_max_lines, side_note=""):
    geometry = make_geometry(
        series,
        query_ids,
        query_descriptions,
        description_wrap_chars,
        description_max_lines,
    )
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
        center = geometry.query_centers[query_id]
        text(center, geometry.top + geometry.plot_height + 30, query_id, 12, bold=True, anchor="center")

    for center, description_lines in query_description_items(query_ids, geometry, query_descriptions, description_wrap_chars, description_max_lines):
        for line_index, description in enumerate(description_lines):
            y = geometry.top + geometry.plot_height + 55 + line_index * 17
            text(center, y, description, 13, color="#333333", anchor="center")

    legend_x = geometry.left + geometry.plot_width + 35
    legend_y = geometry.top + 32
    text(legend_x, legend_y - 32, "System", 13, bold=True)
    for index, item in enumerate(legend_items(series)):
        y = legend_y + index * 28
        label, color = item
        rect(legend_x, y - 12, 16, 16, color)
        text(legend_x + 24, y + 1, label, 12)

    for index, note in enumerate(parse_side_note(side_note)):
        y = legend_y + len(legend_items(series)) * 28 + 18 + index * 18
        prefix, rest = side_note_parts(note)
        text(legend_x, y, prefix, 12, color="#333333", bold=True)
        text(legend_x + approximate_text_width(prefix, 12, True), y, rest, 12, color="#333333")

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


def font_widths(*groups):
    return {
        character: width
        for width, characters in groups
        for character in characters
    }


# Adobe Font Metrics widths for the built-in PDF Helvetica fonts, expressed in
# thousandths of an em. Using the real glyph widths keeps independently drawn
# lines centered on the same x coordinate.
HELVETICA_WIDTHS = font_widths(
    (191, "'"),
    (222, "ijl"),
    (260, "|"),
    (278, " !,./:;I[\\]ft"),
    (333, "()-`r"),
    (334, "{}"),
    (355, '"'),
    (389, "*"),
    (469, "^"),
    (500, "Jcksvxyz"),
    (556, "#$0123456789?_Labdeghnopqu"),
    (584, "+<=>~"),
    (611, "FTZ"),
    (667, "&ABEKPSXY"),
    (722, "CDHNRUVw"),
    (778, "GOQ"),
    (833, "Mm"),
    (889, "%"),
    (944, "W"),
    (1015, "@"),
)

HELVETICA_BOLD_WIDTHS = font_widths(
    (238, "'"),
    (278, " ,./I\\ijl"),
    (280, "|"),
    (333, "!()-:;[]`ft"),
    (389, "*r{}"),
    (474, '"'),
    (500, "z"),
    (556, "#$0123456789J_aceksvxy"),
    (584, "+<=>^~"),
    (611, "?FLTZbdghnopqu"),
    (667, "EPSVXY"),
    (722, "&ABCDHKNRU"),
    (778, "GOQw"),
    (833, "M"),
    (889, "%m"),
    (944, "W"),
    (975, "@"),
)


def approximate_text_width(value, size, bold):
    widths = HELVETICA_BOLD_WIDTHS if bold else HELVETICA_WIDTHS
    width_units = sum(widths.get(character, 556) for character in str(value))
    return width_units * size / 1000


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


def make_geometry(series, query_ids, query_descriptions=None, description_wrap_chars=18, description_max_lines=2):
    width = plot_width_for_descriptions(
        query_ids,
        query_descriptions,
        description_wrap_chars,
        description_max_lines,
    )
    height = 720 if query_descriptions else 620
    left = 90
    right = 260
    top = 80
    bottom = 130 if query_descriptions else 80
    plot_width = width - left - right
    plot_height = height - top - bottom
    lower, upper = plot_bounds(series)
    ticks = nice_log_ticks(lower, upper)
    bar_gap = 3
    slots = series_slots(series)
    bar_width, query_centers = query_layout(query_ids, slots, plot_width, left, bar_gap)
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
        bar_gap,
        max(1.0, bar_width),
        query_centers,
    )


def plot_width_for_descriptions(query_ids, query_descriptions, description_wrap_chars, description_max_lines):
    base_width = 1180
    left = 90
    right = 260
    groups = description_groups(query_ids, query_descriptions)
    if not groups:
        return base_width

    longest_line = max(
        len(line)
        for _prefix, description in groups
        for line in wrap_text(description, description_wrap_chars, description_max_lines)
    )
    group_width = min(145, max(105, longest_line * 7.4 + 26))
    return max(base_width, int(left + right + len(groups) * group_width))


def bars(series, query_ids, geometry):
    slots = series_slots(series)
    slot_indexes = {label: index for index, label in enumerate(slots)}
    for query_id in query_ids:
        total_bar_width = len(slots) * geometry.bar_width + (len(slots) - 1) * geometry.bar_gap
        start_x = geometry.query_centers[query_id] - total_bar_width / 2
        for item in series:
            value = item.values.get(query_id)
            if value is None:
                continue
            x = start_x + slot_indexes[item.visual_label] * (geometry.bar_width + geometry.bar_gap)
            y = y_for(value, geometry)
            height = geometry.top + geometry.plot_height - y
            yield x, y, geometry.bar_width, height, item, query_id, value


def query_layout(query_ids, slots, plot_width, left, bar_gap):
    pair_gap_units = layout_pair_gap_units(query_ids)
    use_pair_spacing = any(unit == 1 for unit in pair_gap_units)
    if not use_pair_spacing:
        group_width = plot_width / len(query_ids)
        bar_width = max(1.0, min(24, (group_width - 20) / len(slots) - bar_gap))
        centers = {
            query_id: left + index * group_width + group_width / 2
            for index, query_id in enumerate(query_ids)
        }
        return bar_width, centers

    fixed_gap_width = len(query_ids) * (len(slots) - 1) * bar_gap
    bar_units = len(query_ids) * len(slots) + sum(pair_gap_units)
    bar_width = max(1.0, min(24, (plot_width - fixed_gap_width) / bar_units))
    group_bar_width = len(slots) * bar_width + (len(slots) - 1) * bar_gap
    required_width = len(query_ids) * group_bar_width + sum(unit * bar_width for unit in pair_gap_units)
    cursor = left + max(0, (plot_width - required_width) / 2)

    centers = {}
    for index, query_id in enumerate(query_ids):
        centers[query_id] = cursor + group_bar_width / 2
        cursor += group_bar_width
        if index < len(pair_gap_units):
            cursor += pair_gap_units[index] * bar_width
    return bar_width, centers


def layout_pair_gap_units(query_ids):
    return [
        1 if paired_layout_prefix(left) is not None and paired_layout_prefix(left) == paired_layout_prefix(right) else 3
        for left, right in zip(query_ids, query_ids[1:])
    ]


def paired_layout_prefix(query_id):
    if "_" not in query_id:
        return None
    prefix, suffix = query_id.rsplit("_", 1)
    if suffix in {"P", "NP", "RP", "UP"}:
        return prefix
    return None


def description_groups(query_ids, query_descriptions):
    if not query_descriptions:
        return []
    groups = []
    seen = set()
    for query_id in query_ids:
        prefix = paired_layout_prefix(query_id) or query_id
        if prefix in seen:
            continue
        seen.add(prefix)
        description = query_descriptions.get(prefix) or query_descriptions.get(query_id)
        if description:
            groups.append((prefix, description))
    return groups


def query_description_items(query_ids, geometry, query_descriptions, description_wrap_chars, description_max_lines):
    if not query_descriptions:
        return []
    items = []
    seen = set()
    for query_id in query_ids:
        prefix = paired_layout_prefix(query_id) or query_id
        if prefix in seen:
            continue
        seen.add(prefix)
        related = [
            item
            for item in query_ids
            if (paired_layout_prefix(item) or item) == prefix
        ]
        description = query_descriptions.get(prefix) or query_descriptions.get(query_id)
        if not description:
            continue
        center = sum(geometry.query_centers[item] for item in related) / len(related)
        items.append((center, wrap_text(description, description_wrap_chars, description_max_lines)))
    return items


def wrap_text(text, max_chars, max_lines):
    lines = []
    current = ""
    for word in text.split():
        candidate = word if not current else current + " " + word
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            lines.append(current)
        current = word
    if current:
        lines.append(current)
    return lines[:max_lines]


def normalize_visual_label(label):
    for suffix in (" Repartitioned", " Unpartitioned"):
        if label.endswith(suffix):
            return label[: -len(suffix)]
    return label


def color_for_label(label, fallback_color_index):
    for prefix, color in SYSTEM_COLORS:
        if label.startswith(prefix):
            return color, fallback_color_index
    return COLORS[fallback_color_index % len(COLORS)], fallback_color_index + 1


def series_slots(series):
    slots = []
    seen = set()
    for item in series:
        if item.visual_label in seen:
            continue
        seen.add(item.visual_label)
        slots.append(item.visual_label)
    return slots


def legend_items(series):
    items = []
    seen = set()
    for item in series:
        if item.visual_label in seen:
            continue
        seen.add(item.visual_label)
        items.append((item.visual_label, item.color))
    return items


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
    suffix_order = {"P": 0, "RP": 0, "NP": 1, "UP": 1}
    if "_" in value:
        prefix, suffix = value.rsplit("_", 1)
        if suffix in suffix_order:
            return natural_key(prefix) + [suffix_order[suffix]]

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
