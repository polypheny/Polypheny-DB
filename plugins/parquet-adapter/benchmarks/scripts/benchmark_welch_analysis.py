#!/usr/bin/env python3

import argparse
import csv
import itertools
import math
import statistics
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path


@dataclass
class MeasurementInput:
    label: str
    path: Path
    values: dict[str, list[float]]
    attempted_runs: dict[str, int]


@dataclass
class SampleSummary:
    count: int
    mean: float
    standard_deviation: float


@dataclass
class WelchResult:
    group: str
    system_a: str
    query_a: str
    system_b: str
    query_b: str
    sample_a: SampleSummary
    sample_b: SampleSummary
    difference: float
    confidence_low: float
    confidence_high: float
    t_statistic: float
    degrees_of_freedom: float
    p_value: float
    adjusted_p_value: float = math.nan


def main():
    args = parse_args()
    inputs = [read_measurements(*parse_input(value), args.phase) for value in args.csv]
    results, skipped_measurements, skipped_comparisons = build_comparisons(
        inputs,
        args.comparison,
        args.alpha,
    )
    apply_holm_adjustment(results)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        render_report(
            args.title,
            inputs,
            results,
            skipped_measurements,
            skipped_comparisons,
            args.phase,
            args.alpha,
        ),
        encoding="utf-8",
    )
    print(f"Wrote {output}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run exploratory pairwise Welch t-tests on benchmark measurements."
    )
    parser.add_argument(
        "csv",
        nargs="+",
        help="CSV input. Use LABEL=path.csv to define the system label.",
    )
    parser.add_argument("--title", required=True, help="Report title.")
    parser.add_argument("--output", required=True, help="Markdown output path.")
    parser.add_argument(
        "--phase",
        default="measured",
        help="Benchmark phase to analyze. Default: measured.",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.05,
        help="Family-wise significance level. Default: 0.05.",
    )
    parser.add_argument(
        "--comparison",
        action="append",
        default=[],
        help=(
            "Additional comparison in LABEL@QUERY=LABEL@QUERY form. "
            "Useful for comparing different query IDs such as partitioned and unpartitioned variants."
        ),
    )
    args = parser.parse_args()
    if not 0 < args.alpha < 1:
        parser.error("--alpha must be between 0 and 1.")
    return args


def parse_input(value):
    if "=" in value:
        label, path = value.split("=", 1)
        return label.strip(), Path(path.strip())
    path = Path(value)
    return path.stem, path


def read_measurements(label, path, phase):
    values = {}
    attempted_runs = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if row.get("phase") != phase:
                continue
            query_id = row.get("query_id", "").strip()
            if not query_id:
                continue
            attempted_runs[query_id] = attempted_runs.get(query_id, 0) + 1
            if row.get("success", "").strip().lower() != "true":
                continue
            elapsed_ms = row.get("elapsed_ms", "").strip()
            if not elapsed_ms:
                continue
            values.setdefault(query_id, []).append(float(elapsed_ms))
    return MeasurementInput(label, path, values, attempted_runs)


def build_comparisons(inputs, explicit_comparisons, alpha):
    input_by_label = {item.label: item for item in inputs}
    if len(input_by_label) != len(inputs):
        raise ValueError("System labels must be unique.")

    query_ids = sorted(
        {query_id for item in inputs for query_id in item.attempted_runs},
        key=natural_key,
    )
    candidates = []
    skipped_measurements = []
    skipped_comparisons = []
    seen = set()

    for query_id in query_ids:
        available = []
        for item in inputs:
            run_count = len(item.values.get(query_id, []))
            if run_count >= 2:
                available.append(item)
            elif query_id in item.attempted_runs:
                skipped_measurements.append((query_id, item.label, run_count))
        for left, right in itertools.combinations(available, 2):
            add_candidate(
                candidates,
                seen,
                query_id,
                left.label,
                query_id,
                left.values[query_id],
                right.label,
                query_id,
                right.values[query_id],
            )

    for value in explicit_comparisons:
        left, right = parse_explicit_comparison(value)
        system_a, query_a = left
        system_b, query_b = right
        if system_a not in input_by_label:
            raise ValueError(f"Unknown system label in --comparison: {system_a}")
        if system_b not in input_by_label:
            raise ValueError(f"Unknown system label in --comparison: {system_b}")
        values_a = input_by_label[system_a].values.get(query_a, [])
        values_b = input_by_label[system_b].values.get(query_b, [])
        group = query_a if query_a == query_b else f"{query_a} vs {query_b}"
        if len(values_a) < 2 or len(values_b) < 2:
            skipped_comparisons.append(
                (system_a, query_a, len(values_a), system_b, query_b, len(values_b))
            )
            continue
        add_candidate(
            candidates,
            seen,
            group,
            system_a,
            query_a,
            values_a,
            system_b,
            query_b,
            values_b,
        )

    results = [
        welch_test(group, system_a, query_a, values_a, system_b, query_b, values_b, alpha)
        for group, system_a, query_a, values_a, system_b, query_b, values_b in candidates
    ]
    return results, skipped_measurements, skipped_comparisons


def parse_explicit_comparison(value):
    if "=" not in value:
        raise ValueError("--comparison must use LABEL@QUERY=LABEL@QUERY format.")
    left, right = value.split("=", 1)
    return parse_endpoint(left), parse_endpoint(right)


def parse_endpoint(value):
    if "@" not in value:
        raise ValueError("Comparison endpoints must use LABEL@QUERY format.")
    label, query_id = value.rsplit("@", 1)
    label = label.strip()
    query_id = query_id.strip()
    if not label or not query_id:
        raise ValueError("Comparison labels and query IDs cannot be empty.")
    return label, query_id


def add_candidate(candidates, seen, group, system_a, query_a, values_a, system_b, query_b, values_b):
    endpoint_a = (system_a, query_a)
    endpoint_b = (system_b, query_b)
    key = tuple(sorted((endpoint_a, endpoint_b)))
    if key in seen:
        return
    seen.add(key)
    candidates.append((group, system_a, query_a, values_a, system_b, query_b, values_b))


def summarize(values):
    return SampleSummary(
        len(values),
        statistics.mean(values),
        statistics.stdev(values),
    )


def welch_test(group, system_a, query_a, values_a, system_b, query_b, values_b, alpha):
    sample_a = summarize(values_a)
    sample_b = summarize(values_b)
    difference = sample_a.mean - sample_b.mean
    variance_term_a = sample_a.standard_deviation ** 2 / sample_a.count
    variance_term_b = sample_b.standard_deviation ** 2 / sample_b.count
    standard_error_squared = variance_term_a + variance_term_b

    if standard_error_squared == 0:
        t_statistic = 0.0 if difference == 0 else math.copysign(math.inf, difference)
        degrees_of_freedom = math.inf
        p_value = 1.0 if difference == 0 else 0.0
        margin = 0.0
    else:
        standard_error = math.sqrt(standard_error_squared)
        denominator = (
            variance_term_a ** 2 / (sample_a.count - 1)
            + variance_term_b ** 2 / (sample_b.count - 1)
        )
        degrees_of_freedom = standard_error_squared ** 2 / denominator
        t_statistic = difference / standard_error
        p_value = student_t_two_sided_p(t_statistic, degrees_of_freedom)
        critical_value = student_t_critical_value(alpha, degrees_of_freedom)
        margin = critical_value * standard_error

    return WelchResult(
        group,
        system_a,
        query_a,
        system_b,
        query_b,
        sample_a,
        sample_b,
        difference,
        difference - margin,
        difference + margin,
        t_statistic,
        degrees_of_freedom,
        p_value,
    )


def student_t_two_sided_p(t_statistic, degrees_of_freedom):
    if math.isinf(t_statistic):
        return 0.0
    if math.isinf(degrees_of_freedom):
        normal = statistics.NormalDist()
        return 2 * (1 - normal.cdf(abs(t_statistic)))
    x = degrees_of_freedom / (degrees_of_freedom + t_statistic ** 2)
    return regularized_incomplete_beta(x, degrees_of_freedom / 2, 0.5)


def student_t_critical_value(alpha, degrees_of_freedom):
    if math.isinf(degrees_of_freedom):
        return statistics.NormalDist().inv_cdf(1 - alpha / 2)
    lower = 0.0
    upper = 1.0
    while student_t_two_sided_p(upper, degrees_of_freedom) > alpha:
        upper *= 2
    for _ in range(100):
        midpoint = (lower + upper) / 2
        if student_t_two_sided_p(midpoint, degrees_of_freedom) > alpha:
            lower = midpoint
        else:
            upper = midpoint
    return (lower + upper) / 2


def regularized_incomplete_beta(x, a, b):
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    log_beta_factor = (
        math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        + a * math.log(x)
        + b * math.log1p(-x)
    )
    beta_factor = math.exp(log_beta_factor)
    if x < (a + 1) / (a + b + 2):
        return beta_factor * beta_continued_fraction(a, b, x) / a
    return 1 - beta_factor * beta_continued_fraction(b, a, 1 - x) / b


def beta_continued_fraction(a, b, x):
    maximum_iterations = 200
    epsilon = 3e-14
    minimum = 1e-300
    qab = a + b
    qap = a + 1
    qam = a - 1
    c = 1.0
    d = 1 - qab * x / qap
    if abs(d) < minimum:
        d = minimum
    d = 1 / d
    result = d

    for iteration in range(1, maximum_iterations + 1):
        doubled = 2 * iteration
        numerator = iteration * (b - iteration) * x / ((qam + doubled) * (a + doubled))
        d = 1 + numerator * d
        if abs(d) < minimum:
            d = minimum
        c = 1 + numerator / c
        if abs(c) < minimum:
            c = minimum
        d = 1 / d
        result *= d * c

        numerator = -(a + iteration) * (qab + iteration) * x / (
            (a + doubled) * (qap + doubled)
        )
        d = 1 + numerator * d
        if abs(d) < minimum:
            d = minimum
        c = 1 + numerator / c
        if abs(c) < minimum:
            c = minimum
        d = 1 / d
        delta = d * c
        result *= delta
        if abs(delta - 1) < epsilon:
            return result
    raise ArithmeticError("Incomplete beta calculation did not converge.")


def apply_holm_adjustment(results):
    comparison_count = len(results)
    running_maximum = 0.0
    for rank, result in enumerate(sorted(results, key=lambda item: item.p_value)):
        adjusted = (comparison_count - rank) * result.p_value
        running_maximum = max(running_maximum, adjusted)
        result.adjusted_p_value = min(1.0, running_maximum)


def render_report(title, inputs, results, skipped_measurements, skipped_comparisons, phase, alpha):
    lines = [
        f"# {title}",
        "",
        "> This is an exploratory analysis: each tested group contains only five measurements, "
        "and benchmark execution order was not randomized. The results do not provide definitive "
        "evidence of performance equivalence or difference.",
        "",
        "## Method",
        "",
        f"- Only `{phase}` rows with `success=true` are included; warm-up and failed rows are excluded.",
        "- Tests use the raw `elapsed_ms` measurements rather than summary means.",
        "- Every comparison is a two-sided Welch t-test, which does not assume equal variances.",
        "- Execution order was not randomized, so caching, time trends, or other order effects may influence comparisons.",
        "- The reported difference and unadjusted 95% confidence interval are calculated as system A minus system B.",
        f"- Raw p-values are adjusted together across all {len(results)} comparisons using the Holm method.",
        f"- `Significant` means Holm-adjusted `p <= {alpha:g}`.",
        "- A non-significant result means that these measurements do not establish a difference; it does not prove equal performance.",
        "",
        "## Inputs",
        "",
        "| System | Result file |",
        "|---|---|",
    ]
    for item in inputs:
        lines.append(f"| {markdown(item.label)} | `{item.path.as_posix()}` |")

    lines.extend(["", "## Results", ""])
    if not results:
        lines.append("No comparisons had at least two successful measurements in both groups.")
    else:
        grouped = OrderedDict()
        for result in results:
            grouped.setdefault(result.group, []).append(result)
        for group, group_results in grouped.items():
            lines.extend(
                [
                    f"### {markdown(group)}",
                    "",
                    "| Comparison (A vs B) | n (A/B) | A mean +/- SD (ms) | B mean +/- SD (ms) | Difference A-B (ms) | 95% CI (ms) | t (df) | Raw p | Holm p | Significant |",
                    "|---|---:|---:|---:|---:|---:|---:|---:|---:|:---:|",
                ]
            )
            for result in group_results:
                comparison = comparison_label(result)
                lines.append(
                    "| "
                    + " | ".join(
                        (
                            markdown(comparison),
                            f"{result.sample_a.count}/{result.sample_b.count}",
                            format_mean_sd(result.sample_a),
                            format_mean_sd(result.sample_b),
                            format_number(result.difference),
                            f"[{format_number(result.confidence_low)}, {format_number(result.confidence_high)}]",
                            f"{format_statistic(result.t_statistic)} ({format_statistic(result.degrees_of_freedom)})",
                            format_p_value(result.p_value),
                            format_p_value(result.adjusted_p_value),
                            "Yes" if result.adjusted_p_value <= alpha else "No",
                        )
                    )
                    + " |"
                )
            lines.append("")

    if skipped_measurements or skipped_comparisons:
        lines.extend(["## Skipped Data", ""])
    if skipped_measurements:
        lines.extend(
            [
                "The following system/query groups had fewer than two successful measured runs and were not used in same-query pairwise tests:",
                "",
                "| Query | System | Successful runs |",
                "|---|---|---:|",
            ]
        )
        for query_id, system, run_count in skipped_measurements:
            lines.append(f"| {markdown(query_id)} | {markdown(system)} | {run_count} |")
        lines.append("")
    if skipped_comparisons:
        lines.extend(
            [
                "The following explicitly requested comparisons could not be calculated:",
                "",
                "| Endpoint A | Runs A | Endpoint B | Runs B |",
                "|---|---:|---|---:|",
            ]
        )
        for system_a, query_a, count_a, system_b, query_b, count_b in skipped_comparisons:
            lines.append(
                f"| {markdown(system_a)} [{markdown(query_a)}] | {count_a} | "
                f"{markdown(system_b)} [{markdown(query_b)}] | {count_b} |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def comparison_label(result):
    if result.query_a == result.query_b:
        return f"{result.system_a} vs {result.system_b}"
    return (
        f"{result.system_a} [{result.query_a}] vs "
        f"{result.system_b} [{result.query_b}]"
    )


def format_mean_sd(sample):
    return f"{format_number(sample.mean)} +/- {format_number(sample.standard_deviation)}"


def format_number(value):
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    absolute = abs(value)
    if absolute >= 1000:
        return f"{value:,.1f}"
    if absolute >= 10:
        return f"{value:.2f}"
    return f"{value:.3f}"


def format_statistic(value):
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return f"{value:.3f}"


def format_p_value(value):
    if value == 0:
        return "<1e-300"
    if value < 0.0001:
        return f"{value:.2e}"
    return f"{value:.4f}"


def markdown(value):
    return str(value).replace("|", "\\|")


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


if __name__ == "__main__":
    main()
