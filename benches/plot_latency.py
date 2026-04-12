#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["matplotlib"]
# ///
"""
Generate latency percentile chart from memtier_benchmark HDR histogram output.
Reproduces the chart from https://hdrhistogram.github.io/HdrHistogram/plotFiles.html

Usage:
    uv run benches/plot_latency.py [--output FILE] FILE1 [FILE2 ...]
    # or just: ./benches/plot_latency.py FILE1 [FILE2 ...]

Each FILE should be a .txt percentile distribution file produced by
memtier_benchmark --hdr-file-prefix. The format is:
    Value   Percentile   TotalCount   1/(1-Percentile)
"""

import argparse
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def parse_hdr_file(path):
    """Parse a memtier HDR percentile .txt file, return (percentiles, values)."""
    percentiles = []
    values = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("Value"):
                continue
            parts = line.split()
            if len(parts) < 4:
                continue
            try:
                value = float(parts[0])
                percentile = float(parts[1])
                if percentile >= 1.0:
                    continue
                percentiles.append(percentile)
                values.append(value)
            except ValueError:
                continue
    return percentiles, values


def label_from_path(path):
    """Derive a legend label from the file path."""
    base = os.path.basename(path)
    name = os.path.splitext(base)[0]
    for suffix in ("_FULL_RUN_1", "_GET_command_run_1", "_SET_command_run_1"):
        name = name.replace(suffix, "")
    return name


def main():
    parser = argparse.ArgumentParser(description="Plot HDR latency percentile chart")
    parser.add_argument("files", nargs="+", help="HDR percentile .txt files")
    parser.add_argument(
        "-o",
        "--output",
        default="assets/latency.png",
        help="Output image path (default: assets/latency.png)",
    )
    parser.add_argument(
        "--title", default="Latency by Percentile Distribution", help="Chart title"
    )
    args = parser.parse_args()

    fig, ax = plt.subplots(figsize=(12, 6))

    for path in args.files:
        percentiles, values = parse_hdr_file(path)
        if not percentiles:
            print(f"Warning: no data in {path}", file=sys.stderr)
            continue
        # X-axis: 1/(1-percentile), log scale — same as the HdrHistogram website
        inv = [1.0 / (1.0 - p) for p in percentiles if p < 1.0]
        values = values[: len(inv)]
        ax.plot(inv, values, linewidth=1.5, label=label_from_path(path))

    ax.set_xscale("log")
    ax.set_xlabel("Percentile (%)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title(args.title)

    # Custom x-axis labels showing percentiles instead of raw 1/(1-p) values
    percentile_ticks = [1, 10, 100, 1000, 10000, 100000]
    percentile_labels = [
        "0%",
        "90%",
        "99%",
        "99.9%",
        "99.99%",
        "99.999%",
    ]
    ax.set_xticks(percentile_ticks)
    ax.set_xticklabels(percentile_labels)
    ax.set_xticks([], minor=True)
    ax.set_xlim(left=1)
    ax.set_ylim(bottom=0)

    ax.grid(False)
    ax.legend()

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    fig.savefig(args.output, dpi=150, bbox_inches="tight")
    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
