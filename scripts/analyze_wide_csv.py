#!/usr/bin/env python3

import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def analyze_csv(input_csv, sample_rows=None, sample_cols=None, output_plot=None):
    """
    - Loads a large "wide" CSV (one row per station, many columns).
    - Computes basic coverage stats for each column (how many non-null values).
    - Optionally samples the DataFrame to a smaller subset of rows and columns.
    - Plots a heatmap of missingness for quick visualization.
    - If output_plot is given, saves the figure to file (PNG); otherwise shows interactively.
    """
    print(f"Loading CSV: {input_csv}")
    df = pd.read_csv(input_csv, low_memory=False)  # Attempt to reduce memory overhead

    print(f"Data shape: {df.shape[0]} rows, {df.shape[1]} columns")

    # ---- COVERAGE STATS ----
    # Count how many non-null entries each column has
    nonnull_counts = df.notnull().sum()  # For each column
    coverage_pct = (nonnull_counts / len(df)) * 100  # Convert to percentage
    coverage_stats = coverage_pct.sort_values(ascending=False)

    print("\nTop 10 columns by coverage (percentage of non-null entries):")
    print(coverage_stats.head(10).round(2))
    print("\nBottom 10 columns by coverage:")
    print(coverage_stats.tail(10).round(2))

    # If the dataset is huge, sampling can help us visualize
    # (We don't necessarily want a 15k x 2k heatmap.)
    df_sample = df
    if sample_rows is not None and sample_rows < len(df_sample):
        df_sample = df_sample.sample(n=sample_rows, random_state=42)

    if sample_cols is not None and sample_cols < len(df_sample.columns):
        df_sample = df_sample.sample(n=sample_cols, axis="columns", random_state=42)

    # ---- HEATMAP OF MISSINGNESS ----
    print(f"\nGenerating heatmap on a sample of shape {df_sample.shape} ...")
    sns.set(style="whitegrid", font_scale=0.8)

    plt.figure(figsize=(12, 6))
    # The heatmap: True for missing, False for present
    # We'll display missingness as 1 (missing) or 0 (not missing)
    sns.heatmap(
        df_sample.isnull(),
        cbar=False,
        cmap=["#2e7d32", "#e53935"],  # green for not null, red for missing
    )
    plt.title("Missingness Heatmap (sampled)")

    if output_plot:
        plt.savefig(output_plot, dpi=150, bbox_inches="tight")
        print(f"Heatmap saved to {output_plot}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze a large wide CSV (single-row station files merged)."
    )
    parser.add_argument("input_csv", help="Path to the combined CSV file.")
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=None,
        help="Randomly sample this many rows for the heatmap (optional).",
    )
    parser.add_argument(
        "--sample-cols",
        type=int,
        default=None,
        help="Randomly sample this many columns for the heatmap (optional).",
    )
    parser.add_argument(
        "--output-plot",
        default=None,
        help="If provided, save the heatmap to this file (e.g., 'heatmap.png').",
    )
    args = parser.parse_args()

    analyze_csv(
        input_csv=args.input_csv,
        sample_rows=args.sample_rows,
        sample_cols=args.sample_cols,
        output_plot=args.output_plot,
    )


if __name__ == "__main__":
    main()
