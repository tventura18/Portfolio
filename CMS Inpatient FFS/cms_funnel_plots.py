"""
CMS Medicare Inpatient FFS - Funnel Plot Analysis
Replaces I-Chart for Medicare Payment with Funnel Plot for CTP Ratio by State
Author: Toni

Funnel plots are the gold standard for comparing rates across providers/states
with different sample sizes. They show:
    - Expected variation based on sample size
    - 95% and 99.8% confidence bands
    - True statistical outliers vs small-sample noise
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "cms_analytics",
    "user":     "toniventura",
    "password": "cms2023"
}

OUTPUT_DIR = "output/"
import os
os.makedirs(OUTPUT_DIR, exist_ok=True)

CTP_THRESHOLD = 10  # high risk threshold

# Okabe-Ito colorblind safe palette
COLORS = {
    "blue":   "#0072B2",
    "orange": "#E69F00",
    "green":  "#009E73",
    "red":    "#D55E00",
    "purple": "#CC79A7",
    "grey":   "#999999",
}

YEAR_COLORS = {
    2019: "#0072B2",
    2020: "#E69F00",
    2021: "#009E73",
    2022: "#D55E00",
    2023: "#CC79A7",
}

# ── Load from PostgreSQL ───────────────────────────────────────────────────────
def load_data(config):
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(conn_str)
    print("Loading multi-year data from PostgreSQL...")
    df = pd.read_sql("SELECT * FROM cms_inpatient_multiyear", engine)
    print(f"Loaded {len(df):,} rows")
    return df


# ── Aggregate by State and Year ───────────────────────────────────────────────
def aggregate_by_state_year(df):
    # Top 25 states by discharge volume for line charts
    top_25_states = (
        df.groupby("Rndrng_Prvdr_State_Abrvtn")["total_discharges"]
        .sum()
        .sort_values(ascending=False)
        .head(25)
        .index.tolist()
    )

    state_year = (
        df.groupby(["data_year", "Rndrng_Prvdr_State_Abrvtn"])
        .agg(
            total_cases      = ("total_discharges", "sum"),
            avg_ctp          = ("charge_to_payment_ratio", "mean"),
            avg_medicare_pmt = ("avg_medicare_payment", "mean"),
            high_risk_cases  = ("charge_to_payment_ratio",
                                lambda x: (x > CTP_THRESHOLD).sum()),
            n_drgs           = ("drg_code", "nunique"),
        )
        .reset_index()
        .rename(columns={"Rndrng_Prvdr_State_Abrvtn": "state"})
    )
    state_year = state_year[state_year["total_cases"] > 1000]

    # All valid states (present in all 5 years)
    state_counts = state_year.groupby("state")["data_year"].count()
    valid_states = sorted(state_counts[state_counts == 5].index.tolist())

    state_year = state_year[state_year["state"].isin(valid_states)]
    state_year = state_year.sort_values(["data_year", "state"])

    print(f"Total valid states: {len(valid_states)}")
    print(f"Top 25 states by volume: {top_25_states[:5]}... etc")

    return state_year, top_25_states, valid_states


# ── Funnel Plot Builder ───────────────────────────────────────────────────────
def build_funnel_plot(ax, state_data, metric, n_col, title, ylabel,
                      color=None, year=None, show_labels=True):
    """
    Build a funnel plot for a given metric vs sample size.
    
    Parameters:
        state_data : DataFrame with state-level aggregated data
        metric     : column name for the rate/ratio being plotted
        n_col      : column name for the sample size (denominator)
        title      : chart title
        ylabel     : y-axis label
        color      : dot color
        year       : year label for legend
        show_labels: whether to label outlier points
    """
    values = state_data[metric].values
    n      = state_data[n_col].values
    states = state_data["state"].values

    # Overall mean (weighted by sample size)
    theta  = np.average(values, weights=n)

    # Generate funnel bands across range of n
    n_range = np.linspace(n.min(), n.max(), 500)

    # For ratio metrics — use normal approximation
    # sigma = std dev of the metric / sqrt(n) scaled
    pooled_std = np.std(values)

    # 95% limits (±1.96 sigma)
    ucl_95 = theta + 1.96 * (pooled_std / np.sqrt(n_range / n_range.mean()))
    lcl_95 = theta - 1.96 * (pooled_std / np.sqrt(n_range / n_range.mean()))

    # 99.8% limits (±3.09 sigma)
    ucl_998 = theta + 3.09 * (pooled_std / np.sqrt(n_range / n_range.mean()))
    lcl_998 = theta - 3.09 * (pooled_std / np.sqrt(n_range / n_range.mean()))

    # Floor at 0
    lcl_95  = np.maximum(lcl_95, 0)
    lcl_998 = np.maximum(lcl_998, 0)

    # ── Plot funnel bands ─────────────────────────────────────────────────────
    ax.fill_between(n_range, lcl_998, ucl_998,
                    alpha=0.10, color=COLORS["grey"], label="99.8% limits")
    ax.fill_between(n_range, lcl_95, ucl_95,
                    alpha=0.20, color=COLORS["grey"], label="95% limits")
    ax.axhline(theta, color="green", linestyle="-", linewidth=1.5,
               label=f"Overall mean: {theta:.2f}")

    # ── Plot state points ─────────────────────────────────────────────────────
    dot_color = color or COLORS["blue"]
    ax.scatter(n, values, color=dot_color, alpha=0.6, s=40, zorder=5)

    # ── Flag and label outliers ───────────────────────────────────────────────
    outliers_high = []
    outliers_low  = []

    for i, (ni, vi, state) in enumerate(zip(n, values, states)):
        # Compute limit at this specific n
        scale   = pooled_std / np.sqrt(ni / n.mean())
        ucl_i   = theta + 3.09 * scale
        lcl_i   = max(theta - 3.09 * scale, 0)

        if vi > ucl_i:
            outliers_high.append((ni, vi, state))
            ax.scatter(ni, vi, color=COLORS["red"], s=80, zorder=6,
                       marker="*", label="_nolegend_")
            if show_labels:
                ax.annotate(state, (ni, vi),
                            textcoords="offset points",
                            xytext=(5, 4), fontsize=7,
                            color=COLORS["red"], fontweight="bold")
        elif vi < lcl_i and lcl_i > 0:
            outliers_low.append((ni, vi, state))
            ax.scatter(ni, vi, color=COLORS["orange"], s=80, zorder=6,
                       marker="v", label="_nolegend_")
            if show_labels:
                ax.annotate(state, (ni, vi),
                            textcoords="offset points",
                            xytext=(5, -10), fontsize=7,
                            color=COLORS["orange"], fontweight="bold")

    print(f"\n{title}")
    print(f"  Overall mean: {theta:.3f}")
    print(f"  High outliers: {[s for _, _, s in outliers_high]}")
    print(f"  Low outliers:  {[s for _, _, s in outliers_low]}")

    ax.set_title(title, fontsize=10, fontweight="bold")
    ax.set_xlabel("Total Discharge Volume (Sample Size)", fontsize=9)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.grid(True, alpha=0.3)

    return outliers_high, outliers_low


# ── Multi-Year Funnel Plots ───────────────────────────────────────────────────
def create_funnel_charts(state_year):
    years = sorted(state_year["data_year"].unique())

    fig = plt.figure(figsize=(22, 20))
    fig.suptitle(
        "CMS Medicare Inpatient FFS 2019–2023 — Funnel Plot Analysis\n"
        "Charge-to-Payment Ratio & High-Risk Proportion by State",
        fontsize=14, fontweight="bold", y=0.99
    )
    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.55, wspace=0.35)

    # ── Row 1-2: One funnel plot per year for CTP ratio ───────────────────────
    positions = [(0, 0), (0, 1), (1, 0), (1, 1), (2, 0)]

    all_outliers = {}
    for i, year in enumerate(years):
        row, col = positions[i]
        ax = fig.add_subplot(gs[row, col])
        year_data = state_year[state_year["data_year"] == year]

        outliers_high, outliers_low = build_funnel_plot(
            ax=ax,
            state_data=year_data,
            metric="avg_ctp",
            n_col="total_cases",
            title=f"Funnel Plot — Avg CTP Ratio by State ({year})",
            ylabel="Avg Charge-to-Payment Ratio",
            color=YEAR_COLORS[year],
            year=year
        )
        all_outliers[year] = {
            "high": [s for _, _, s in outliers_high],
            "low":  [s for _, _, s in outliers_low]
        }

        # Add legend only on first chart
        if i == 0:
            ax.legend(fontsize=7, loc="upper right")

    # ── Row 3 right: Outlier persistence summary ──────────────────────────────
    ax_summary = fig.add_subplot(gs[2, 1])

    # Count how many years each state was a high outlier
    outlier_counts = {}
    for year, data in all_outliers.items():
        for state in data["high"]:
            outlier_counts[state] = outlier_counts.get(state, 0) + 1

    if outlier_counts:
        states_sorted = sorted(outlier_counts.items(),
                               key=lambda x: x[1], reverse=True)
        states_list   = [s for s, _ in states_sorted]
        counts_list   = [c for _, c in states_sorted]

        bars = ax_summary.barh(states_list, counts_list,
                               color=COLORS["red"], edgecolor="white")
        for bar, val in zip(bars, counts_list):
            ax_summary.text(
                bar.get_width() * 0.95,
                bar.get_y() + bar.get_height() / 2,
                f"{val}/5 years", va="center", ha="right",
                color="white", fontsize=8, fontweight="bold"
            )
        ax_summary.set_title(
            "Persistent Outliers — States Above\n99.8% Funnel Limit (CTP Ratio)",
            fontsize=10, fontweight="bold"
        )
        ax_summary.set_xlabel("Number of Years Above Upper Limit")
        ax_summary.invert_yaxis()
        ax_summary.set_xlim(0, 5.5)
        ax_summary.set_xticks(range(6))
        ax_summary.grid(True, alpha=0.3, axis="x")
    else:
        ax_summary.text(0.5, 0.5, "No persistent outliers detected",
                        ha="center", va="center", transform=ax_summary.transAxes)
        ax_summary.set_title("Persistent Outliers", fontsize=10, fontweight="bold")

    plt.savefig(OUTPUT_DIR + "cms_funnel_plots.png", dpi=150, bbox_inches="tight")
    print(f"\nFunnel plots saved to {OUTPUT_DIR}cms_funnel_plots.png")
    plt.show()

    return all_outliers


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df                              = load_data(DB_CONFIG)
    state_year, top_25, valid_states = aggregate_by_state_year(df)

    print("\n" + "="*60)
    print("FUNNEL PLOT ANALYSIS — CTP RATIO BY STATE")
    print("="*60)

    all_outliers = create_funnel_charts(state_year)

    print("\n── Outlier Summary by Year ──────────────────────────────────")
    for year, data in all_outliers.items():
        print(f"{year}: High={data['high']}  Low={data['low']}")

    print("\n── Done! ────────────────────────────────────────────────")
