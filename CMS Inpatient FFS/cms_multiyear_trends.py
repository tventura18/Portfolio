"""
CMS Medicare Inpatient FFS - Multi-Year Trend Analysis
Data: Medicare Inpatient Hospitals by Provider and Service (2019-2023)
Author: Toni
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

# Okabe-Ito colorblind safe palette
COLORS = {
    "blue":   "#0072B2",
    "orange": "#E69F00",
    "green":  "#009E73",
    "red":    "#D55E00",
    "purple": "#CC79A7",
}
YEAR_COLORS = [COLORS["blue"], COLORS["orange"], COLORS["green"], 
               COLORS["red"], COLORS["purple"]]

# ── Load from PostgreSQL ───────────────────────────────────────────────────────
def load_data(config):
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(conn_str)
    print("Loading multi-year data from PostgreSQL...")
    df = pd.read_sql("SELECT * FROM cms_inpatient_multiyear", engine)
    print(f"Loaded {len(df):,} rows across {df['data_year'].nunique()} years")
    print(f"Years: {sorted(df['data_year'].unique())}")
    return df


# ── 1. YEARLY SUMMARY ─────────────────────────────────────────────────────────
def yearly_summary(df):
    print("\n" + "="*60)
    print("YEARLY SUMMARY STATISTICS")
    print("="*60)

    summary = (
        df.groupby("data_year")
        .agg(
            total_rows          = ("total_discharges", "count"),
            total_discharges    = ("total_discharges", "sum"),
            unique_providers    = ("provider_ccn", "nunique"),
            avg_medicare_pmt    = ("avg_medicare_payment", "mean"),
            avg_submitted_charge= ("avg_submitted_charge", "mean"),
            avg_ctp_ratio       = ("charge_to_payment_ratio", "mean"),
            avg_medicare_pct    = ("medicare_pct_of_total", "mean"),
        )
        .round(2)
    )
    print(summary.to_string())
    return summary


# ── 2. RUCA TREND ─────────────────────────────────────────────────────────────
def ruca_trend(df):
    print("\n" + "="*60)
    print("PAYMENT TREND BY RUCA CATEGORY")
    print("="*60)

    trend = (
        df.groupby(["data_year", "ruca_category"])
        .agg(
            avg_medicare_pmt = ("avg_medicare_payment", "mean"),
            avg_ctp_ratio    = ("charge_to_payment_ratio", "mean"),
            total_discharges = ("total_discharges", "sum"),
        )
        .round(2)
        .reset_index()
    )
    print(trend.to_string())
    return trend


# ── 3. TOP DRG TREND ──────────────────────────────────────────────────────────
def top_drg_trend(df, top_n=5):
    print("\n" + "="*60)
    print(f"TOP {top_n} DRGs PAYMENT TREND OVER TIME")
    print("="*60)

    # Get top DRGs by total discharge volume across all years
    top_drgs = (
        df.groupby("drg_code")["total_discharges"]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
        .index.tolist()
    )

    drg_trend = (
        df[df["drg_code"].isin(top_drgs)]
        .groupby(["data_year", "drg_code", "drg_desc"])
        .agg(
            avg_medicare_pmt = ("avg_medicare_payment", "mean"),
            total_discharges = ("total_discharges", "sum"),
        )
        .round(2)
        .reset_index()
    )
    print(drg_trend.to_string())
    return drg_trend, top_drgs


# ── 4. STATE TREND ────────────────────────────────────────────────────────────
def state_ctp_trend(df):
    print("\n" + "="*60)
    print("CHARGE-TO-PAYMENT RATIO TREND BY STATE (TOP 10 STATES)")
    print("="*60)

    # Get top 10 states by avg CTP in 2023
    top_states = (
        df[df["data_year"] == 2023]
        .groupby("Rndrng_Prvdr_State_Abrvtn")["charge_to_payment_ratio"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
        .index.tolist()
    )

    state_trend = (
        df[df["Rndrng_Prvdr_State_Abrvtn"].isin(top_states)]
        .groupby(["data_year", "Rndrng_Prvdr_State_Abrvtn"])
        .agg(avg_ctp_ratio=("charge_to_payment_ratio", "mean"))
        .round(2)
        .reset_index()
    )
    print(state_trend.to_string())
    return state_trend, top_states


# ── 5. CREATE TREND CHARTS ────────────────────────────────────────────────────
def create_trend_charts(df, yearly_stats, ruca_trend_df, drg_trend_df, 
                        state_trend_df, top_states):
    sns.set_theme(style="whitegrid")
    fig = plt.figure(figsize=(22, 18))
    fig.suptitle("CMS Medicare Inpatient FFS 2019–2023 — Multi-Year Trend Analysis",
                 fontsize=16, fontweight="bold", y=0.99)
    gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.55, wspace=0.4)

    years = sorted(df["data_year"].unique())

    # ── Chart 1: Total Discharges by Year ─────────────────────────────────────
    ax1 = fig.add_subplot(gs[0, 0])
    bars = ax1.bar(yearly_stats.index, yearly_stats["total_discharges"] / 1_000_000,
                   color=YEAR_COLORS, edgecolor="white")
    for bar, val in zip(bars, yearly_stats["total_discharges"]):
        ax1.text(bar.get_x() + bar.get_width()/2,
                 bar.get_height() * 0.95,
                 f"{val/1e6:.2f}M", ha="center", va="top",
                 color="white", fontsize=8, fontweight="bold")
    ax1.set_title("Total Discharges by Year", fontsize=10, fontweight="bold")
    ax1.set_ylabel("Total Discharges (Millions)")
    ax1.set_xlabel("")

    # ── Chart 2: Avg Medicare Payment Trend ───────────────────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.plot(yearly_stats.index, yearly_stats["avg_medicare_pmt"],
             marker="o", color=COLORS["blue"], linewidth=2, markersize=8)
    for x, y in zip(yearly_stats.index, yearly_stats["avg_medicare_pmt"]):
        ax2.annotate(f"${y:,.0f}", (x, y),
                     textcoords="offset points", xytext=(0, 10),
                     ha="center", fontsize=8)
    ax2.set_title("Avg Medicare Payment Trend", fontsize=10, fontweight="bold")
    ax2.set_ylabel("Avg Medicare Payment ($)")
    ax2.set_xlabel("")
    ax2.set_xticks(years)

    # ── Chart 3: Avg CTP Ratio Trend ──────────────────────────────────────────
    ax3 = fig.add_subplot(gs[0, 2])
    ax3.plot(yearly_stats.index, yearly_stats["avg_ctp_ratio"],
             marker="o", color=COLORS["red"], linewidth=2, markersize=8)
    for x, y in zip(yearly_stats.index, yearly_stats["avg_ctp_ratio"]):
        ax3.annotate(f"{y:.2f}x", (x, y),
                     textcoords="offset points", xytext=(0, 10),
                     ha="center", fontsize=8)
    ax3.set_title("Avg Charge-to-Payment Ratio Trend", fontsize=10, fontweight="bold")
    ax3.set_ylabel("Avg CTP Ratio")
    ax3.set_xlabel("")
    ax3.set_xticks(years)

    # ── Chart 4: Medicare Payment by RUCA over time ───────────────────────────
    ax4 = fig.add_subplot(gs[1, 0:2])
    ruca_cats = ["Urban", "Suburban", "Rural"]
    ruca_colors = [COLORS["blue"], COLORS["orange"], COLORS["green"]]
    for cat, color in zip(ruca_cats, ruca_colors):
        subset = ruca_trend_df[ruca_trend_df["ruca_category"] == cat]
        ax4.plot(subset["data_year"], subset["avg_medicare_pmt"],
                 marker="o", label=cat, color=color, linewidth=2, markersize=7)
    ax4.set_title("Avg Medicare Payment by Location — 2019 to 2023",
                  fontsize=10, fontweight="bold")
    ax4.set_ylabel("Avg Medicare Payment ($)")
    ax4.set_xlabel("")
    ax4.legend(fontsize=9)
    ax4.set_xticks(years)
    ax4.grid(True, alpha=0.3)

    # ── Chart 5: CTP Ratio by RUCA over time ──────────────────────────────────
    ax5 = fig.add_subplot(gs[1, 2])
    for cat, color in zip(ruca_cats, ruca_colors):
        subset = ruca_trend_df[ruca_trend_df["ruca_category"] == cat]
        ax5.plot(subset["data_year"], subset["avg_ctp_ratio"],
                 marker="o", label=cat, color=color, linewidth=2, markersize=7)
    ax5.set_title("Avg CTP Ratio by Location\n2019 to 2023",
                  fontsize=10, fontweight="bold")
    ax5.set_ylabel("Avg CTP Ratio")
    ax5.set_xlabel("")
    ax5.legend(fontsize=9)
    ax5.set_xticks(years)
    ax5.grid(True, alpha=0.3)

    # ── Chart 6: Top DRG payment trends ───────────────────────────────────────
    ax6 = fig.add_subplot(gs[2, 0:2])
    drg_colors = [COLORS["blue"], COLORS["orange"], COLORS["green"],
                  COLORS["red"], COLORS["purple"]]
    for i, (drg_code, group) in enumerate(drg_trend_df.groupby("drg_code")):
        desc = group["drg_desc"].iloc[0][:30] + "..."
        ax6.plot(group["data_year"], group["avg_medicare_pmt"],
                 marker="o", label=f"{drg_code}: {desc}",
                 color=drg_colors[i % len(drg_colors)],
                 linewidth=2, markersize=7)
    ax6.set_title("Avg Medicare Payment — Top 5 DRGs by Volume (2019–2023)",
                  fontsize=10, fontweight="bold")
    ax6.set_ylabel("Avg Medicare Payment ($)")
    ax6.set_xlabel("")
    ax6.legend(fontsize=7, loc="upper left")
    ax6.set_xticks(years)
    ax6.grid(True, alpha=0.3)

    # ── Chart 7: State CTP ratio heatmap ──────────────────────────────────────
    ax7 = fig.add_subplot(gs[2, 2])
    pivot = state_trend_df.pivot(
        index="Rndrng_Prvdr_State_Abrvtn",
        columns="data_year",
        values="avg_ctp_ratio"
    )
    sns.heatmap(pivot, ax=ax7, cmap="RdYlGn_r", annot=True, fmt=".1f",
                linewidths=0.5, cbar_kws={"label": "Avg CTP Ratio"},
                annot_kws={"size": 7})
    
    ax7.set_yticklabels(ax7.get_yticklabels(), rotation=0, fontsize=7)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    
    ax7.set_title("CTP Ratio Heatmap\nTop 10 States by 2023 CTP",
                  fontsize=10, fontweight="bold")
    ax7.set_xlabel("")
    ax7.set_ylabel("")
    

    plt.savefig(OUTPUT_DIR + "cms_multiyear_trends.png", dpi=150, bbox_inches="tight")
    print(f"\nTrend charts saved to {OUTPUT_DIR}cms_multiyear_trends.png")
    plt.show()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df                          = load_data(DB_CONFIG)
    yearly_stats                = yearly_summary(df)
    ruca_trend_df               = ruca_trend(df)
    drg_trend_df, top_drgs      = top_drg_trend(df, top_n=5)
    state_trend_df, top_states  = state_ctp_trend(df)
    create_trend_charts(df, yearly_stats, ruca_trend_df, drg_trend_df,
                        state_trend_df, top_states)

    print("\n── Done! ────────────────────────────────────────────────")
