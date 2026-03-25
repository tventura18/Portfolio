"""
CMS Medicare Inpatient FFS - Descriptive Statistics
Data: Medicare Inpatient Hospitals by Provider and Service (2023)
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
import textwrap

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "cms_analytics",
    "user":     "toniventura",
    "password": "cms2023"
}

OUTPUT_DIR = "output/"  # folder to save charts
import os
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Load from PostgreSQL ───────────────────────────────────────────────────────
def load_data(config):
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(conn_str)
    print("Loading data from PostgreSQL...")
    df = pd.read_sql("SELECT * FROM cms_inpatient_2023", engine)
    print(f"Loaded {len(df):,} rows")
    return df

# ── 1. OVERALL SUMMARY STATS ──────────────────────────────────────────────────
def overall_summary(df):
    print("\n" + "="*60)
    print("OVERALL SUMMARY STATISTICS")
    print("="*60)

    cols = [
        "avg_submitted_charge",
        "avg_total_payment",
        "avg_medicare_payment",
        "charge_to_payment_ratio",
        "medicare_pct_of_total",
        "total_discharges"
    ]

    summary = df[cols].describe(percentiles=[.25, .5, .75, .90, .95]).T
    summary["cv"] = (df[cols].std() / df[cols].mean() * 100).round(2)  # coefficient of variation
    print(summary.round(2).to_string())
    return summary

# ── 2. STATS BY RUCA CATEGORY (Urban/Suburban/Rural) ─────────────────────────
def stats_by_ruca(df):
    print("\n" + "="*60)
    print("AVERAGE PAYMENTS BY RURAL/URBAN CATEGORY")
    print("="*60)

    ruca_stats = df.groupby("ruca_category").agg(
        #providers        = ("provider_ccn", "nunique"),
        providers = ("Rndrng_Prvdr_CCN", "nunique"),
        total_discharges = ("total_discharges", "sum"),
        avg_charge       = ("avg_submitted_charge", "mean"),
        avg_medicare_pmt = ("avg_medicare_payment", "mean"),
        avg_ctp_ratio    = ("charge_to_payment_ratio", "mean"),
        avg_medicare_pct = ("medicare_pct_of_total", "mean")
    ).round(2)

    print(ruca_stats.to_string())
    return ruca_stats

# ── 3. TOP 10 DRGs BY VOLUME ──────────────────────────────────────────────────
def top_drgs_by_volume(df):
    print("\n" + "="*60)
    print("TOP 10 DRGs BY TOTAL DISCHARGES")
    print("="*60)

    top_drgs = (
        df.groupby(["drg_code", "drg_desc"])
        .agg(
            total_discharges  = ("total_discharges", "sum"),
            avg_charge        = ("avg_submitted_charge", "mean"),
            avg_medicare_pmt  = ("avg_medicare_payment", "mean"),
            avg_ctp_ratio     = ("charge_to_payment_ratio", "mean")
        )
        .sort_values("total_discharges", ascending=False)
        .head(10)
        .round(2)
    )
    print(top_drgs.to_string())
    return top_drgs

# ── 4. TOP 10 STATES BY AVG CHARGE-TO-PAYMENT RATIO ─────────────────────────
def top_states_by_ctp(df):
    print("\n" + "="*60)
    print("TOP 10 STATES BY AVG CHARGE-TO-PAYMENT RATIO")
    print("="*60)

    state_stats = (
        df.groupby("Rndrng_Prvdr_State_Abrvtn")
        .agg(
            providers        = ("Rndrng_Prvdr_State_Abrvtn", "nunique"),
            total_discharges = ("total_discharges", "sum"),
            avg_ctp_ratio    = ("charge_to_payment_ratio", "mean"),
            avg_medicare_pmt = ("avg_medicare_payment", "mean")
        )
        .sort_values("avg_ctp_ratio", ascending=False)
        .head(10)
        .round(2)
    )
    print(state_stats.to_string())
    return state_stats

# ── 5. OUTLIER DETECTION (Z-SCORE) ───────────────────────────────────────────
def flag_outliers(df):
    print("\n" + "="*60)
    print("OUTLIER DETECTION - CHARGE-TO-PAYMENT RATIO (Z-SCORE > 3)")
    print("="*60)

    mean = df["charge_to_payment_ratio"].mean()
    std  = df["charge_to_payment_ratio"].std()
    df["ctp_zscore"] = ((df["charge_to_payment_ratio"] - mean) / std).round(4)
    outliers = df[df["ctp_zscore"].abs() > 3][
        ["provider_name", "Rndrng_Prvdr_State_Abrvtn", "drg_code", "drg_desc",
         "charge_to_payment_ratio", "ctp_zscore", "total_discharges"]
    ].sort_values("ctp_zscore", ascending=False)

    print(f"Total outliers flagged: {len(outliers):,}")
    print(outliers.head(10).to_string())
    return df, outliers

# ── 6. VISUALIZATIONS ─────────────────────────────────────────────────────────
def create_charts(df, ruca_stats, top_drgs, state_stats):
    sns.set_theme(style="whitegrid", palette="muted")
    
    #fig.suptitle("CMS Medicare Inpatient FFS 2023 — Descriptive Analysis", 
    #             fontsize=16, fontweight="bold", y=0.98)
    #fig.suptitle("CMS Medicare Inpatient FFS 2023 — Descriptive Analysis",
    #         fontsize=16, fontweight="bold", y=1.02)
    
    #gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.35)
    fig = plt.figure(figsize=(18, 14))

    #fig.suptitle(
    #    "CMS Medicare Inpatient FFS 2023 — Descriptive Analysis",
    #    fontsize=16,
    #    fontweight="bold",
    #    y=0.98
    #)

    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.45)

    # ── Chart 1: Distribution of charge-to-payment ratio ─────────────────────
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.hist(df["charge_to_payment_ratio"].clip(0, 30), bins=60, color="#4C72B0", edgecolor="white")
    ax1.axvline(df["charge_to_payment_ratio"].mean(), color="red", linestyle="--", label=f"Mean: {df['charge_to_payment_ratio'].mean():.2f}")
    ax1.axvline(df["charge_to_payment_ratio"].median(), color="orange", linestyle="--", label=f"Median: {df['charge_to_payment_ratio'].median():.2f}")
    ax1.set_title("Charge-to-Payment Ratio Distribution")
    ax1.set_xlabel("Ratio")
    ax1.set_ylabel("Frequency")
    ax1.legend(fontsize=8)

    # ── Chart 2: Avg Medicare Payment by RUCA Category ───────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    ruca_plot = ruca_stats["avg_medicare_pmt"].sort_values()
    bars = ax2.barh(ruca_plot.index, ruca_plot.values, color=["#55A868", "#C44E52", "#4C72B0"])
    ax2.set_title("Avg Medicare Payment by Location")
    ax2.set_xlabel("Avg Medicare Payment ($)")
    #for bar, val in zip(bars, ruca_plot.values):
    #    ax2.text(val + 50, bar.get_y() + bar.get_height()/2, f"${val:,.0f}", va="center", fontsize=8)
    for bar, val in zip(bars, ruca_plot.values):
        ax2.text(
            bar.get_width() * 0.98,
            bar.get_y() + bar.get_height() / 2,
            f"${val:,.0f}",
            va="center", ha="right",
            color="white", fontsize=8, fontweight="bold"
    )
    # ── Chart 3: Top 10 DRGs by Volume ───────────────────────────────────────
    ax3 = fig.add_subplot(gs[0, 2])
    #drg_labels = [d[:30] + "..." if len(d) > 30 else d for d in top_drgs.index.get_level_values("drg_desc")]
    drg_labels = [d[:20] + "..." if len(d) > 20 else d for d in top_drgs.index.get_level_values("drg_desc")]
    #drg_labels = ["\n".join(textwrap.wrap(d, width=25)) for d in top_drgs.index.get_level_values("drg_desc")]
    #drg_labels = ["\n".join(textwrap.wrap(d, width=18)) for d in top_drgs.index.get_level_values("drg_desc")]
    drg_codes = top_drgs.index.get_level_values("drg_code")
    drg_descs = top_drgs.index.get_level_values("drg_desc")
    drg_labels = [f"{code}: {desc[:22]}..." if len(desc) > 22 else f"{code}: {desc}" 
              for code, desc in zip(drg_codes, drg_descs)]
    
    ax3.barh(drg_labels, top_drgs["total_discharges"], color="#4C72B0")
    ax3.set_title("Top 10 DRGs by Discharge Volume")
    ax3.set_xlabel("Total Discharges")
    ax3.invert_yaxis()
    #ax3.tick_params(axis="y", labelsize=7)
    ax3.tick_params(axis="y", labelsize=6.5)

    # ── Chart 4: Avg Charge vs Avg Medicare Payment scatterplot ──────────────
    ax4 = fig.add_subplot(gs[1, 0])
    sample = df.sample(min(5000, len(df)), random_state=42)
    scatter = ax4.scatter(
        sample["avg_submitted_charge"],
        sample["avg_medicare_payment"],
        c=sample["charge_to_payment_ratio"],
        cmap="RdYlGn_r", alpha=0.4, s=10
    )
    plt.colorbar(scatter, ax=ax4, label="CTP Ratio")
    ax4.set_title("Submitted Charge vs Medicare Payment")
    ax4.set_xlabel("Avg Submitted Charge ($)")
    ax4.set_ylabel("Avg Medicare Payment ($)")

    # ── Chart 5: Top 10 States by Avg CTP Ratio ──────────────────────────────
    ax5 = fig.add_subplot(gs[1, 1])
    ax5.barh(state_stats.index, state_stats["avg_ctp_ratio"], color="#C44E52")
    ax5.set_title("Top 10 States by Avg Charge-to-Payment Ratio")
    ax5.set_xlabel("Avg CTP Ratio")
    ax5.invert_yaxis()

    # ── Chart 6: Medicare % of Total Payment by RUCA ─────────────────────────
    ax6 = fig.add_subplot(gs[1, 2])
    ruca_order = ["Urban", "Suburban", "Rural", "Unknown"]
    ruca_filtered = [r for r in ruca_order if r in df["ruca_category"].unique()]
    df.boxplot(
        column="medicare_pct_of_total",
        by="ruca_category",
        ax=ax6,
        boxprops=dict(color="#4C72B0"),
        medianprops=dict(color="red")
    )
    ax6.set_title("Medicare % of Total Payment by Location")
    ax6.set_xlabel("RUCA Category")
    ax6.set_ylabel("Medicare % of Total Payment")
    plt.sca(ax6)
    plt.title("Medicare % of Total Payment by Location")

    fig.suptitle("CMS Medicare Inpatient FFS 2023 — Descriptive Analysis")
    plt.suptitle("")
    fig.suptitle("CMS Medicare Inpatient FFS 2023 — Descriptive Analysis")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(OUTPUT_DIR + "cms_descriptive_stats.png", dpi=150, bbox_inches="tight")
    print(f"\nChart saved to {OUTPUT_DIR}cms_descriptive_stats.png")
    plt.show()

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df                  = load_data(DB_CONFIG)
    summary             = overall_summary(df)
    ruca_stats          = stats_by_ruca(df)
    top_drgs            = top_drgs_by_volume(df)
    state_stats         = top_states_by_ctp(df)
    df, outliers        = flag_outliers(df)
    create_charts(df, ruca_stats, top_drgs, state_stats)

    print("\n── Done! ────────────────────────────────────────────────")
