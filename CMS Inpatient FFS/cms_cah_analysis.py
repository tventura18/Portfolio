"""
CMS Medicare Inpatient FFS - CAH Analysis Charts
Adds CAH breakdown to descriptive statistics
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
    print(f"CAH rows: {df['is_cah'].sum():,}")
    return df


# ── 1. PAYMENT GAP: CAH vs NON-CAH by RUCA ───────────────────────────────────
def payment_gap_summary(df):
    print("\n" + "="*60)
    print("PAYMENT GAP: CAH vs NON-CAH BY RUCA CATEGORY")
    print("="*60)

    summary = (
        df.groupby(["ruca_category", "is_cah"])
        .agg(
            providers        = ("Rndrng_Prvdr_CCN", "nunique"),
            rows             = ("total_discharges", "count"),
            total_discharges = ("total_discharges", "sum"),
            avg_medicare_pmt = ("avg_medicare_payment", "mean"),
            avg_ctp_ratio    = ("charge_to_payment_ratio", "mean"),
            avg_charge       = ("avg_submitted_charge", "mean"),
        )
        .round(2)
    )
    print(summary.to_string())
    return summary


# ── 2. RURAL DEEP DIVE ────────────────────────────────────────────────────────
def rural_deep_dive(df):
    print("\n" + "="*60)
    print("RURAL DEEP DIVE: TOP DRGs FOR CAH vs NON-CAH")
    print("="*60)

    rural = df[df["ruca_category"] == "Rural"]

    for cah_flag in [True, False]:
        label = "CAH" if cah_flag else "Non-CAH"
        subset = rural[rural["is_cah"] == cah_flag]
        top = (
            subset.groupby(["drg_code", "drg_desc"])
            .agg(total_discharges=("total_discharges", "sum"),
                 avg_medicare_pmt=("avg_medicare_payment", "mean"))
            .sort_values("total_discharges", ascending=False)
            .head(5)
            .round(2)
        )
        print(f"\nTop 5 DRGs — Rural {label}:")
        print(top.to_string())

    return rural


# ── 3. CREATE CAH CHARTS ──────────────────────────────────────────────────────
def create_cah_charts(df):
    sns.set_theme(style="whitegrid")
    plt.rcParams["axes.prop_cycle"] = plt.cycler(color=["#0072B2", "#E69F00"])

    #fig = plt.figure(figsize=(20, 16))
    fig = plt.figure(figsize=(22, 14))
    fig.suptitle("CMS Medicare Inpatient FFS 2023 — CAH vs Non-CAH Analysis",
                 fontsize=16, fontweight="bold", y=0.99)
    #gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.5, wspace=0.4)
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.5, wspace=0.8)

    #fig.subplots_adjust(wspace=0.95)

    # ── Prep data ─────────────────────────────────────────────────────────────
    rural_suburban = df[df["ruca_category"].isin(["Rural", "Suburban"])]
    rural_only     = df[df["ruca_category"] == "Rural"]

    cah_labels = {True: "CAH", False: "Non-CAH"}
    colors_cah = {"CAH": "#E69F00", "Non-CAH": "#0072B2"}
    #colors_cah = {"CAH": "#648FFF", "Non-CAH": "#785EF0"}

    # ── Chart 1: Avg Medicare Payment — 4-way breakdown ──────────────────────
    ax1 = fig.add_subplot(gs[0, 0])
    group = (
        rural_suburban.groupby(["ruca_category", "is_cah"])["avg_medicare_payment"]
        .mean().round(2).reset_index()
    )
    group["label"] = group["ruca_category"] + "\n" + group["is_cah"].map(cah_labels)
    group["color"] = group["is_cah"].map({True: "#E69F00", False: "#0072B2"})
    bars = ax1.bar(group["label"], group["avg_medicare_payment"],
                   color=group["color"], edgecolor="white", width=0.6)
    for bar, val in zip(bars, group["avg_medicare_payment"]):
        ax1.text(bar.get_x() + bar.get_width()/2,
                 bar.get_height() * 0.95,
                 f"${val:,.0f}", ha="center", va="top",
                 color="white", fontsize=8, fontweight="bold")
    ax1.set_title("Avg Medicare Payment\nby Location & CAH Status", fontsize=10, fontweight="bold")
    ax1.set_ylabel("Avg Medicare Payment ($)")
    ax1.set_xlabel("")

    # ── Chart 2: Avg CTP Ratio — 4-way breakdown ─────────────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    group2 = (
        rural_suburban.groupby(["ruca_category", "is_cah"])["charge_to_payment_ratio"]
        .mean().round(2).reset_index()
    )
    group2["label"] = group2["ruca_category"] + "\n" + group2["is_cah"].map(cah_labels)
    group2["color"] = group2["is_cah"].map({True: "#E69F00", False: "#0072B2"})
    bars2 = ax2.bar(group2["label"], group2["charge_to_payment_ratio"],
                    color=group2["color"], edgecolor="white", width=0.6)
    for bar, val in zip(bars2, group2["charge_to_payment_ratio"]):
        ax2.text(bar.get_x() + bar.get_width()/2,
                 bar.get_height() * 0.95,
                 f"{val:.2f}x", ha="center", va="top",
                 color="white", fontsize=8, fontweight="bold")
    ax2.set_title("Avg Charge-to-Payment Ratio\nby Location & CAH Status", fontsize=10, fontweight="bold")
    ax2.set_ylabel("Avg CTP Ratio")
    ax2.set_xlabel("")

    # ── Chart 3: Box plot — Medicare Payment by CAH status (rural only) ───────
    ax3 = fig.add_subplot(gs[0, 2])
    rural_cah    = rural_only[rural_only["is_cah"] == True]["avg_medicare_payment"]
    rural_noncah = rural_only[rural_only["is_cah"] == False]["avg_medicare_payment"]
    bp = ax3.boxplot([rural_cah, rural_noncah],
                     labels=["CAH", "Non-CAH"],
                     patch_artist=True,
                     medianprops=dict(color="white", linewidth=2))
    bp["boxes"][0].set_facecolor("#E69F00")
    bp["boxes"][1].set_facecolor("#0072B2")
    ax3.set_title("Medicare Payment Distribution\nRural CAH vs Non-CAH", fontsize=10, fontweight="bold")
    ax3.set_ylabel("Avg Medicare Payment ($)")

    # ── Chart 4: Top DRGs for Rural CAH ──────────────────────────────────────
    ax4 = fig.add_subplot(gs[1, 0])
    top_cah = (
        rural_only[rural_only["is_cah"] == True]
        .groupby("drg_desc")["total_discharges"].sum()
        .sort_values(ascending=False).head(8)
    )
    #labels4 = [d[:25] + "..." if len(d) > 15 else d for d in top_cah.index]
    labels4 = [d[:15] + "..." if len(d) > 20 else d for d in top_cah.index]
    ax4.barh(labels4, top_cah.values, color="#E69F00")
    ax4.invert_yaxis()
    ax4.set_title("Top DRGs by Volume\nRural CAH", fontsize=10, fontweight="bold")
    ax4.set_xlabel("Total Discharges")

    ax4.set_yticklabels(labels4, fontsize=6.5, ha='right')
    ax4.set_xlim(0, top_cah.values.max() * 1.15)
    ax4.margins(y=0.1)
    #ax4.tick_params(axis="y", labelsize=7)

    # ── Chart 5: Top DRGs for Rural Non-CAH ──────────────────────────────────
    ax5 = fig.add_subplot(gs[1, 1])
    top_noncah = (
        rural_only[rural_only["is_cah"] == False]
        .groupby("drg_desc")["total_discharges"].sum()
        .sort_values(ascending=False).head(8)
    )
    labels5 = [d[:25] + "..." if len(d) > 14 else d for d in top_noncah.index]
    ax5.barh(labels5, top_noncah.values, color="#0072B2")
    ax5.invert_yaxis()
    ax5.set_title("Top DRGs by Volume\nRural Non-CAH", fontsize=10, fontweight="bold")
    ax5.set_xlabel("Total Discharges")
    #ax5.tick_params(axis="y", labelsize=7)
    ax5.tick_params(axis="y", labelsize=6.5)
    ax5.yaxis.set_tick_params(pad=2)

    # ── Chart 6: CTP ratio distribution CAH vs non-CAH ───────────────────────
    ax6 = fig.add_subplot(gs[1, 2])
    ax6.hist(rural_noncah.clip(0, 50000), bins=40,
             alpha=0.6, color="#4C72B0", label="Non-CAH Rural", density=True)
    ax6.hist(rural_cah.clip(0, 50000), bins=40,
             alpha=0.6, color="#E69F00", label="CAH Rural", density=True)
    ax6.axvline(rural_noncah.mean(), color="#0072B2", linestyle="--", linewidth=1.5,
                label=f"Non-CAH Mean: ${rural_noncah.mean():,.0f}")
    ax6.axvline(rural_cah.mean(), color="#E69F00", linestyle="--", linewidth=1.5,
                label=f"CAH Mean: ${rural_cah.mean():,.0f}")
    ax6.set_title("Medicare Payment Distribution\nRural CAH vs Non-CAH (Density)", fontsize=10, fontweight="bold")
    ax6.set_xlabel("Avg Medicare Payment ($)")
    ax6.set_ylabel("Density")
    ax6.legend(fontsize=7)
    

    # ── Legend patch ──────────────────────────────────────────────────────────
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor="#E69F00", label="CAH"),
                       Patch(facecolor="#0072B2", label="Non-CAH")]
    fig.legend(handles=legend_elements, loc="upper right",
               fontsize=9, title="Provider Type", bbox_to_anchor=(0.99, 0.97))

    plt.savefig(OUTPUT_DIR + "cms_cah_analysis.png", dpi=150, bbox_inches="tight")
    print(f"\nCAH analysis chart saved to {OUTPUT_DIR}cms_cah_analysis.png")
    plt.show()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df      = load_data(DB_CONFIG)
    summary = payment_gap_summary(df)
    rural   = rural_deep_dive(df)
    create_cah_charts(df)
    print("\n── Done! ────────────────────────────────────────────────")
