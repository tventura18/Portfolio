"""
CMS Medicare Inpatient FFS - Control Charts
Data: Medicare Inpatient Hospitals by Provider and Service (2023)
Author: Toni

Control Charts:
    1. I-MR Chart — Avg Charge-to-Payment Ratio by State
    2. I-MR Chart — Avg Medicare Payment by State
    3. P-Chart   — Proportion of High-Risk Cases (CTP > 10) by State
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
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
    return df


# ── Helper: I-MR Control Limits ───────────────────────────────────────────────
def imr_limits(values):
    """Calculate I-MR control limits."""
    values = np.array(values)
    n = len(values)

    # Individuals chart
    x_bar = values.mean()
    mr = np.abs(np.diff(values))          # moving ranges
    mr_bar = mr.mean()
    d2 = 1.128                            # constant for n=2 subgroup
    sigma = mr_bar / d2

    ucl_i = x_bar + 3 * sigma
    lcl_i = x_bar - 3 * sigma

    # Moving range chart
    D4 = 3.267                            # constant for n=2
    ucl_mr = D4 * mr_bar

    return {
        "x_bar": x_bar,
        "ucl_i": ucl_i,
        "lcl_i": max(lcl_i, 0),           # floor at 0 for non-negative metrics
        "mr_bar": mr_bar,
        "ucl_mr": ucl_mr,
        "sigma": sigma,
        "values": values,
        "mr": mr
    }


# ── Helper: P-Chart Control Limits ────────────────────────────────────────────
def pchart_limits(n_total, n_defect):
    """Calculate P-chart control limits."""
    p_bar = n_defect.sum() / n_total.sum()
    sigma_p = np.sqrt(p_bar * (1 - p_bar) / n_total)

    ucl = p_bar + 3 * sigma_p
    lcl = np.maximum(p_bar - 3 * sigma_p, 0)
    p = n_defect / n_total

    return {
        "p_bar": p_bar,
        "ucl":   ucl,
        "lcl":   lcl,
        "p":     p
    }


# ── 1. AGGREGATE BY STATE ─────────────────────────────────────────────────────
def aggregate_by_state(df, ctp_threshold=10):
    state_agg = (
        df.groupby("Rndrng_Prvdr_State_Abrvtn")
        .agg(
            total_cases      = ("total_discharges", "sum"),
            avg_ctp          = ("charge_to_payment_ratio", "mean"),
            avg_medicare_pmt = ("avg_medicare_payment", "mean"),
            high_risk_cases  = ("charge_to_payment_ratio", lambda x: (x > ctp_threshold).sum())
        )
        .reset_index()
        .sort_values("Rndrng_Prvdr_State_Abrvtn")
    )
    # Exclude territories / unknowns with very low volume
    state_agg = state_agg[state_agg["total_cases"] > 1000]
    print(f"\nStates included in control charts: {len(state_agg)}")
    return state_agg


# ── 2. PLOT I-MR CHART ────────────────────────────────────────────────────────
def plot_imr(ax_i, ax_mr, values, labels, title, ylabel, color="#4C72B0"):
    limits = imr_limits(values)

    # ── Individuals chart ─────────────────────────────────────────────────────
    ax_i.plot(range(len(values)), limits["values"], marker="o", color=color,
              linewidth=1, markersize=4, label="Value")
    ax_i.axhline(limits["x_bar"], color="green",  linestyle="-",  linewidth=1.2, label=f"CL: {limits['x_bar']:.2f}")
    ax_i.axhline(limits["ucl_i"], color="red",    linestyle="--", linewidth=1.2, label=f"UCL: {limits['ucl_i']:.2f}")
    ax_i.axhline(limits["lcl_i"], color="red",    linestyle="--", linewidth=1.2, label=f"LCL: {limits['lcl_i']:.2f}")

    # Flag out-of-control points
    ooc = [i for i, v in enumerate(limits["values"]) if v > limits["ucl_i"] or v < limits["lcl_i"]]
    for i in ooc:
        ax_i.plot(i, limits["values"][i], "r*", markersize=10, zorder=5)
        ax_i.annotate(labels[i], (i, limits["values"][i]),
                      textcoords="offset points", xytext=(4, 4), fontsize=6, color="red")

    ax_i.set_title(f"{title} — Individuals Chart", fontsize=10, fontweight="bold")
    ax_i.set_ylabel(ylabel)
    ax_i.set_xticks(range(len(labels)))
    ax_i.set_xticklabels(labels, rotation=90, fontsize=6)
    ax_i.legend(fontsize=7, loc="upper right")
    ax_i.grid(True, alpha=0.3)

    # ── Moving Range chart ────────────────────────────────────────────────────
    ax_mr.plot(range(len(limits["mr"])), limits["mr"], marker="o", color="#C44E52",
               linewidth=1, markersize=4)
    ax_mr.axhline(limits["mr_bar"], color="green", linestyle="-",  linewidth=1.2, label=f"CL: {limits['mr_bar']:.2f}")
    ax_mr.axhline(limits["ucl_mr"], color="red",   linestyle="--", linewidth=1.2, label=f"UCL: {limits['ucl_mr']:.2f}")
    ax_mr.axhline(0,                color="red",   linestyle="--", linewidth=1.2, label="LCL: 0")

    # Flag OOC on MR chart
    ooc_mr = [i for i, v in enumerate(limits["mr"]) if v > limits["ucl_mr"]]
    for i in ooc_mr:
        ax_mr.plot(i, limits["mr"][i], "r*", markersize=10, zorder=5)

    ax_mr.set_title(f"{title} — Moving Range Chart", fontsize=10, fontweight="bold")
    ax_mr.set_ylabel("Moving Range")
    ax_mr.set_xticks(range(len(labels) - 1))
    ax_mr.set_xticklabels(labels[:-1], rotation=90, fontsize=6)
    ax_mr.legend(fontsize=7, loc="upper right")
    ax_mr.grid(True, alpha=0.3)

    print(f"\n{title} — Out of Control States: {[labels[i] for i in ooc]}")
    return ooc


# ── 3. PLOT P-CHART ───────────────────────────────────────────────────────────
def plot_pchart(ax, n_total, n_defect, labels, title, threshold):
    limits = pchart_limits(n_total, n_defect)

    ax.plot(range(len(limits["p"])), limits["p"], marker="o", color="#4C72B0",
            linewidth=1, markersize=4, label="Proportion")
    ax.axhline(limits["p_bar"], color="green", linestyle="-",  linewidth=1.2,
               label=f"CL: {limits['p_bar']:.4f}")

    # Variable control limits (different n per state)
    ax.plot(range(len(limits["ucl"])), limits["ucl"], color="red",
            linestyle="--", linewidth=1, label="UCL (variable)")
    ax.plot(range(len(limits["lcl"])), limits["lcl"], color="red",
            linestyle="--", linewidth=1, label="LCL (variable)")
    ax.fill_between(range(len(limits["ucl"])), limits["lcl"], limits["ucl"],
                    alpha=0.05, color="red")

    # Flag OOC
    ooc = [i for i, (p, u, l) in enumerate(zip(limits["p"], limits["ucl"], limits["lcl"]))
           if p > u or p < l]
    for i in ooc:
        ax.plot(i, limits["p"][i], "r*", markersize=10, zorder=5)
        ax.annotate(labels[i], (i, limits["p"][i]),
                    textcoords="offset points", xytext=(4, 4), fontsize=6, color="red")

    ax.set_title(f"{title}\n(Proportion of cases with CTP Ratio > {threshold})",
                 fontsize=10, fontweight="bold")
    ax.set_ylabel("Proportion High-Risk Cases")
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=90, fontsize=6)
    ax.legend(fontsize=7, loc="upper right")
    ax.grid(True, alpha=0.3)

    print(f"\nP-Chart — Out of Control States: {[labels[i] for i in ooc]}")
    return ooc


# ── 4. MAIN CHART LAYOUT ──────────────────────────────────────────────────────
def create_control_charts(state_agg, ctp_threshold=10):
    labels = state_agg["Rndrng_Prvdr_State_Abrvtn"].tolist()
    ctp_values = state_agg["avg_ctp"].values
    pmt_values = state_agg["avg_medicare_pmt"].values
    n_total    = state_agg["total_cases"].values
    n_defect   = state_agg["high_risk_cases"].values

    fig = plt.figure(figsize=(20, 18))
    fig.suptitle("CMS Medicare Inpatient FFS 2023 — Control Charts by State",
                 fontsize=15, fontweight="bold", y=0.99)

    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.65, wspace=0.35)

    ax1 = fig.add_subplot(gs[0, :])    # I chart CTP — full width
    ax2 = fig.add_subplot(gs[1, :])    # MR chart CTP — full width
    ax3 = fig.add_subplot(gs[2, 0])    # I chart Medicare Payment
    ax4 = fig.add_subplot(gs[2, 1])    # P-chart High Risk

    # I-MR for charge-to-payment ratio
    plot_imr(ax1, ax2, ctp_values, labels,
             "Avg Charge-to-Payment Ratio by State", "Avg CTP Ratio", color="#4C72B0")

    # I chart for Medicare payment (individuals only)
    limits_pmt = imr_limits(pmt_values)
    ax3.plot(range(len(pmt_values)), pmt_values, marker="o", color="#55A868",
             linewidth=1, markersize=4)
    ax3.axhline(limits_pmt["x_bar"], color="green",  linestyle="-",  linewidth=1.2,
                label=f"CL: ${limits_pmt['x_bar']:,.0f}")
    ax3.axhline(limits_pmt["ucl_i"], color="red",    linestyle="--", linewidth=1.2,
                label=f"UCL: ${limits_pmt['ucl_i']:,.0f}")
    ax3.axhline(limits_pmt["lcl_i"], color="red",    linestyle="--", linewidth=1.2,
                label=f"LCL: ${limits_pmt['lcl_i']:,.0f}")
    ooc_pmt = [i for i, v in enumerate(pmt_values)
               if v > limits_pmt["ucl_i"] or v < limits_pmt["lcl_i"]]
    for i in ooc_pmt:
        ax3.plot(i, pmt_values[i], "r*", markersize=10, zorder=5)
        ax3.annotate(labels[i], (i, pmt_values[i]),
                     textcoords="offset points", xytext=(4, 4), fontsize=6, color="red")
    ax3.set_title("Avg Medicare Payment by State — Individuals Chart",
                  fontsize=10, fontweight="bold")
    ax3.set_ylabel("Avg Medicare Payment ($)")
    ax3.set_xticks(range(len(labels)))
    ax3.set_xticklabels(labels, rotation=90, fontsize=6)
    ax3.legend(fontsize=7)
    ax3.grid(True, alpha=0.3)
    print(f"\nMedicare Payment I-Chart — Out of Control States: {[labels[i] for i in ooc_pmt]}")

    # P-chart for high risk proportion
    plot_pchart(ax4, n_total, n_defect, labels,
                "P-Chart: High-Risk Cases by State", ctp_threshold)

    plt.savefig(OUTPUT_DIR + "cms_control_charts.png", dpi=150, bbox_inches="tight")
    print(f"\nControl charts saved to {OUTPUT_DIR}cms_control_charts.png")
    plt.show()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    CTP_THRESHOLD = 10    # CTP ratio above this = high risk

    df         = load_data(DB_CONFIG)
    state_agg  = aggregate_by_state(df, ctp_threshold=CTP_THRESHOLD)
    create_control_charts(state_agg, ctp_threshold=CTP_THRESHOLD)

    print("\n── Done! ────────────────────────────────────────────────")
