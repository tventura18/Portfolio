"""
CMS Medicare Inpatient FFS - Multi-Year Control Charts
Data: Medicare Inpatient Hospitals by Provider and Service (2019-2023)
Author: Toni

Control Charts:
    1. I-MR Chart — Avg CTP Ratio by State (each year overlaid)
    2. P-Chart   — Proportion High-Risk Cases by State (each year)
    3. I-Chart   — Avg Medicare Payment by State (each year)
    4. Year-over-Year Special Cause Summary
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
YEAR_COLORS = {
    2019: "#0072B2",  # blue
    2020: "#E69F00",  # orange
    2021: "#009E73",  # green
    2022: "#D55E00",  # red
    2023: "#CC79A7",  # purple
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


# ── Helper: I-MR Control Limits ───────────────────────────────────────────────
def imr_limits(values):
    values = np.array(values)
    x_bar  = values.mean()
    mr     = np.abs(np.diff(values))
    mr_bar = mr.mean()
    d2     = 1.128
    sigma  = mr_bar / d2
    ucl_i  = x_bar + 3 * sigma
    lcl_i  = max(x_bar - 3 * sigma, 0)
    D4     = 3.267
    ucl_mr = D4 * mr_bar
    return {
        "x_bar": x_bar, "ucl_i": ucl_i, "lcl_i": lcl_i,
        "mr_bar": mr_bar, "ucl_mr": ucl_mr, "sigma": sigma,
        "values": values, "mr": mr
    }


# ── Helper: P-Chart Control Limits ────────────────────────────────────────────
def pchart_limits(n_total, n_defect):
    p_bar   = n_defect.sum() / n_total.sum()
    sigma_p = np.sqrt(p_bar * (1 - p_bar) / n_total)
    ucl     = p_bar + 3 * sigma_p
    lcl     = np.maximum(p_bar - 3 * sigma_p, 0)
    p       = n_defect / n_total
    return {"p_bar": p_bar, "ucl": ucl, "lcl": lcl, "p": p}


# ── 1. AGGREGATE BY STATE AND YEAR ────────────────────────────────────────────
def aggregate_by_state_year(df):
    state_year = (
        df.groupby(["data_year", "Rndrng_Prvdr_State_Abrvtn"])
        .agg(
            total_cases      = ("total_discharges", "sum"),
            avg_ctp          = ("charge_to_payment_ratio", "mean"),
            avg_medicare_pmt = ("avg_medicare_payment", "mean"),
            high_risk_cases  = ("charge_to_payment_ratio",
                                lambda x: (x > CTP_THRESHOLD).sum())
        )
        .reset_index()
        .rename(columns={"Rndrng_Prvdr_State_Abrvtn": "state"})
    )
    # Filter low volume states
    state_year = state_year[state_year["total_cases"] > 1000]

    # Keep only states present in ALL years for clean comparisons
    state_counts = state_year.groupby("state")["data_year"].count()
    valid_states = state_counts[state_counts == 5].index
    state_year   = state_year[state_year["state"].isin(valid_states)]
    state_year   = state_year.sort_values(["data_year", "state"])

    print(f"\nStates included in control charts: {len(valid_states)}")
    print(f"Years: {sorted(state_year['data_year'].unique())}")
    return state_year, sorted(valid_states)


# ── 2. SPECIAL CAUSE DETECTION ────────────────────────────────────────────────
def detect_special_causes(state_year, valid_states):
    print("\n" + "="*60)
    print("SPECIAL CAUSE DETECTION BY YEAR — CTP RATIO")
    print("="*60)

    results = []
    for year in sorted(state_year["data_year"].unique()):
        year_data = state_year[state_year["data_year"] == year].sort_values("state")
        values    = year_data["avg_ctp"].values
        labels    = year_data["state"].tolist()
        limits    = imr_limits(values)

        ooc = [labels[i] for i, v in enumerate(values)
               if v > limits["ucl_i"] or v < limits["lcl_i"]]

        print(f"\n{year}: UCL={limits['ucl_i']:.2f}, "
              f"CL={limits['x_bar']:.2f}, LCL={limits['lcl_i']:.2f}")
        print(f"  Out of control states: {ooc}")

        for state in ooc:
            val = year_data[year_data["state"] == state]["avg_ctp"].values[0]
            results.append({
                "year": year, "state": state, "avg_ctp": round(val, 2),
                "ucl": round(limits["ucl_i"], 2), "cl": round(limits["x_bar"], 2)
            })

    ooc_df = pd.DataFrame(results)
    if not ooc_df.empty:
        print("\n── Persistent Out-of-Control States ────────────────────")
        persistent = ooc_df.groupby("state")["year"].count()
        persistent = persistent[persistent > 1].sort_values(ascending=False)
        print(persistent.to_string())

    return ooc_df


# ── 3. MULTI-YEAR I-CHART (overlaid) ─────────────────────────────────────────
def plot_multiyear_ichart(ax, state_year, valid_states, metric, ylabel, title):
    """Plot I-chart for each year overlaid on same axes."""
    years = sorted(state_year["data_year"].unique())

    for year in years:
        year_data = state_year[state_year["data_year"] == year].sort_values("state")
        values    = year_data[metric].values
        labels    = year_data["state"].tolist()
        limits    = imr_limits(values)
        color     = YEAR_COLORS[year]

        ax.plot(range(len(values)), values, marker="o", color=color,
                linewidth=1, markersize=3, alpha=0.7, label=str(year))

        # Only show control limits for 2023 to keep chart readable
        if year == 2023:
            ax.axhline(limits["ucl_i"], color="red", linestyle="--",
                       linewidth=1.5, label=f"UCL 2023: {limits['ucl_i']:.2f}")
            ax.axhline(limits["x_bar"], color="green", linestyle="-",
                       linewidth=1.5, label=f"CL 2023: {limits['x_bar']:.2f}")
            ax.axhline(limits["lcl_i"], color="red", linestyle="--",
                       linewidth=1.5, label=f"LCL 2023: {limits['lcl_i']:.2f}")

            # Flag 2023 OOC points
            ooc = [i for i, v in enumerate(values)
                   if v > limits["ucl_i"] or v < limits["lcl_i"]]
            for i in ooc:
                ax.plot(i, values[i], "r*", markersize=12, zorder=5)
                ax.annotate(labels[i], (i, values[i]),
                            textcoords="offset points",
                            xytext=(4, 4), fontsize=6, color="red",
                            fontweight="bold")

    ax.set_title(title, fontsize=10, fontweight="bold")
    ax.set_ylabel(ylabel)
    ax.set_xticks(range(len(valid_states)))
    ax.set_xticklabels(valid_states, rotation=90, fontsize=5)
    ax.legend(fontsize=7, loc="upper right", ncol=3)
    ax.grid(True, alpha=0.3)


# ── 4. PERSISTENT OOC HEATMAP ─────────────────────────────────────────────────
def plot_ooc_heatmap(ax, state_year, valid_states):
    """Heatmap showing which states are OOC in which years."""
    years  = sorted(state_year["data_year"].unique())
    matrix = pd.DataFrame(index=valid_states, columns=years, data=0.0)

    for year in years:
        year_data = state_year[state_year["data_year"] == year].sort_values("state")
        values    = year_data["avg_ctp"].values
        labels    = year_data["state"].tolist()
        limits    = imr_limits(values)

        for i, (state, val) in enumerate(zip(labels, values)):
            if val > limits["ucl_i"]:
                matrix.loc[state, year] = val  # store actual value
            else:
                matrix.loc[state, year] = np.nan

    # Only show states with at least one OOC year
    matrix = matrix.dropna(how="all")

    if not matrix.empty:
        sns.heatmap(matrix.astype(float), ax=ax, cmap="Reds",
                    annot=True, fmt=".1f", linewidths=0.5,
                    cbar_kws={"label": "Avg CTP (OOC only)"},
                    annot_kws={"size": 7})
        ax.set_title("Out-of-Control States by Year\n(CTP Ratio — blank = in control)",
                     fontsize=10, fontweight="bold")
        ax.set_xlabel("")
        ax.set_ylabel("")
        ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=8)
    else:
        ax.text(0.5, 0.5, "No out-of-control states detected",
                ha="center", va="center", transform=ax.transAxes)
        ax.set_title("Out-of-Control States by Year", fontsize=10, fontweight="bold")


# ── 5. P-CHART BY YEAR ────────────────────────────────────────────────────────
def plot_pchart_by_year(ax, state_year, valid_states):
    """P-chart for high risk cases — one line per year."""
    years = sorted(state_year["data_year"].unique())

    for year in years:
        year_data  = state_year[state_year["data_year"] == year].sort_values("state")
        n_total    = year_data["total_cases"].values
        n_defect   = year_data["high_risk_cases"].values
        limits     = pchart_limits(n_total, n_defect)
        color      = YEAR_COLORS[year]

        ax.plot(range(len(limits["p"])), limits["p"], marker="o",
                color=color, linewidth=1, markersize=3,
                alpha=0.7, label=str(year))

        if year == 2023:
            ax.axhline(limits["p_bar"], color="green", linestyle="-",
                       linewidth=1.5, label=f"CL 2023: {limits['p_bar']:.4f}")
            ax.plot(range(len(limits["ucl"])), limits["ucl"],
                    color="red", linestyle="--", linewidth=1,
                    label="UCL 2023 (variable)")

    ax.set_title(f"P-Chart: High-Risk Cases by State\n(CTP Ratio > {CTP_THRESHOLD}) — All Years",
                 fontsize=10, fontweight="bold")
    ax.set_ylabel("Proportion High-Risk Cases")
    ax.set_xticks(range(len(valid_states)))
    ax.set_xticklabels(valid_states, rotation=90, fontsize=5)
    ax.legend(fontsize=7, loc="upper right", ncol=3)
    ax.grid(True, alpha=0.3)


# ── 6. MAIN CHART LAYOUT ──────────────────────────────────────────────────────
def create_control_charts(state_year, valid_states, ooc_df):
    fig = plt.figure(figsize=(22, 18))
    fig.suptitle(
        "CMS Medicare Inpatient FFS 2019–2023 — Multi-Year Control Charts",
        fontsize=15, fontweight="bold", y=0.99
    )
    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.65, wspace=0.35)

    ax1 = fig.add_subplot(gs[0, :])   # I-chart CTP — full width
    ax2 = fig.add_subplot(gs[1, :])   # P-chart — full width
    ax3 = fig.add_subplot(gs[2, 0])   # I-chart Medicare Payment
    ax4 = fig.add_subplot(gs[2, 1])   # OOC Heatmap

    # I-Chart: CTP Ratio all years overlaid
    plot_multiyear_ichart(
        ax1, state_year, valid_states,
        metric="avg_ctp",
        ylabel="Avg CTP Ratio",
        title="Charge-to-Payment Ratio by State — All Years Overlaid\n(2023 Control Limits shown, red ★ = 2023 out-of-control)"
    )

    # P-Chart: High risk proportion all years
    plot_pchart_by_year(ax2, state_year, valid_states)

    # I-Chart: Medicare Payment all years
    plot_multiyear_ichart(
        ax3, state_year, valid_states,
        metric="avg_medicare_pmt",
        ylabel="Avg Medicare Payment ($)",
        title="Avg Medicare Payment by State\nAll Years Overlaid"
    )

    # OOC Heatmap
    plot_ooc_heatmap(ax4, state_year, valid_states)

    plt.savefig(OUTPUT_DIR + "cms_multiyear_control_charts.png",
                dpi=150, bbox_inches="tight")
    print(f"\nControl charts saved to {OUTPUT_DIR}cms_multiyear_control_charts.png")
    plt.show()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df                      = load_data(DB_CONFIG)
    state_year, valid_states = aggregate_by_state_year(df)
    ooc_df                  = detect_special_causes(state_year, valid_states)
    create_control_charts(state_year, valid_states, ooc_df)

    print("\n── Done! ────────────────────────────────────────────────")
