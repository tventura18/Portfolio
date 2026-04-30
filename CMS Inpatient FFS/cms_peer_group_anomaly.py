"""
CMS Medicare Inpatient FFS - Peer Group Anomaly Detection
Three Separate Isolation Forest Models:
    1. CAH providers
    2. Rural Non-CAH providers  
    3. Urban/Suburban providers

Author: Toni

AI Governance Implication:
    A single model trained on all providers flags rural and CAH providers
    at disproportionately high rates — not because they are doing anything
    wrong, but because their billing structure is fundamentally different
    from the urban majority the model learns as "normal."
    
    Peer group models correct for this by comparing providers only to
    their structural peers — the same approach CMS uses in provider profiling.
"""

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use('Agg')  # ← add this
import matplotlib.pyplot as plt
#import matplotlib.pyplot as plt

import matplotlib.gridspec as gridspec
import seaborn as sns
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder, StandardScaler
import shap
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

CONTAMINATION = 0.05
RANDOM_STATE  = 42

# Okabe-Ito colorblind safe palette
COLORS = {
    "blue":   "#0072B2",
    "orange": "#E69F00",
    "green":  "#009E73",
    "red":    "#D55E00",
    "purple": "#CC79A7",
    "grey":   "#999999",
}

GROUP_COLORS = {
    "CAH":              COLORS["orange"],
    "Rural Non-CAH":    COLORS["green"],
    "Urban/Suburban":   COLORS["blue"],
}

# ── 1. LOAD DATA ──────────────────────────────────────────────────────────────
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


# ── 2. ASSIGN PEER GROUPS ─────────────────────────────────────────────────────
def assign_peer_groups(df):
    print("\nAssigning peer groups...")

    conditions = [
        df["is_cah"] == True,
        (df["is_cah"] == False) & (df["ruca_category"] == "Rural"),
    ]
    choices = ["CAH", "Rural Non-CAH"]
    df["peer_group"] = np.select(conditions, choices, default="Urban/Suburban")

    group_counts = df["peer_group"].value_counts()
    print("\nPeer group distribution:")
    for group, count in group_counts.items():
        print(f"  {group}: {count:,} rows ({count/len(df)*100:.1f}%)")

    return df


# ── 3. FEATURE ENGINEERING ────────────────────────────────────────────────────
def engineer_features(df):
    print("\nEngineering features...")

    # Encode state
    le_state = LabelEncoder()
    df["state_enc"] = le_state.fit_transform(
        df["Rndrng_Prvdr_State_Abrvtn"].fillna("Unknown")
    )

    # Log transforms
    df["log_avg_submitted_charge"] = np.log1p(df["avg_submitted_charge"])
    df["log_avg_medicare_payment"] = np.log1p(df["avg_medicare_payment"])
    df["log_total_discharges"]     = np.log1p(df["total_discharges"])
    df["log_est_total_charges"]    = np.log1p(df["est_total_charges"])
    df["payment_gap"]              = df["avg_submitted_charge"] - df["avg_medicare_payment"]
    df["log_payment_gap"]          = np.log1p(df["payment_gap"].clip(lower=0))

    return df


# ── 4. FEATURE COLUMNS PER GROUP ──────────────────────────────────────────────
def get_feature_cols(group):
    """
    Feature sets differ by group because some features aren't meaningful
    across groups (e.g. CTP ratio behaves differently for CAH vs DRG hospitals)
    """
    base_features = [
        "log_avg_submitted_charge",
        "log_avg_medicare_payment",
        "medicare_pct_of_total",
        "log_total_discharges",
        "log_payment_gap",
        "data_year",
        "state_enc",
    ]

    if group == "CAH":
        # CAH: CTP ratio less relevant, focus on cost patterns and volume
        return base_features + ["charge_to_payment_ratio"]

    elif group == "Rural Non-CAH":
        # Rural non-CAH: DRG-based but small volume, different case mix
        return base_features + [
            "charge_to_payment_ratio",
            "avg_non_medicare_payment",
        ]

    else:  # Urban/Suburban
        # Urban/Suburban: full feature set, CTP ratio most meaningful here
        return base_features + [
            "charge_to_payment_ratio",
            "avg_non_medicare_payment",
            "log_est_total_charges",
        ]


# ── 5. RUN MODEL FOR ONE GROUP ────────────────────────────────────────────────
def run_group_model(df_group, group_name, contamination=CONTAMINATION):
    print(f"\n{'='*60}")
    print(f"Running Isolation Forest — {group_name}")
    print(f"{'='*60}")
    print(f"Rows: {len(df_group):,}")

    feature_cols = get_feature_cols(group_name)
    df_clean     = df_group.dropna(subset=feature_cols).copy()
    print(f"Rows after dropping nulls: {len(df_clean):,}")

    X        = df_clean[feature_cols].values
    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    iso_forest = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=RANDOM_STATE,
        n_jobs=-1
    )
    iso_forest.fit(X_scaled)

    df_clean["anomaly_flag"]  = iso_forest.predict(X_scaled)
    df_clean["anomaly_score"] = iso_forest.score_samples(X_scaled)
    df_clean["is_anomaly"]    = df_clean["anomaly_flag"] == -1
    df_clean["peer_group"]    = group_name

    n_anomalies = df_clean["is_anomaly"].sum()
    print(f"Anomalies flagged: {n_anomalies:,} ({n_anomalies/len(df_clean)*100:.1f}%)")

    # ── SHAP ──────────────────────────────────────────────────────────────────
    sample_size = min(3000, len(X_scaled))
    idx_sample  = np.random.RandomState(RANDOM_STATE).choice(
        len(X_scaled), sample_size, replace=False
    )
    X_sample    = X_scaled[idx_sample]
    explainer   = shap.TreeExplainer(iso_forest)
    shap_values = explainer.shap_values(X_sample)

    # ── Summary stats ─────────────────────────────────────────────────────────
    anomalies    = df_clean[df_clean["is_anomaly"] == True]
    normal       = df_clean[df_clean["is_anomaly"] == False]

    print(f"\nAnomaly vs Normal — Key Metrics:")
    for col in ["charge_to_payment_ratio", "avg_medicare_payment",
                "avg_submitted_charge", "total_discharges"]:
        if col in df_clean.columns:
            print(f"  {col}:")
            print(f"    Anomaly mean: {anomalies[col].mean():.2f}")
            print(f"    Normal mean:  {normal[col].mean():.2f}")

    # ── Top anomalies ─────────────────────────────────────────────────────────
    print(f"\nTop 5 Most Anomalous — {group_name}:")
    top = anomalies.nsmallest(5, "anomaly_score")[
        ["provider_name", "Rndrng_Prvdr_State_Abrvtn", "drg_code",
         "charge_to_payment_ratio", "avg_medicare_payment",
         "total_discharges", "anomaly_score", "data_year"]
    ]
    print(top.to_string())

    return df_clean, iso_forest, shap_values, X_sample, feature_cols


# ── 6. ANALYZE ANOMALIES BY GROUP ─────────────────────────────────────────────
def analyze_by_group(results):
    print("\n" + "="*60)
    print("CROSS-GROUP ANOMALY COMPARISON")
    print("="*60)

    summary_rows = []
    for group_name, (df_clean, _, _, _, _) in results.items():
        year_rates = (
            df_clean.groupby("data_year")["is_anomaly"]
            .mean() * 100
        ).round(2)

        state_rates = (
            df_clean.groupby("Rndrng_Prvdr_State_Abrvtn")["is_anomaly"]
            .mean() * 100
        ).sort_values(ascending=False)

        print(f"\n{group_name} — Anomaly Rate by Year:")
        print(year_rates.to_string())

        print(f"\n{group_name} — Top 5 States by Anomaly Rate:")
        print(state_rates.head(5).to_string())

        summary_rows.append({
            "peer_group":    group_name,
            "total_rows":    len(df_clean),
            "anomaly_count": df_clean["is_anomaly"].sum(),
            "anomaly_rate":  round(df_clean["is_anomaly"].mean() * 100, 2),
            "avg_ctp_anomaly": round(
                df_clean[df_clean["is_anomaly"]]["charge_to_payment_ratio"].mean(), 2
            ),
            "avg_ctp_normal": round(
                df_clean[~df_clean["is_anomaly"]]["charge_to_payment_ratio"].mean(), 2
            ),
        })

    summary_df = pd.DataFrame(summary_rows)
    print("\n── Group Summary ─────────────────────────────────────────")
    print(summary_df.to_string())
    return summary_df


# ── 7. VISUALIZATIONS ─────────────────────────────────────────────────────────
def create_comparison_charts(results, summary_df):
    sns.set_theme(style="whitegrid")
    fig = plt.figure(figsize=(24, 22))
    fig.suptitle(
        "CMS Medicare Inpatient FFS 2019–2023\n"
        "Peer Group Isolation Forest — Anomaly Detection Comparison",
        fontsize=15, fontweight="bold", y=0.99
    )
    #gs = gridspec.GridSpec(4, 3, figure=fig, hspace=0.6, wspace=0.4)
    gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.6, wspace=0.4)

    groups     = list(results.keys())
    group_cols = [GROUP_COLORS[g] for g in groups]

    # ── Row 1: Anomaly score distributions by group ───────────────────────────
    for i, group_name in enumerate(groups):
        ax = fig.add_subplot(gs[0, i])
        df_clean = results[group_name][0]
        normal   = df_clean[df_clean["is_anomaly"] == False]["anomaly_score"]
        anomaly  = df_clean[df_clean["is_anomaly"] == True]["anomaly_score"]
        ax.hist(normal, bins=40, alpha=0.6, color=COLORS["blue"],
                label="Normal", density=True)
        ax.hist(anomaly, bins=40, alpha=0.6, color=COLORS["red"],
                label="Anomaly", density=True)
        ax.set_title(f"Anomaly Score Distribution\n{group_name}",
                     fontsize=9, fontweight="bold")
        ax.set_xlabel("Anomaly Score")
        ax.set_ylabel("Density")
        ax.legend(fontsize=7)

    # ── Row 2: Anomaly rate by year for each group ────────────────────────────
    ax_trend = fig.add_subplot(gs[1, :])
    for group_name, color in GROUP_COLORS.items():
        df_clean   = results[group_name][0]
        year_rates = (
            df_clean.groupby("data_year")["is_anomaly"]
            .mean() * 100
        ).reset_index()
        ax_trend.plot(
            year_rates["data_year"], year_rates["is_anomaly"],
            marker="o", color=color, linewidth=2,
            markersize=8, label=group_name
        )
        for _, row in year_rates.iterrows():
            ax_trend.annotate(
                f"{row['is_anomaly']:.1f}%",
                (row["data_year"], row["is_anomaly"]),
                textcoords="offset points", xytext=(0, 8),
                ha="center", fontsize=7, color=color
            )
    ax_trend.set_title("Anomaly Rate by Year — Peer Group Comparison",
                       fontsize=11, fontweight="bold")
    ax_trend.set_ylabel("Anomaly Rate (%)")
    ax_trend.set_xlabel("")
    ax_trend.legend(fontsize=9)
    ax_trend.set_xticks(sorted(results["CAH"][0]["data_year"].unique()))
    ax_trend.grid(True, alpha=0.3)

    # ── Row 3: CTP ratio — anomaly vs normal by group ─────────────────────────
    for i, group_name in enumerate(groups):
        ax = fig.add_subplot(gs[2, i])
        df_clean = results[group_name][0]
        bp = ax.boxplot(
            [
                df_clean[df_clean["is_anomaly"] == False]["charge_to_payment_ratio"]
                .clip(0, 20),
                df_clean[df_clean["is_anomaly"] == True]["charge_to_payment_ratio"]
                .clip(0, 20),
            ],
            labels=["Normal", "Anomaly"],
            patch_artist=True,
            medianprops=dict(color="white", linewidth=2)
        )
        bp["boxes"][0].set_facecolor(COLORS["blue"])
        bp["boxes"][1].set_facecolor(COLORS["red"])
        ax.set_title(f"CTP Ratio — Normal vs Anomaly\n{group_name}",
                     fontsize=9, fontweight="bold")
        ax.set_ylabel("CTP Ratio (clipped at 20)")

    # ── Row 4: SHAP summary for each group ────────────────────────────────────
    '''for i, group_name in enumerate(groups):
        ax = fig.add_subplot(gs[3, i])
        _, _, shap_values, X_sample, feature_cols = results[group_name]
        shap.summary_plot(
            shap_values, X_sample,
            feature_names=feature_cols,
            show=False, plot_size=None,
            max_display=6
        )
        plt.sca(ax)
        ax.set_title(f"SHAP Feature Importance\n{group_name}",
                     fontsize=9, fontweight="bold")'''

    plt.savefig(OUTPUT_DIR + "cms_peer_group_anomaly.png",
                dpi=150, bbox_inches="tight")
    print(f"\nPeer group charts saved to {OUTPUT_DIR}cms_peer_group_anomaly.png")
    #plt.show()

def save_shap_charts(results):
    print("=== SAVING SHAP CHARTS ===")
    print(f"Output dir: {os.path.abspath(OUTPUT_DIR)}")
    for group_name, (_, _, shap_values, X_sample, feature_cols) in results.items():
        print(f"  Processing {group_name}...")
        plt.figure(figsize=(10, 6))
        shap.summary_plot(
            shap_values, X_sample,
            feature_names=feature_cols,
            show=False,
            max_display=6
        )
        plt.title(f"SHAP Feature Importance — {group_name}",
                  fontsize=11, fontweight="bold")
        filename = f"cms_shap_{group_name.replace('/', '_').replace(' ', '_')}.png"
        plt.savefig(OUTPUT_DIR + filename, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"  Saved: {filename}")
        


# ── 8. SAVE RESULTS ───────────────────────────────────────────────────────────
def save_results(results, config):
    print("\nSaving peer group anomaly results to PostgreSQL...")
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(conn_str)

    all_results = []
    for group_name, (df_clean, _, _, _, _) in results.items():
        all_results.append(df_clean)

    combined = pd.concat(all_results, ignore_index=True)
    combined[[
        "provider_ccn", "provider_name", "drg_code", "drg_desc",
        "data_year", "peer_group", "is_anomaly", "anomaly_score",
        "charge_to_payment_ratio", "avg_medicare_payment",
        "total_discharges", "ruca_category", "is_cah",
        "Rndrng_Prvdr_State_Abrvtn"
    ]].to_sql(
        "cms_peer_group_anomaly_results",
        engine,
        if_exists="replace",
        index=False,
        chunksize=5000
    )
    print(f"Saved {len(combined):,} rows to cms_peer_group_anomaly_results")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Load and prepare
    df = load_data(DB_CONFIG)
    df = assign_peer_groups(df)
    df = engineer_features(df)

    # Run separate model for each peer group
    results = {}
    for group_name in ["CAH", "Rural Non-CAH", "Urban/Suburban"]:
        df_group = df[df["peer_group"] == group_name].copy()
        df_clean, iso_forest, shap_values, X_sample, feature_cols = \
            run_group_model(df_group, group_name)
        results[group_name] = (
            df_clean, iso_forest, shap_values, X_sample, feature_cols
        )

    # Cross-group comparison
    summary_df = analyze_by_group(results)

    # Visualize
    create_comparison_charts(results, summary_df)
    save_shap_charts(results)
    plt.show()

    # Save to PostgreSQL
    save_results(results, DB_CONFIG)

    print("\n── Done! ────────────────────────────────────────────────")
