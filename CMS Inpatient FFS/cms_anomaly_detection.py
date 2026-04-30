"""
CMS Medicare Inpatient FFS - Anomaly Detection
Isolation Forest + SHAP Explainability
Data: Medicare Inpatient Hospitals by Provider and Service (2019-2023)
Author: Toni

ML Pipeline:
    1. Feature engineering and preprocessing
    2. Isolation Forest anomaly detection
    3. SHAP explainability
    4. Anomaly analysis — by state, DRG, RUCA, CAH status, year
    5. Visualization
"""

import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('TkAgg')  # or 'MacOSX' on Mac
import matplotlib.pyplot as plt
plt.ion()

import matplotlib.gridspec as gridspec
from scipy import io
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

CONTAMINATION = 0.05   # flag top 5% as anomalies
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
    print(f"Years: {sorted(df['data_year'].unique())}")
    print(f"CAH rows: {df['is_cah'].sum():,}")
    return df


# ── 2. FEATURE ENGINEERING ────────────────────────────────────────────────────
def engineer_features(df):
    print("\nEngineering features...")

    # ── Encode categorical variables ──────────────────────────────────────────
    le_ruca = LabelEncoder()
    df["ruca_category_enc"] = le_ruca.fit_transform(
        df["ruca_category"].fillna("Unknown")
    )

    le_state = LabelEncoder()
    df["state_enc"] = le_state.fit_transform(
        df["Rndrng_Prvdr_State_Abrvtn"].fillna("Unknown")
    )

    # ── CAH as integer ────────────────────────────────────────────────────────
    df["is_cah_int"] = df["is_cah"].astype(int)

    # ── Additional derived features ───────────────────────────────────────────
    # Log transform skewed financial fields
    df["log_avg_submitted_charge"] = np.log1p(df["avg_submitted_charge"])
    df["log_avg_medicare_payment"] = np.log1p(df["avg_medicare_payment"])
    df["log_total_discharges"]     = np.log1p(df["total_discharges"])
    df["log_est_total_charges"]    = np.log1p(df["est_total_charges"])

    # Payment gap
    df["payment_gap"] = df["avg_submitted_charge"] - df["avg_medicare_payment"]
    df["log_payment_gap"] = np.log1p(df["payment_gap"].clip(lower=0))

    print(f"Feature engineering complete. Shape: {df.shape}")
    return df, le_ruca, le_state


# ── 3. PREPARE FEATURE MATRIX ─────────────────────────────────────────────────
def prepare_features(df):
    feature_cols = [
        "log_avg_submitted_charge",
        "log_avg_medicare_payment",
        "charge_to_payment_ratio",
        "medicare_pct_of_total",
        "log_total_discharges",
        "log_payment_gap",
        "ruca_category_enc",
        "is_cah_int",
        "data_year",
        "state_enc",
    ]

    # Drop rows with nulls in feature columns
    df_clean = df.dropna(subset=feature_cols).copy()
    print(f"\nRows after dropping nulls: {len(df_clean):,} "
          f"(dropped {len(df) - len(df_clean):,})")

    X = df_clean[feature_cols].values

    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return df_clean, X_scaled, feature_cols, scaler


# ── 4. ISOLATION FOREST ───────────────────────────────────────────────────────
def run_isolation_forest(df_clean, X_scaled, contamination=CONTAMINATION):
    print(f"\nRunning Isolation Forest (contamination={contamination})...")

    iso_forest = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=RANDOM_STATE,
        n_jobs=-1
    )
    iso_forest.fit(X_scaled)

    # Predictions: -1 = anomaly, 1 = normal
    df_clean["anomaly_flag"]  = iso_forest.predict(X_scaled)
    df_clean["anomaly_score"] = iso_forest.score_samples(X_scaled)
    df_clean["is_anomaly"]    = df_clean["anomaly_flag"] == -1

    n_anomalies = df_clean["is_anomaly"].sum()
    print(f"Anomalies flagged: {n_anomalies:,} "
          f"({n_anomalies/len(df_clean)*100:.1f}%)")

    return df_clean, iso_forest


# ── 5. SHAP EXPLAINABILITY ────────────────────────────────────────────────────
def run_shap(iso_forest, X_scaled, feature_cols, df_clean):
    print("\nCalculating SHAP values (this may take a moment)...")

    # Use a sample for SHAP to keep it manageable
    sample_size = min(5000, len(X_scaled))
    idx_sample  = np.random.RandomState(RANDOM_STATE).choice(
        len(X_scaled), sample_size, replace=False
    )
    X_sample = X_scaled[idx_sample]

    explainer   = shap.TreeExplainer(iso_forest)
    shap_values = explainer.shap_values(X_sample)

    print(f"SHAP values calculated for {sample_size:,} samples")
    return shap_values, X_sample, idx_sample, feature_cols


# ── 6. ANOMALY ANALYSIS ───────────────────────────────────────────────────────
def analyze_anomalies(df_clean):
    print("\n" + "="*60)
    print("ANOMALY ANALYSIS")
    print("="*60)

    anomalies = df_clean[df_clean["is_anomaly"] == True]

    # ── By year ───────────────────────────────────────────────────────────────
    print("\nAnomalies by Year:")
    year_summary = (
        df_clean.groupby("data_year")["is_anomaly"]
        .agg(["sum", "count", "mean"])
        .rename(columns={"sum": "anomalies", "count": "total", "mean": "rate"})
    )
    year_summary["rate"] = (year_summary["rate"] * 100).round(2)
    print(year_summary.to_string())

    # ── By RUCA category ──────────────────────────────────────────────────────
    print("\nAnomalies by RUCA Category:")
    ruca_summary = (
        df_clean.groupby("ruca_category")["is_anomaly"]
        .agg(["sum", "count", "mean"])
        .rename(columns={"sum": "anomalies", "count": "total", "mean": "rate"})
    )
    ruca_summary["rate"] = (ruca_summary["rate"] * 100).round(2)
    print(ruca_summary.to_string())

    # ── By CAH status ─────────────────────────────────────────────────────────
    print("\nAnomalies by CAH Status:")
    cah_summary = (
        df_clean.groupby("is_cah")["is_anomaly"]
        .agg(["sum", "count", "mean"])
        .rename(columns={"sum": "anomalies", "count": "total", "mean": "rate"})
    )
    cah_summary["rate"] = (cah_summary["rate"] * 100).round(2)
    print(cah_summary.to_string())

    # ── Top states by anomaly rate ─────────────────────────────────────────────
    print("\nTop 10 States by Anomaly Rate:")
    state_summary = (
        df_clean.groupby("Rndrng_Prvdr_State_Abrvtn")["is_anomaly"]
        .agg(["sum", "count", "mean"])
        .rename(columns={"sum": "anomalies", "count": "total", "mean": "rate"})
        .sort_values("rate", ascending=False)
        .head(10)
    )
    state_summary["rate"] = (state_summary["rate"] * 100).round(2)
    print(state_summary.to_string())

    # ── Top DRGs by anomaly rate ───────────────────────────────────────────────
    print("\nTop 10 DRGs by Anomaly Rate:")
    drg_summary = (
        df_clean.groupby(["drg_code", "drg_desc"])["is_anomaly"]
        .agg(["sum", "count", "mean"])
        .rename(columns={"sum": "anomalies", "count": "total", "mean": "rate"})
        .sort_values("rate", ascending=False)
        .head(10)
    )
    drg_summary["rate"] = (drg_summary["rate"] * 100).round(2)
    print(drg_summary.to_string())

    # ── Top anomalous providers ────────────────────────────────────────────────
    print("\nTop 10 Most Anomalous Provider-DRG Combinations:")
    top_anomalies = (
        anomalies.nsmallest(10, "anomaly_score")[
            ["provider_name", "Rndrng_Prvdr_State_Abrvtn", "drg_code",
             "drg_desc", "charge_to_payment_ratio", "avg_medicare_payment",
             "total_discharges", "anomaly_score", "is_cah", "ruca_category",
             "data_year"]
        ]
    )
    print(top_anomalies.to_string())

    return anomalies, year_summary, ruca_summary, cah_summary, state_summary


# ── 7. VISUALIZATIONS ─────────────────────────────────────────────────────────
def create_charts(df_clean, anomalies, shap_values, X_sample,
                  feature_cols, year_summary, ruca_summary,
                  cah_summary, state_summary):
    sns.set_theme(style="whitegrid")
    #fig = plt.figure(figsize=(22, 20))
    fig = plt.figure(figsize=(24, 28))
    fig.suptitle(
        "CMS Medicare Inpatient FFS 2019–2023 — Isolation Forest Anomaly Detection",
        fontsize=15, fontweight="bold", y=0.99
    )
    #gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.55, wspace=0.4)
    #gs = gridspec.GridSpec(4, 3, figure=fig, hspace=0.55, wspace=0.4)
    gs = gridspec.GridSpec(
    3, 3,
    figure=fig,
    hspace=0.55,
    wspace=0.4,
    height_ratios=[1, 1, 1.6]   # <-- Goldilocks proportions
)


    # ── Chart 1: Anomaly score distribution ───────────────────────────────────
    ax1 = fig.add_subplot(gs[0, 0])
    normal   = df_clean[df_clean["is_anomaly"] == False]["anomaly_score"]
    anomaly  = df_clean[df_clean["is_anomaly"] == True]["anomaly_score"]
    ax1.hist(normal, bins=50, alpha=0.6, color=COLORS["blue"],
             label="Normal", density=True)
    ax1.hist(anomaly, bins=50, alpha=0.6, color=COLORS["red"],
             label="Anomaly", density=True)
    ax1.set_title("Anomaly Score Distribution", fontsize=10, fontweight="bold")
    ax1.set_xlabel("Anomaly Score (lower = more anomalous)")
    ax1.set_ylabel("Density")
    ax1.legend(fontsize=8)

    # ── Chart 2: Anomaly rate by year ─────────────────────────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.plot(year_summary.index, year_summary["rate"],
             marker="o", color=COLORS["orange"], linewidth=2, markersize=8)
    for x, y in zip(year_summary.index, year_summary["rate"]):
        ax2.annotate(f"{y:.1f}%", (x, y),
                     textcoords="offset points", xytext=(0, 8),
                     ha="center", fontsize=8)
    ax2.set_title("Anomaly Rate by Year", fontsize=10, fontweight="bold")
    ax2.set_ylabel("Anomaly Rate (%)")
    ax2.set_xticks(year_summary.index)
    ax2.grid(True, alpha=0.3)

    # ── Chart 3: Anomaly rate by RUCA ─────────────────────────────────────────
    ax3 = fig.add_subplot(gs[0, 2])
    ruca_order = ["Urban", "Suburban", "Rural", "Unknown"]
    ruca_plot  = ruca_summary.reindex(
        [r for r in ruca_order if r in ruca_summary.index]
    )
    bars = ax3.bar(ruca_plot.index, ruca_plot["rate"],
                   color=[COLORS["blue"], COLORS["orange"],
                          COLORS["green"], COLORS["grey"]][:len(ruca_plot)],
                   edgecolor="white")
    for bar, val in zip(bars, ruca_plot["rate"]):
        ax3.text(bar.get_x() + bar.get_width()/2,
                 bar.get_height() * 0.95,
                 f"{val:.1f}%", ha="center", va="top",
                 color="white", fontsize=8, fontweight="bold")
    ax3.set_title("Anomaly Rate by Location", fontsize=10, fontweight="bold")
    ax3.set_ylabel("Anomaly Rate (%)")

    # ── Chart 4: Anomaly rate CAH vs Non-CAH ──────────────────────────────────
    ax4 = fig.add_subplot(gs[1, 0])
    cah_labels = {False: "Non-CAH", True: "CAH"}
    cah_plot   = cah_summary.copy()
    cah_plot.index = [cah_labels[i] for i in cah_plot.index]
    bars4 = ax4.bar(cah_plot.index, cah_plot["rate"],
                    color=[COLORS["blue"], COLORS["orange"]],
                    edgecolor="white", width=0.5)
    for bar, val in zip(bars4, cah_plot["rate"]):
        ax4.text(bar.get_x() + bar.get_width()/2,
                 bar.get_height() * 0.95,
                 f"{val:.1f}%", ha="center", va="top",
                 color="white", fontsize=9, fontweight="bold")
    ax4.set_title("Anomaly Rate\nCAH vs Non-CAH", fontsize=10, fontweight="bold")
    ax4.set_ylabel("Anomaly Rate (%)")

    # ── Chart 5: Scatter — CTP ratio vs Medicare payment colored by anomaly ───
    ax5 = fig.add_subplot(gs[1, 1:])
    sample = df_clean.sample(min(8000, len(df_clean)), random_state=RANDOM_STATE)
    normal_s  = sample[sample["is_anomaly"] == False]
    anomaly_s = sample[sample["is_anomaly"] == True]
    ax5.scatter(normal_s["charge_to_payment_ratio"],
                normal_s["avg_medicare_payment"],
                c=COLORS["blue"], alpha=0.3, s=8, label="Normal")
    ax5.scatter(anomaly_s["charge_to_payment_ratio"],
                anomaly_s["avg_medicare_payment"],
                c=COLORS["red"], alpha=0.6, s=15, label="Anomaly", zorder=5)
    ax5.set_title("CTP Ratio vs Medicare Payment\nAnomalies Highlighted",
                  fontsize=10, fontweight="bold")
    ax5.set_xlabel("Charge-to-Payment Ratio")
    ax5.set_ylabel("Avg Medicare Payment ($)")
    ax5.legend(fontsize=8)
    ax5.set_xlim(0, 30)
    ax5.grid(True, alpha=0.3)

    # ── Chart 6: Top 10 states by anomaly rate ────────────────────────────────
    ax6 = fig.add_subplot(gs[2, 0])
    ax6.barh(state_summary.index, state_summary["rate"],
             color=COLORS["red"], edgecolor="white")
    ax6.invert_yaxis()
    ax6.set_title("Top 10 States by Anomaly Rate",
                  fontsize=10, fontweight="bold")
    ax6.set_xlabel("Anomaly Rate (%)")
    ax6.grid(True, alpha=0.3, axis="x")

    # ── Chart 7: SHAP summary plot ────────────────────────────────────────────
    '''ax7 = fig.add_subplot(gs[2, 1:])
    shap.summary_plot(
        shap_values, X_sample,
        feature_names=feature_cols,
        show=False, plot_size=None,
        color_bar=True
    )
    plt.sca(ax7)
    ax7.set_title("SHAP Feature Importance\n(Impact on Anomaly Score)",
                  fontsize=10, fontweight="bold")'''
    
    # ── Chart 7: SHAP summary plot ────────────────────────────────────────────
    #ax7 = fig.add_subplot(gs[2, 1:])
    #ax7 = fig.add_subplot(gs[2:, :])


    # Create SHAP plot in its own figure
    #fig_shap, ax_temp = plt.subplots(figsize=(6, 4))
    #fig_shap = plt.figure(figsize=(12, 8))
    '''shap.summary_plot(
        shap_values,
        X_sample,
        feature_names=feature_cols,
        show=False,
        plot_size=None,
        color_bar= True
    )'''

    # Move SHAP artists into ax7
    '''for artist in fig_shap.axes[0].get_children():
        try:
            artist.remove()
            ax7.add_artist(artist)
        except Exception:
            pass

    plt.close(fig_shap)

    # Tighten layout inside the subplot
    pos = ax7.get_position()
    ax7.set_position([pos.x0 + 0.03, pos.y0, pos.width, pos.height])'''

    # Save SHAP figure to a PNG buffer
    '''import io
    buf = io.BytesIO()
    fig_shap.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig_shap)
    buf.seek(0)

    # Display the PNG inside the subplot
    import matplotlib.image as mpimg
    img = mpimg.imread(buf)
    ax7.imshow(img)
    ax7.axis("off")

   # Expand the subplot area so the image fills more space
    pos = ax7.get_position()
    ax7.set_position([
        pos.x0 - 0.02,   # shift slightly left
        pos.y0 - 0.02,   # shift slightly down
        pos.width + 0.04,  # widen
        pos.height + 0.04  # heighten
    ]) 

    
    ax7.set_title(
        "SHAP Feature Importance\n(Impact on Anomaly Score)",
        fontsize=10,
        fontweight="bold"
    )'''

    # ── Chart 7: placeholder for SHAP note ───────────────────────────────────────
    ax7 = fig.add_subplot(gs[2, 1:])
    ax7.text(0.5, 0.5, 
         "SHAP Feature Importance\nsaved separately as\ncms_shap_single_model.png",
         ha="center", va="center", fontsize=12,
         transform=ax7.transAxes,
         bbox=dict(boxstyle="round", facecolor=COLORS["grey"], alpha=0.3))
    ax7.axis("off")

    plt.savefig(OUTPUT_DIR + "cms_anomaly_detection.png",
                dpi=150, bbox_inches="tight")
    print(f"\nAnomaly detection charts saved to "
          f"{OUTPUT_DIR}cms_anomaly_detection.png")
    plt.show()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Load
    df = load_data(DB_CONFIG)

    # Feature engineering
    df, le_ruca, le_state = engineer_features(df)

    # Prepare feature matrix
    df_clean, X_scaled, feature_cols, scaler = prepare_features(df)

    # Run Isolation Forest
    df_clean, iso_forest = run_isolation_forest(
        df_clean, X_scaled, contamination=CONTAMINATION
    )

    # SHAP explainability
    shap_values, X_sample, idx_sample, feature_cols = run_shap(
        iso_forest, X_scaled, feature_cols, df_clean
    )

    # Analyze anomalies
    anomalies, year_summary, ruca_summary, cah_summary, state_summary = \
        analyze_anomalies(df_clean)

    # Visualize
    create_charts(
        df_clean, anomalies, shap_values, X_sample,
        feature_cols, year_summary, ruca_summary,
        cah_summary, state_summary
    )

    plt.figure(figsize=(12, 7))
    shap.summary_plot(shap_values, X_sample, 
                  feature_names=feature_cols,
                  show=False, color_bar=True)
    plt.title("SHAP Feature Importance — Single Model", 
          fontsize=12, fontweight="bold")
    plt.savefig(OUTPUT_DIR + "cms_shap_single_model.png", 
            dpi=150, bbox_inches="tight")
    plt.close()

    # Save anomaly results to PostgreSQL
    print("\nSaving anomaly results to PostgreSQL...")
    conn_str = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(conn_str)
    df_clean[["provider_ccn", "provider_name", "drg_code", "drg_desc",
              "data_year", "is_anomaly", "anomaly_score",
              "charge_to_payment_ratio", "avg_medicare_payment",
              "total_discharges", "ruca_category", "is_cah",
              "Rndrng_Prvdr_State_Abrvtn"]].to_sql(
        "cms_anomaly_results",
        engine,
        if_exists="replace",
        index=False,
        chunksize=5000
    )
    print("Anomaly results saved to cms_anomaly_results table.")

    print("\n── Done! ────────────────────────────────────────────────")
