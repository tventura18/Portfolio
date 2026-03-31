import pandas as pd
df_pos = pd.read_csv("/Users/toniventura/Downloads/Provider of Services File - Quality Improvement and Evaluation System/2025-Q4/Hospital_and_other.DATA.Q4_2025.csv", dtype=str, low_memory=False)
print(df_pos["CAH_SB_SW"].value_counts())
print(df_pos["PRVDR_CTGRY_SBTYP_CD"].value_counts().head(10))
print(f"Total rows: {len(df_pos):,}")