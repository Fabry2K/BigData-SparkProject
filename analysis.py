import pandas as pd

# Analisi Dataframe
def analyze_dataframe(df):
    total_rows = len(df)

    null_counts = df.isnull().sum()
    unique_counts = df.nunique(dropna=True)

    report = pd.DataFrame({
        "null_count": null_counts,
        "null_%": (null_counts / total_rows) * 100,
        "unique_values": unique_counts,
        "unique_%": (unique_counts / total_rows) * 100
    })

    report["null_%"] = report["null_%"].round(2)
    report["unique_%"] = report["unique_%"].round(2)

    report = report.sort_values(by="null_%", ascending=False)

    print("=" * 100)
    print(f"Numero righe: {total_rows}")
    print(f"Numero colonne: {len(df.columns)}")
    print("=" * 100)
    print(report)

    return report



def check_duplicates(df):

    key_cols = ["fl_date", "op_unique_carrier", "op_carrier_fl_num"]

    # maschera duplicati (tutte le occorrenze)
    duplicates_mask = df.duplicated(subset=key_cols, keep=False)

    # numero totale righe duplicate
    n_duplicates = duplicates_mask.sum()

    print("=" * 80)
    print("DUPLICATE CHECK")
    print("=" * 80)
    print(f"Numero totale righe duplicate: {n_duplicates}")
    print("=" * 80)

    return n_duplicates