import pandas as pd
import analysis
import spark_analysis
from pathlib import Path

original_file = "files/flight_data_2024.csv"
file_analysis_3_1 = "files/analisi_3_1.csv"

# Analisi 3.1: job in grado di generare le statistiche di ciascuna compagnia aerea presente nel dataset

# se non essite il file per l'analisi 3.1, allora crealo a partire dal file originale
if not Path(file_analysis_3_1).exists():

    print("file per analisi 3.1 non presente, si procede con la creazione")

    df = pd.read_csv(original_file, dtype=str)
    # analysis.analyze_dataframe(df)
    # analysis.check_duplicates(df)
    print("file csv originale letto correttamente")

    # colonne utili per 3.1
    cols_to_keep = [
        "fl_date",
        "month",
        "op_unique_carrier",
        "op_carrier_fl_num",
        "origin",
        "dest",
        "dep_delay",
        "arr_delay",
        "cancelled",
        "cancellation_code",
        "distance"
    ]

    df_analisi = df[cols_to_keep].copy()

    # Rimozione parte decimale dalle colonne in cols
    for col in ["op_carrier_fl_num", "dep_delay", "arr_delay", "distance"]:
        df_analisi[col] = df_analisi[col].str.split(".").str[0]

    # analysis.analyze_dataframe(df_analisi)
    # print(df_analisi.head(30))

    df_analisi.to_csv(file_analysis_3_1, index=False)
    print("file csv per analisi 3.1 creato correttamente")

# Analisi 3.1 con SPARK CORE: LOCALE
spark_analysis.local_analysis(file_analysis_3_1)
print("Analisi 3.1 completata")

# QUERY IN CLUSTER
