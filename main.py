import pandas as pd
import analysis
import spark_analysis
from pathlib import Path
import utils
import plot

original_file = "files/flight_data_2024.csv"
file_analysis_3_1 = "files/analisi_3_1.csv"

# cancella il contenuto del file di log
open("output/log.txt", "w").close()

###### Analisi 3.1: job in grado di generare le statistiche di ciascuna compagnia aerea presente nel dataset#####

# se non esiste il file per l'analisi 3.1, allora crealo a partire dal file originale
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

    # Creazione dei dataset di dimensione 1/4, 1/2, 2x, 4x
    utils.generate_scaled_datasets(file_analysis_3_1)

# creazione della sessione Spark
spark = spark_analysis.create_session()

# Analisi 3.1 con SPARK CORE in LOCALE

# file 1/4x
timer_3_1_quarter = spark_analysis.local_analysis(
    spark,
    "files/analisi_3_1_quarter.csv"
)

print("Analisi 3.1 con grandezza 1/4x completata")

# file 1/2x
timer_3_1_half = spark_analysis.local_analysis(
    spark,
    "files/analisi_3_1_half.csv"
)

print("Analisi 3.1 con grandezza 1/2x completata")

# file 1x
timer_3_1_normal = spark_analysis.local_analysis(
    spark,
    file_analysis_3_1
)

print("Analisi 3.1 completata")

# file 2x
timer_3_1_double = spark_analysis.local_analysis(
    spark,
    "files/analisi_3_1_double.csv"
)

print("Analisi 3.1 con grandezza 2x completata")

# file 4x
timer_3_1_quadruple = spark_analysis.local_analysis(
    spark,
    "files/analisi_3_1_quadruple.csv"
)

print("Analisi 3.1 con grandezza 4x completata")

# Chiusura Spark session
spark.stop()

plot.plot_analisi_3_1(timer_3_1_quarter, timer_3_1_half, timer_3_1_normal, timer_3_1_double, timer_3_1_quadruple)

# Analisi 3.1 con SPARK CORE in CLUSTER