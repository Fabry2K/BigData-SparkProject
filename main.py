import pandas as pd
import analysis
import spark_core_analysis
import spark_sql_analysis
import hadoop_analysis
import hive_analysis
from pathlib import Path
import utils
import plot
import subprocess
import time



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
spark = spark_core_analysis.create_session()

# Analisi 3.1 con SPARK CORE in LOCALE

# file 1/4x
timer_spark_3_1_quarter = spark_core_analysis.local_analysis(
    spark,
    "files/analisi_3_1_quarter.csv"
)
print("Analisi 3.1 SPARK CORE locale con grandezza 1/4x completata")

# file 1/2x
timer_spark_3_1_half = spark_core_analysis.local_analysis(
    spark,
    "files/analisi_3_1_half.csv"
)
print("Analisi 3.1 SPARK CORE locale con grandezza 1/2x completata")

# file 1x
timer_spark_3_1_normal = spark_core_analysis.local_analysis(
    spark,
    file_analysis_3_1
)
print("Analisi 3.1 SPARK CORE locale completata")

# file 2x
timer_spark_3_1_double = spark_core_analysis.local_analysis(
    spark,
    "files/analisi_3_1_double.csv"
)
print("Analisi 3.1 SPARK CORE locale con grandezza 2x completata")
# file 4x
timer_spark_3_1_quadruple = spark_core_analysis.local_analysis(
    spark,
    "files/analisi_3_1_quadruple.csv"
)
print("Analisi 3.1 SPARK CORE locale con grandezza 4x completata")


plot.plot_analisi_3_1(timer_spark_3_1_quarter, timer_spark_3_1_half, timer_spark_3_1_normal, timer_spark_3_1_double, timer_spark_3_1_quadruple, "Analisi 3.1 Spark Core Locale", "output/spark_core_local_analysis_3_1.png")

# Analisi 3.1 con SPARK CORE in CLUSTER
# TODO




# Analisi 3.1 con SPARK SQL in LOCALE

# file 1/4x
timer_spark_sql_3_1_quarter = spark_sql_analysis.local_analysis(
    spark,
    "files/analisi_3_1_quarter.csv"
)
print("Analisi 3.1 SPARK SQL locale con grandezza 1/4x completata")

# file 1/2x
timer_spark_sql_3_1_half = spark_sql_analysis.local_analysis(
    spark,
    "files/analisi_3_1_half.csv"
)
print("Analisi 3.1 SPARK SQL locale con grandezza 1/2x completata")

# file 1x
timer_spark_sql_3_1_normal = spark_sql_analysis.local_analysis(
    spark,
    file_analysis_3_1
)
print("Analisi 3.1 SPARK SQL locale completata")

# file 2x
timer_spark_sql_3_1_double = spark_sql_analysis.local_analysis(
    spark,
    "files/analisi_3_1_double.csv"
)
print("Analisi 3.1 SPARK SQL locale con grandezza 2x completata")

# file 4x
timer_spark_sql_3_1_quadruple = spark_sql_analysis.local_analysis(
    spark,
    "files/analisi_3_1_quadruple.csv"
)
print("Analisi 3.1 SPARK SQL locale con grandezza 4x completata")


plot.plot_analisi_3_1(timer_spark_sql_3_1_quarter, timer_spark_sql_3_1_half, timer_spark_sql_3_1_normal, timer_spark_sql_3_1_double, timer_spark_sql_3_1_quadruple, "Analisi 3.1 Spark SQL Locale", "output/spark_sql_local_analysis_3_1.png")

# Analisi 3.1 con SPARK SQL in CLUSTER
# TODO


# Analisi 3.1 con HADOOP MAPREDUCE in LOCALE

timer_hadoop_3_1_quarter = hadoop_analysis.local_analysis(
    "files/analisi_3_1_quarter.csv",
    "output/log.txt"
)

timer_hadoop_3_1_half = hadoop_analysis.local_analysis(
    "files/analisi_3_1_half.csv",
    "output/log.txt"
)

timer_hadoop_3_1 = hadoop_analysis.local_analysis(
    "files/analisi_3_1.csv",
    "output/log.txt"
)

timer_hadoop_3_1_double = hadoop_analysis.local_analysis(
    "files/analisi_3_1_double.csv",
    "output/log.txt"
)

timer_hadoop_3_1_quadruple = hadoop_analysis.local_analysis(
    "files/analisi_3_1_quadruple.csv",
    "output/log.txt"
)

plot.plot_analisi_3_1(timer_hadoop_3_1_quarter, timer_hadoop_3_1_half, timer_hadoop_3_1, timer_hadoop_3_1_double, timer_hadoop_3_1_quadruple, "Analisi 3.1 Hadoop Map Reduce Locale", "output/hadoop_local_analysis_3_1.png")


# Analisi 3.1 con HADOOP MAPREDUCE in CLUSTER
# TODO


# # Analisi 3.1 con HIVE in LOCAL
# hive_analysis.hive_setup()

# hive_analysis.hive_load("files/analisi_3_1_quarter.csv")
# time_hive_quarter = hive_analysis.hive_query_3_1_local("output/log.txt")

# hive_analysis.hive_load("files/analisi_3_1_half.csv")
# time_hive_half = hive_analysis.hive_query_3_1_local("output/log.txt")

# hive_analysis.hive_load("files/analisi_3_1.csv")
# time_hive = hive_analysis.hive_query_3_1_local("output/log.txt")

# hive_analysis.hive_load("files/analisi_3_1_double.csv")
# time_hive_double = hive_analysis.hive_query_3_1_local("output/log.txt")

# hive_analysis.hive_load("files/analisi_3_1_quadruple.csv")
# time_hive_quadruple = hive_analysis.hive_query_3_1_local("output/log.txt")


# plot.plot_analisi_3_1(time_hive_quarter, time_hive_half, time_hive, time_hive_double, time_hive_quadruple, "Analisi 3.1 Hive Locale", "output/hive_local_analysis_3_1.png")


# Analisi 3.1 con HIVE in CLUSTER
# TODO

# Chiusura Spark session
spark.stop()