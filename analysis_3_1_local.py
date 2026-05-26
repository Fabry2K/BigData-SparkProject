import spark_core_analysis
import spark_sql_analysis
from hadoop_exec import hadoop_executor
import os
import hive_analysis
from pathlib import Path
import utils
import plot
from py4j.java_gateway import java_import
from pyspark.sql.functions import col, split
from pathlib import Path
import pandas as pd

###### Analisi 3.1: job in grado di generare le statistiche di ciascuna compagnia aerea presente nel dataset#####        

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

# inizializza i file sia per l'analisi in locale che su cluster
def initialize_files(original_file, file_local, hdfs_input_path):

    # crea il file locale, se non esiste
    if not Path(file_local).exists():
        print("File locale non presente, creazione in corso...")

        df = pd.read_csv(original_file, dtype=str)
        print("CSV originale letto correttamente")


        df_base = df[cols_to_keep].copy()

        # pulizia decimali
        for col in ["dep_delay", "arr_delay"]:
            df_base[col] = df_base[col].astype(str).str.split(".").str[0]

        df_base.to_csv(file_local, index=False)
        print("File locale creato")

        # dataset scalati
        utils.generate_scaled_datasets_local(file_local)

    else:
        print("File locale già presente")


    # lista file da caricare
    local_files = [
        file_local,
        file_local.replace(".csv", "_quarter.csv"),
        file_local.replace(".csv", "_half.csv"),
        file_local.replace(".csv", "_double.csv"),
        file_local.replace(".csv", "_quadruple.csv"),
    ]




    # upload su hdfs solo se non esistono
    for local_file in local_files:

        hdfs_path = f"{hdfs_input_path}/{Path(local_file).name}"




        if not utils.hdfs_exists(hdfs_path):
            print(f"Uploading {local_file} -> {hdfs_path}")
            utils.hdfs_put(local_file, hdfs_path)
        else:
            print(f"{hdfs_path} già presente su HDFS")

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# analisi con spark core in locale
# def analyze_with_spark_core(spark):

#     # SPARK CORE locale
#     # analisi file 1/4x
#     timer_spark_3_1_quarter = spark_core_analysis.analysis_3_1(
#         spark,
#         "/input/analisi_3_1_quarter.csv"
#     )
#     print("Analisi 3.1 SPARK CORE locale con grandezza 1/4x completata")

#     # analisi file 1/2x
#     timer_spark_3_1_half = spark_core_analysis.analysis_3_1(
#         spark,
#         "/input/analisi_3_1_half.csv"
#     )
#     print("Analisi 3.1 SPARK CORE locale con grandezza 1/2x completata")

#     # analisi file 1x
#     timer_spark_3_1_normal = spark_core_analysis.analysis_3_1(
#         spark,
#         "/input/analisi_3_1.csv"
#     )
#     print("Analisi 3.1 SPARK CORE locale completata")

#     # analisi file 2x
#     timer_spark_3_1_double = spark_core_analysis.analysis_3_1(
#         spark,
#         "/input/analisi_3_1_double.csv"
#     )
#     print("Analisi 3.1 SPARK CORE locale con grandezza 2x completata")

#     # analisi file 4x
#     timer_spark_3_1_quadruple = spark_core_analysis.analysis_3_1(
#         spark,
#         "/input/analisi_3_1_quadruple.csv"
#     )
#     print("Analisi 3.1 SPARK CORE locale con grandezza 4x completata")

#     # plot dei tempi SPARK CORE locale
#     plot.plot_analisi(timer_spark_3_1_quarter, timer_spark_3_1_half, timer_spark_3_1_normal, timer_spark_3_1_double, timer_spark_3_1_quadruple, "Analisi 3.1 Spark Core Locale", "output/spark_core_local_analysis_3_1.png")

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# analisi con spark sql in locale
# def analyze_with_spark_sql(spark):

#     # SPARK SQL in LOCALE
#     # file 1/4x
#     timer_spark_sql_3_1_quarter = spark_sql_analysis.analysis_3_1(
#         spark,
#         "/input/analisi_3_1_quarter.csv"
#     )
#     print("Analisi 3.1 SPARK SQL locale con grandezza 1/4x completata")

#     # file 1/2x
#     timer_spark_sql_3_1_half = spark_sql_analysis.analysis_3_1(
#         spark,
#         "/input/analisi_3_1_half.csv"
#     )
#     print("Analisi 3.1 SPARK SQL locale con grandezza 1/2x completata")

#     # file 1x
#     timer_spark_sql_3_1_normal = spark_sql_analysis.analysis_3_1(
#         spark,
#         "/input/analisi_3_1.csv"

#     )
#     print("Analisi 3.1 SPARK SQL locale completata")

#     # file 2x
#     timer_spark_sql_3_1_double = spark_sql_analysis.analysis_3_1(
#         spark,
#         "/input/analisi_3_1_double.csv"
#     )
#     print("Analisi 3.1 SPARK SQL locale con grandezza 2x completata")

#     # file 4x
#     timer_spark_sql_3_1_quadruple = spark_sql_analysis.analysis_3_1(
#         spark,
#         "/input/analisi_3_1_quadruple.csv"
#     )
#     print("Analisi 3.1 SPARK SQL locale con grandezza 4x completata")

#     # plot dei tempi SPARK SQL locale
#     plot.plot_analisi(timer_spark_sql_3_1_quarter, timer_spark_sql_3_1_half, timer_spark_sql_3_1_normal, timer_spark_sql_3_1_double, timer_spark_sql_3_1_quadruple, "Analisi 3.1 Spark SQL Locale", "output/spark_sql_local_analysis_3_1.png")

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Analisi 3.1 con HIVE in LOCAL
# def hive_analysis():
#     hive_analysis.hive_setup()

#     hive_analysis.hive_load("files/analisi_3_1_quarter.csv")
#     time_hive_quarter = hive_analysis.hive_query_3_1_local("output/log.txt")

#     hive_analysis.hive_load("files/analisi_3_1_half.csv")
#     time_hive_half = hive_analysis.hive_query_3_1_local("output/log.txt")

#     hive_analysis.hive_load("files/analisi_3_1.csv")
#     time_hive = hive_analysis.hive_query_3_1_local("output/log.txt")

#     hive_analysis.hive_load("files/analisi_3_1_double.csv")
#     time_hive_double = hive_analysis.hive_query_3_1_local("output/log.txt")

#     hive_analysis.hive_load("files/analisi_3_1_quadruple.csv")
#     time_hive_quadruple = hive_analysis.hive_query_3_1_local("output/log.txt")


#     plot.plot_analisi(time_hive_quarter, time_hive_half, time_hive, time_hive_double, time_hive_quadruple, "Analisi 3.1 Hive Locale", "output/hive_local_analysis_3_1.png")

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# analisi con hadoop in locale
def hadoop_analysis():
# --------------------------------------------
#   HADOOP MAPREDUCE
# --------------------------------------------

#   - Esecuzione HADOOP analysis 3.1
#   - Plot tempi di esecuzione per dimensione

#   - Esecuzione HADOOP cluster AWS
#   - Plot tempi di esecuzione per dimensione

# --------------------------------------------

  # HADOOP output log
    log_path = "output/log_hadooop_3_1.txt"

    # elimina se esiste
    if os.path.exists(log_path):
        os.remove(log_path)

    # ricrea il file (vuoto)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

#   # Esecuzione Hadoop MapReduce su un quarto, metà, intera, doppia e quadrupla dimensione del file di input

#   # file 1/4x
    timer_hadoop_3_1_quarter = hadoop_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_quarter", None, "analisi_3_1.csv", "hadoop_3_1_output", log_path)
    
#   # file 1/2x
    timer_hadoop_3_1_half = hadoop_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_half", None, "analisi_3_1.csv", "hadoop_3_1_output", log_path)

#   # file 1x
    timer_hadoop_3_1 = hadoop_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1", "output/hadoop_3_1_output", "analisi_3_1.csv", "hadoop_3_1_output", log_path)

#   # file 2x
    timer_hadoop_3_1_double = hadoop_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_double", None, "analisi_3_1.csv", "hadoop_3_1_output", log_path)

#   # file 4x
    timer_hadoop_3_1_quadruple = hadoop_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_quadruple", None, "analisi_3_1.csv", "hadoop_3_1_output", log_path)

#   # plot dei tempi HADOOP 
    plot.plot_analisi(timer_hadoop_3_1_quarter, timer_hadoop_3_1_half, timer_hadoop_3_1, timer_hadoop_3_1_double, timer_hadoop_3_1_quadruple, "Analisi 3.1 Hadoop Map Reduce", "output/hadoop_analysis_3_1.png")

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        
# # analisi in locale
# def analize(spark):

#     analyze_with_spark_core(spark)

#     analyze_with_spark_sql(spark)

#     # hive_analysis()

#     hadoop_analysis()