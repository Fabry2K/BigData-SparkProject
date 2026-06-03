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
from utils import run_cmd

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


    run_cmd("hdfs dfs -mkdir -p /input")

    print("Inizializzazione file per analisi 3.1 in corso...")
    # crea il file locale, se non esiste
    if not Path(file_local).exists():
        print("File locale non presente, creazione in corso...")

        df = pd.read_csv(original_file, dtype=str)
        print("CSV originale letto correttamente")

        # droppo le righe con diverted = 1, in quanto non sono voli effettivamente partiti e quindi non hanno senso per l'analisi 3.1
        df = df[df["diverted"] == '0']

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

# ----------------------------
# UPLOAD FUNCTIONS
#-----------------------------
# - Mapper, reducer
# - Input
# ----------------------------

def get_map_red(analisi, file):

    if analisi == "analisi_3_1":
        return f"hadoop_3_1/{file}"
    else:
        return f"hadoop_3_3/{file}"



def get_input(analisi):

    mapper = get_map_red(analisi, "mapper.py")
    reducer = get_map_red(analisi, "reducer.py")

    input = {
            "quarter": f"files/{analisi}_quarter.csv",
            "half": f"files/{analisi}_half.csv",
            "normal": f"files/{analisi}.csv",
            "double": f"files/{analisi}_double.csv",
            "quadruple": f"files/{analisi}_quadruple.csv"
        }
    
    return mapper, reducer, input

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# analisi con spark core in locale
def analyze_with_spark_core(spark):

    # SPARK CORE locale
    # analisi file 1/4x
    timer_spark_3_1_quarter = spark_core_analysis.analysis_3_1(
        spark,
        "/input/analisi_3_1_quarter.csv"
    )
    print("Analisi 3.1 SPARK CORE locale con grandezza 1/4x completata")

    # analisi file 1/2x
    timer_spark_3_1_half = spark_core_analysis.analysis_3_1(
        spark,
        "/input/analisi_3_1_half.csv"
    )
    print("Analisi 3.1 SPARK CORE locale con grandezza 1/2x completata")

    # analisi file 1x
    timer_spark_3_1_normal = spark_core_analysis.analysis_3_1(
        spark,
        "/input/analisi_3_1.csv"
    )
    print("Analisi 3.1 SPARK CORE locale completata")

    # analisi file 2x
    timer_spark_3_1_double = spark_core_analysis.analysis_3_1(
        spark,
        "/input/analisi_3_1_double.csv"
    )
    print("Analisi 3.1 SPARK CORE locale con grandezza 2x completata")

    # analisi file 4x
    timer_spark_3_1_quadruple = spark_core_analysis.analysis_3_1(
        spark,
        "/input/analisi_3_1_quadruple.csv"
    )
    print("Analisi 3.1 SPARK CORE locale con grandezza 4x completata")

    # plot dei tempi SPARK CORE locale
    plot.plot_analisi(timer_spark_3_1_quarter, timer_spark_3_1_half, timer_spark_3_1_normal, timer_spark_3_1_double, timer_spark_3_1_quadruple, "Analisi 3.1 Spark Core Locale", "output/spark_core_local_analysis_3_1.png")

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# analisi con spark sql in locale
def analyze_with_spark_sql(spark):

    # SPARK SQL in LOCALE
    # file 1/4x
    timer_spark_sql_3_1_quarter = spark_sql_analysis.analysis_3_1(
        spark,
        "/input/analisi_3_1_quarter.csv"
    )
    print("Analisi 3.1 SPARK SQL locale con grandezza 1/4x completata")

    # file 1/2x
    timer_spark_sql_3_1_half = spark_sql_analysis.analysis_3_1(
        spark,
        "/input/analisi_3_1_half.csv"
    )
    print("Analisi 3.1 SPARK SQL locale con grandezza 1/2x completata")

    # file 1x
    timer_spark_sql_3_1_normal = spark_sql_analysis.analysis_3_1(
        spark,
        "/input/analisi_3_1.csv"

    )
    print("Analisi 3.1 SPARK SQL locale completata")

    # file 2x
    timer_spark_sql_3_1_double = spark_sql_analysis.analysis_3_1(
        spark,
        "/input/analisi_3_1_double.csv"
    )
    print("Analisi 3.1 SPARK SQL locale con grandezza 2x completata")

    # file 4x
    timer_spark_sql_3_1_quadruple = spark_sql_analysis.analysis_3_1(
        spark,
        "/input/analisi_3_1_quadruple.csv"
    )
    print("Analisi 3.1 SPARK SQL locale con grandezza 4x completata")

    # plot dei tempi SPARK SQL locale
    plot.plot_analisi(timer_spark_sql_3_1_quarter, timer_spark_sql_3_1_half, timer_spark_sql_3_1_normal, timer_spark_sql_3_1_double, timer_spark_sql_3_1_quadruple, "Analisi 3.1 Spark SQL Locale", "output/spark_sql_local_analysis_3_1.png")

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
def hadoop_analysis(analisi):
# --------------------------------------------
#   HADOOP MAPREDUCE
# --------------------------------------------

#   - Esecuzione HADOOP analysis 
#   - Plot tempi di esecuzione per dimensione

# --------------------------------------------

  # HADOOP output log
    log_path = utils.path_existence(f"output/local/hadoop/{analisi}log.txt")

    mapper, reducer, input = get_input(analisi)

#   # Esecuzione Hadoop MapReduce local
    timer = hadoop_executor(mapper, reducer, input, None, analisi, "hadoop_3_1_output", log_path)

#   # plot dei tempi HADOOP 
    plot.plot_analisi(timer.get("quarter"), timer.get("half"), timer.get("normal"), timer.get("double"), timer.get("quadruple"), f"{analisi} Hadoop Map Reduce", f"output/local/hadoop/hadoop_{analisi}.png")

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        
# # analisi in locale
def analize(spark):

    # analyze_with_spark_core(spark)

    # analyze_with_spark_sql(spark)

#     # hive_analysis()

    hadoop_analysis("analisi_3_1")
    hadoop_analysis("analisi_3_3")

