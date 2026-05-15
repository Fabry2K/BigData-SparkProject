import spark_core_analysis
import spark_sql_analysis
import hadoop_analysis
from pathlib import Path
import utils
import plot
from py4j.java_gateway import java_import
from pyspark.sql.functions import col, split
from pathlib import Path
import pandas as pd

###### Analisi 3.3: job in grado di generare, per ciascun aeroporto di partenza e per ciascuna compagnia aerea, un report
# di performance che confronti il comportamento della compagnia in quell’aeroporto con il comportamento medio di
# tutte le compagnie che operano nello stesso aeroporto#####

file_local= "files/analisi_3_3.csv"
file_cluster = "hdfs://localhost:9000/data/analisi_3_3.csv"

cols_to_keep = [
    "op_unique_carrier",
    "origin",
    "dep_delay",
    "arr_delay",
    "cancelled"
]

# controlla l'esistenza del file all'interno di hdfs
def exists_in_hdfs(spark, path):
    # qui spark serve solo come "accesso semplificato" all'hdfs, si può fare anche senza
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))


# inizializza i file sia per l'analisi in locale che su cluster
def initialize_files(spark_local, spark_cluster, original_file):

    # inizializzazione file in locale (usa pandas perchè spark salva i file in partizioni)
    if not Path(file_local).exists():
        print("file per analisi 3.3 in locale non presente, si procede con la creazione")

        df = pd.read_csv(original_file, dtype=str)
        print("file csv originale letto correttamente")

        # selezione delle colonne
        df_base = df[cols_to_keep].copy()

        # pulizia colonne, parte decimale
        for col in ["dep_delay", "arr_delay"]:
            df_base[col] = df_base[col].str.split(".").str[0]

        # TODO: controlli null o record non significativi

        # salvataggio file in locale
        df_base.to_csv(file_local, index=False)
        print("file csv per analisi 3.3 creato correttamente")

        # creazione dei dataset di dimensione 1/4, 1/2, 2x, 4x
        utils.generate_scaled_datasets_local(file_local)
    else: 
        print("file per analisi 3.3 in locale già presenti")
        
    # inizializzazione file su cluster
    # if not exists_in_hdfs(spark, file_cluster):

    #     print("file per analisi 3.3 su cluster non presente, si procede con la generazione di tutti i file")

    #     df = spark_cluster.read.csv(original_file, header=True, inferSchema=True)        # analysis.analyze_dataframe(df)
    #     # analysis.check_duplicates(df)
    #     print("file csv originale letto correttamente")

    #     # selezione delle colonne
    #     df_base = df.select(cols_to_keep)

    #     # pulizia colonne, parte decimale
    #     for c in ["op_carrier_fl_num", "dep_delay", "arr_delay", "distance"]:
    #         df_base = df_base.withColumn(c, split(col(c), "\.").getItem(0))

    #     # creazione dei dataset di dimensione 1/4, 1/2, 2x, 4x
    #     utils.generate_scaled_datasets_cluster(spark_cluster, file_cluster)
    # else: 
    #     print("file per analisi 3.3 in cluster già presenti")



# analisi in locale
def analize_local(spark):

    # SPARK CORE locale
    # analisi file 1/4x
    timer_spark_3_3_quarter = spark_core_analysis.local_analysis_3_3(
        spark,
        "files/analisi_3_3_quarter.csv"
    )
    print("Analisi 3.3 SPARK CORE locale con grandezza 1/4x completata")

    # analisi file 1/2x
    timer_spark_3_3_half = spark_core_analysis.local_analysis_3_3(
        spark,
        "files/analisi_3_3_half.csv"
    )
    print("Analisi 3.3 SPARK CORE locale con grandezza 1/2x completata")

    # analisi file 1x
    timer_spark_3_3_normal = spark_core_analysis.local_analysis_3_3(
        spark,
        file_local
    )
    print("Analisi 3.3 SPARK CORE locale completata")

    # analisi file 2x
    timer_spark_3_3_double = spark_core_analysis.local_analysis_3_3(
        spark,
        "files/analisi_3_3_double.csv"
    )
    print("Analisi 3.3 SPARK CORE locale con grandezza 2x completata")

    # analisi file 4x
    timer_spark_3_3_quadruple = spark_core_analysis.local_analysis_3_3(
        spark,
        "files/analisi_3_3_quadruple.csv"
    )
    print("Analisi 3.3 SPARK CORE locale con grandezza 4x completata")

    # plot dei tempi SPARK CORE locale
    plot.plot_analisi(timer_spark_3_3_quarter, timer_spark_3_3_half, timer_spark_3_3_normal, timer_spark_3_3_double, timer_spark_3_3_quadruple, "Analisi 3.3 Spark Core Locale", "output/spark_core_local_analysis_3_3.png")

    # SPARK SQL in LOCALE
    # file 1/4x
    timer_spark_sql_3_3_quarter = spark_sql_analysis.local_analysis_3_3(
        spark,
        "files/analisi_3_3_quarter.csv"
    )
    print("Analisi 3.3 SPARK SQL locale con grandezza 1/4x completata")

    # file 1/2x
    timer_spark_sql_3_3_half = spark_sql_analysis.local_analysis_3_3(
        spark,
        "files/analisi_3_3_half.csv"
    )
    print("Analisi 3.3 SPARK SQL locale con grandezza 1/2x completata")

    # file 1x
    timer_spark_sql_3_3_normal = spark_sql_analysis.local_analysis_3_3(
        spark,
        file_local
    )
    print("Analisi 3.3 SPARK SQL locale completata")

    # file 2x
    timer_spark_sql_3_3_double = spark_sql_analysis.local_analysis_3_3(
        spark,
        "files/analisi_3_3_double.csv"
    )
    print("Analisi 3.3 SPARK SQL locale con grandezza 2x completata")

    # file 4x
    timer_spark_sql_3_3_quadruple = spark_sql_analysis.local_analysis_3_3(
        spark,
        "files/analisi_3_3_quadruple.csv"
    )
    print("Analisi 3.3 SPARK SQL locale con grandezza 4x completata")

    # plot dei tempi SPARK SQL locale
    plot.plot_analisi(timer_spark_sql_3_3_quarter, timer_spark_sql_3_3_half, timer_spark_sql_3_3_normal, timer_spark_sql_3_3_double, timer_spark_sql_3_3_quadruple, "Analisi 3.3 Spark SQL Locale", "output/spark_sql_local_analysis_3_3.png")


    # analisi HADOOP MAPREDUCE in locale
    # file 1/4x
    timer_hadoop_3_3_quarter = hadoop_analysis.local_analysis_3_3(
        "files/analisi_3_3_quarter.csv",
        "output/log.txt"
    )

    # file 1/2x
    timer_hadoop_3_3_half = hadoop_analysis.local_analysis_3_3(
        "files/analisi_3_3_half.csv",
        "output/log.txt"
    )

    # file 1x
    timer_hadoop_3_3 = hadoop_analysis.local_analysis_3_3(
        file_local,
        "output/log.txt"
    )

    # file 2x
    timer_hadoop_3_3_double = hadoop_analysis.local_analysis_3_3(
        "files/analisi_3_3_double.csv",
        "output/log.txt"
    )

    # file 4x
    timer_hadoop_3_3_quadruple = hadoop_analysis.local_analysis_3_3(
        "files/analisi_3_3_quadruple.csv",
        "output/log.txt"
    )

    # plot dei tempi HADOOP locale
    plot.plot_analisi(timer_hadoop_3_3_quarter, timer_hadoop_3_3_half, timer_hadoop_3_3, timer_hadoop_3_3_double, timer_hadoop_3_3_quadruple, "Analisi 3.3 Hadoop Map Reduce Locale", "output/hadoop_local_analysis_3_3.png")


   
# analisi in cluster
def analize_cluster(spark, original_file):
    print("TODO")
    
