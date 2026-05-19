import analysis_3_1
import analysis_3_3
from pyspark.sql import SparkSession

original_file = "files/flight_data_2024.csv"

# cancella il contenuto del file di log
open("output/log.txt", "w").close()

# creazione Sessione Spark per analisi in locale, una volta per tutta l'esecuzione
spark_local = SparkSession.builder \
    .appName("FlightAnalysis") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# creazione Sessione Spark per analisi su cluster, una volta per tutta l'esecuzione
spark_cluster = SparkSession.builder \
    .appName("FlightAnalysis") \
    .master("local[*]") \
    .getOrCreate()


# TODO: analisi record significativi e nulli


###### Analisi 3.1: job in grado di generare le statistiche di ciascuna compagnia aerea presente nel dataset#####

analysis_3_1.initialize_files(spark_local, spark_cluster, original_file)      # inizializza i file per le analisi, sia per locale che per cluster

analysis_3_1.analize_local(spark_local)  # analisi 3.1 locale



###### Analisi 3.3: job in grado di generare le statistiche di ciascuna compagnia aerea presente nel dataset#####

analysis_3_3.initialize_files(spark_local, spark_cluster, original_file)      # inizializza i file per le analisi, sia per locale che per cluster

analysis_3_3.analize_local(spark_local)  # analisi 3.1 locale


# Chiusura sessioni Spark
spark_local.stop()
spark_cluster.stop()