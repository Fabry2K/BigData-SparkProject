import analysis_3_1_local
import analysis_3_3_local
#import cluster_hadoop
from pyspark.sql import SparkSession

original_file = "files/flight_data_2024.csv"

file_local_3_1= "files/analisi_3_1.csv"
file_cluster_3_1 = "/input" 

file_local_3_3= "files/analisi_3_3.csv"
file_cluster_3_3 = "/input" 
      

# # cancella il contenuto del file di log
open("output/log.txt", "w").close()

# # creazione Sessione Spark per analisi in locale, una volta per tutta l'esecuzione
spark = SparkSession.builder \
    .appName("FlightAnalysis") \
    .master("local[*]") \
    .getOrCreate()


###### Analisi 3.1: job in grado di generare le statistiche di ciascuna compagnia aerea presente nel dataset#####

analysis_3_1_local.initialize_files(original_file, file_local_3_1, file_cluster_3_1)      # inizializza i file per le analisi, sia per locale che per cluster
analysis_3_1_local.analize(spark)  # analisi 3.1 locale

# cluster_spark.analyze_3_1()

# cluster_hadoop.analyze("analisi_3_1", "hadoop_3_1/mapper.py",  "hadoop_3_1/reducer.py", "output/cluster/hadoop_3_1")

###### Analisi 3.3: job in grado di generare le statistiche di ciascuna compagnia aerea presente nel dataset#####

# analysis_3_3_local.initialize_files(original_file, file_local_3_3, file_cluster_3_3)      # inizializza i file per le analisi, sia per locale che per cluster
# analysis_3_3_local.analize(spark)  # analisi 3.3 locale

# cluster_spark.analyze_3_3()

# cluster.analyze("analisi_3_3", "hadoop_3_3/mapper.py",  "hadoop_3_3/reducer.py", "output/cluster/hadoop_3_3")

# Chiusura sessioni Spark
spark.stop()