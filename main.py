import analysis_3_1
import analysis_3_3
from pyspark.sql import SparkSession

original_file = "files/flight_data_2024.csv"

file_local_3_1= "files/analisi_3_1.csv"
file_cluster_3_1 = "/input" 

file_local_3_3= "files/analisi_3_3.csv"
file_cluster_3_3 = "/input" 
      

# # cancella il contenuto del file di log
# open("output/log.txt", "w").close()

# # creazione Sessione Spark per analisi in locale, una volta per tutta l'esecuzione
# spark = SparkSession.builder \
#     .appName("FlightAnalysis") \
#     .master("local[*]") \
#     .getOrCreate()


###### Analisi 3.1: job in grado di generare le statistiche di ciascuna compagnia aerea presente nel dataset#####

analysis_3_1.initialize_files(original_file, file_local_3_1, file_cluster_3_1)      # inizializza i file per le analisi, sia per locale che per cluster

analysis_3_1.analize_local(None)  # analisi 3.1 locale



###### Analisi 3.3: job in grado di generare le statistiche di ciascuna compagnia aerea presente nel dataset#####

analysis_3_3.initialize_files(original_file, file_local_3_3, file_cluster_3_3)      # inizializza i file per le analisi, sia per locale che per cluster

analysis_3_3.analize_local(None)  # analisi 3.3 locale


# Chiusura sessioni Spark
# spark.stop()