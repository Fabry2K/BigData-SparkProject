import pandas as pd
import analysis
from pyspark.sql import SparkSession

# df = pd.read_csv("files/flight_data_2024.csv", dtype=str)
# # analysis.analyze_dataframe(df)
# # analysis.check_duplicates(df)


# # Analisi 3.1: job in grado di generare le statistiche di ciascuna compagnia aerea presente nel dataset

# # colonne utili per 3.1
# cols_to_keep = [
#     "fl_date",
#     "month",
#     "op_unique_carrier",
#     "op_carrier_fl_num",
#     "origin",
#     "dest",
#     "dep_delay",
#     "arr_delay",
#     "cancelled",
#     "cancellation_code",
#     "distance"
# ]

# df_analisi = df[cols_to_keep].copy()

# # Rimozione parte decimale dalle colonne in cols
# for col in ["op_carrier_fl_num", "dep_delay", "arr_delay", "distance"]:
#     df_analisi[col] = df_analisi[col].str.split(".").str[0]

# # analysis.analyze_dataframe(df_analisi)
# # print(df_analisi.head(30))

# df.to_csv("files/flight_data_2024_analisi_3_1.csv", index=False)

# SPARK CORE: LOCALE

spark = SparkSession.builder \
    .appName("FlightAnalysis") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark_df = spark.read.csv(
    "files/flight_data_2024_analisi_3_1.csv",
    header=True,
    inferSchema=True
)


# Controllo
spark_df.show(10)
spark_df.printSchema()

# QUERY IN CLUSTER

