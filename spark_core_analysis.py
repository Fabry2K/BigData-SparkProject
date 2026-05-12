from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import utils
from pathlib import Path
import time


# creazione Sessione Spark
def create_session():
    return SparkSession.builder \
    .appName("FlightAnalysis") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()


# SPARK CORE: LOCALE
def local_analysis(spark, filepath):

    spark_df = spark.read.csv(
        filepath,
        header=True,
        inferSchema=True
    )

    # Controllo
    # spark_df.show(10)
    # spark_df.printSchema()


    # trasformazione della colonna arr_delay: se il valore è negativo viene portato a 0
    spark_df = spark_df \
        .withColumn("arr_delay", greatest(col("arr_delay"), lit(0))) \
        .withColumn("dep_delay", greatest(col("dep_delay"), lit(0)))

    start_time = time.time()

    result_3_1 = spark_df.groupBy("op_unique_carrier", "origin") \
    .agg(
        count("*").alias("num_flights"),
        min("arr_delay").alias("min_arr_delay"),
        max("arr_delay").alias("max_arr_delay"),
        avg("arr_delay").alias("avg_arr_delay"),
        (sum("cancelled") / count("*")).alias("cancellation_rate"),
        collect_set("month").alias("months_active")
    )

    result_3_1.collect()

    end_time = time.time()

    execution_time = end_time - start_time

    # stampa e salvataggio del risultato
    output = result_3_1._jdf.showString(20, 0, False)

    print(output)
    print(f"\nExecution time: {execution_time:.2f} seconds")

    utils.append_to_log(
        "Risultati analisi 3.1 sul file " + Path(filepath).stem,
        output + f"\n\nExecution time: {execution_time:.2f} seconds"
    )

    return execution_time