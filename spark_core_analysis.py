from pyspark.sql.functions import *
from pathlib import Path
import time
from pyspark.sql.window import Window
import os
from datetime import datetime

# funzione di cleaning
def clean_flight_df(df):

    df = df \
        .withColumn("arr_delay", col("arr_delay").cast("double")) \
        .withColumn("dep_delay", col("dep_delay").cast("double")) \
        .withColumn("cancelled", col("cancelled").cast("double"))

    # elimina null/NaN
    df = df.fillna({
        "arr_delay": 0.0,
        "dep_delay": 0.0,
        "cancelled": 0.0
    })

    # sicurezza extra (valori negativi → 0)
    df = df \
        .withColumn("arr_delay", when(col("arr_delay") < 0, 0).otherwise(col("arr_delay"))) \
        .withColumn("dep_delay", when(col("dep_delay") < 0, 0).otherwise(col("dep_delay")))

    return df

# funzione per salvare i log
def save_spark_log(output_df, execution_time, log_file, title="Spark Analysis"):

    # crea directory se non esiste
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # prende output testuale (come facevi con Hadoop)
    output_text = output_df._jdf.showString(20, 0, False)

    with open(log_file, "a", encoding="utf-8") as f:

        f.write("\n")
        f.write("=" * 70 + "\n")
        f.write(f"{title}\n")
        f.write(f"Execution timestamp: {datetime.now()}\n")
        f.write(f"Execution time: {execution_time:.2f} seconds\n")

        f.write("\nRESULTS (TOP 20)\n")
        f.write("-" * 70 + "\n")
        f.write(output_text + "\n")

        f.write("\n")

# funzione per analisi 3.1 con spark core su hdfs in locale
def analysis_3_1(spark, hdfs_input_file):

    df = spark.read.csv(
        hdfs_input_file,
        header=True,
        inferSchema=True
    )

    # data cleaning
    df = clean_flight_df(df)

    start_time = time.time()

    result = df.groupBy("op_unique_carrier", "origin").agg(
        count("*").alias("num_flights"),
        min("arr_delay").alias("min_arr_delay"),
        max("arr_delay").alias("max_arr_delay"),
        avg("arr_delay").alias("avg_arr_delay"),
        (sum("cancelled") / count("*")).alias("cancellation_rate")
    )

    result.collect()

    execution_time = time.time() - start_time

    output = result._jdf.showString(20, 0, False)

    print(output)
    print(f"Execution time: {execution_time:.2f}")

    save_spark_log(
    result,
    execution_time,
    "output/local/spark/spark_core_local.txt",
    title="Analisi 3.1 SPARK CORE"
    )

    return execution_time


# funzione per analisi 3.3 con spark core su hdfs in locale
def analysis_3_3(spark, hdfs_input_file):

    df = spark.read.csv(
        hdfs_input_file,
        header=True,
        inferSchema=True
    )

    # data cleaning
    df = clean_flight_df(df)

    start_time = time.time()

    # statistiche compagnia - aereoporto
    company_airport = df.groupBy("origin", "op_unique_carrier").agg(
        count("*").alias("num_flights"),
        avg("dep_delay").alias("avg_dep_delay"),
        avg("arr_delay").alias("avg_arr_delay"),
        (sum("cancelled") / count("*")).alias("cancellation_rate")
    )

    # media per aereoporto
    airport_avg = df.groupBy("origin").agg(
        avg("dep_delay").alias("airport_avg_dep_delay")
    )

    # join
    result = company_airport.join(airport_avg, "origin")

    # differenza
    result = result.withColumn(
        "dep_delay_diff",
        col("avg_dep_delay") - col("airport_avg_dep_delay")
    )

    # ranking
    window = Window.partitionBy("origin").orderBy(col("avg_dep_delay").asc())

    result = result.withColumn("rank", rank().over(window))

    result.collect()

    execution_time = time.time() - start_time

    output = result._jdf.showString(20, 0, False)

    print(output)
    print(f"Execution time: {execution_time:.2f}")

    save_spark_log(
    result,
    execution_time,
    "output/local/spark/spark_core_local.txt",
    title="Analisi 3.3 SPARK CORE"
    )

    return execution_time