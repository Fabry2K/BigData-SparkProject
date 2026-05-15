from pyspark.sql.functions import *
import utils
from pathlib import Path
import time
from pyspark.sql.window import Window


# SPARK CORE: LOCALE analisi 3.1
def local_analysis_3_1(spark, filepath):

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

# SPARK CORE: LOCALE analisi 3.3
def local_analysis_3_3(spark, filepath):

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

    # statistiche per coppia compagnia-aeroporto
    company_airport_stats = spark_df.groupBy(
        "origin",
        "op_unique_carrier"
    ).agg(
        count("*").alias("num_flights"),
        avg("dep_delay").alias("avg_dep_delay"),
        avg("arr_delay").alias("avg_arr_delay"),
        (sum("cancelled") / count("*")).alias("cancellation_rate")
    )

    # ritardo medio complessivo dell'aeroporto
    airport_avg_stats = spark_df.groupBy("origin").agg(
        avg("dep_delay").alias("airport_avg_dep_delay")
    )

    # join tra statistiche compagnia e statistiche aeroporto
    result_3_3 = company_airport_stats.join(
        airport_avg_stats,
        on="origin",
        how="inner"
    )

    # differenza rispetto alla media dell'aeroporto
    result_3_3 = result_3_3.withColumn(
        "dep_delay_diff",
        col("avg_dep_delay") - col("airport_avg_dep_delay")
    )

    # ranking compagnie nell'aeroporto
    ranking_window = Window.partitionBy("origin") \
        .orderBy(col("avg_dep_delay").asc())

    result_3_3 = result_3_3.withColumn(
        "rank",
        rank().over(ranking_window)
    )

    result_3_3.collect()

    end_time = time.time()

    execution_time = end_time - start_time

    # stampa e salvataggio del risultato
    output = result_3_3._jdf.showString(20, 0, False)

    print(output)
    print(f"\nExecution time: {execution_time:.2f} seconds")

    utils.append_to_log(
        "Risultati analisi 3.3 sul file " + Path(filepath).stem,
        output + f"\n\nExecution time: {execution_time:.2f} seconds"
    )

    return execution_time
