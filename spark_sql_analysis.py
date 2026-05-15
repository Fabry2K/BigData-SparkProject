from pyspark.sql.functions import *
import utils
from pathlib import Path
import time


# SPARK SQL: LOCALE analisi 3.1
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

    # creazione vista temporanea SQL
    spark_df.createOrReplaceTempView("flights")

    start_time = time.time()

    result_3_1 = spark.sql("""
        SELECT
            op_unique_carrier,
            origin,

            COUNT(*) AS num_flights,

            MIN(arr_delay) AS min_arr_delay,

            MAX(arr_delay) AS max_arr_delay,

            AVG(arr_delay) AS avg_arr_delay,

            SUM(cancelled) / COUNT(*) AS cancellation_rate,

            COLLECT_SET(month) AS months_active

        FROM flights

        GROUP BY
            op_unique_carrier,
            origin
    """)

    result_3_1.collect()

    end_time = time.time()

    execution_time = end_time - start_time

    # stampa e salvataggio del risultato
    output = result_3_1._jdf.showString(20, 0, False)

    print(output)
    print(f"\nExecution time: {execution_time:.2f} seconds")

    utils.append_to_log(
        "Risultati SPARK SQL analisi 3.1 sul file " + Path(filepath).stem,
        output + f"\n\nExecution time: {execution_time:.2f} seconds"
    )

    return execution_time


# SPARK SQL: LOCALE analisi 3.3
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

    # creazione vista temporanea SQL
    spark_df.createOrReplaceTempView("flights")

    start_time = time.time()

    result_3_3 = spark.sql("""

        WITH company_airport_stats AS (

            SELECT
                origin,
                op_unique_carrier,

                COUNT(*) AS num_flights,

                AVG(dep_delay) AS avg_dep_delay,

                AVG(arr_delay) AS avg_arr_delay,

                SUM(cancelled) / COUNT(*) AS cancellation_rate

            FROM flights

            GROUP BY
                origin,
                op_unique_carrier
        ),

        airport_avg_stats AS (

            SELECT
                origin,

                AVG(dep_delay) AS airport_avg_dep_delay

            FROM flights

            GROUP BY origin
        )

        SELECT
            c.origin,

            c.op_unique_carrier,

            c.num_flights,

            c.avg_dep_delay,

            c.avg_arr_delay,

            c.cancellation_rate,

            c.avg_dep_delay - a.airport_avg_dep_delay
                AS dep_delay_diff,

            RANK() OVER (
                PARTITION BY c.origin
                ORDER BY c.avg_dep_delay ASC
            ) AS rank

        FROM company_airport_stats c

        JOIN airport_avg_stats a
            ON c.origin = a.origin
    """)

    result_3_3.collect()

    end_time = time.time()

    execution_time = end_time - start_time

    # stampa e salvataggio del risultato
    output = result_3_3._jdf.showString(20, 0, False)

    print(output)
    print(f"\nExecution time: {execution_time:.2f} seconds")

    utils.append_to_log(
        "Risultati SPARK SQL analisi 3.3 sul file " + Path(filepath).stem,
        output + f"\n\nExecution time: {execution_time:.2f} seconds"
    )

    return execution_time