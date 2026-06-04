from pyspark.sql.functions import *
import utils
from pathlib import Path
import time


# funzione di cleaning
def clean_flights(df):

    df = df \
        .withColumn("arr_delay", col("arr_delay").cast("double")) \
        .withColumn("dep_delay", col("dep_delay").cast("double")) \
        .withColumn("cancelled", col("cancelled").cast("double"))

    # NULL → 0
    df = df.fillna({
        "arr_delay": 0.0,
        "dep_delay": 0.0,
        "cancelled": 0.0
    })

    # NEGATIVI → 0 (anticipo = 0 ritardo)
    df = df \
        .withColumn("arr_delay", greatest(col("arr_delay"), lit(0))) \
        .withColumn("dep_delay", greatest(col("dep_delay"), lit(0)))

    return df


# analisi 3.1 con spark sql in hdfs locale
def analysis_3_1(spark, hdfs_filepath):

    df = spark.read.csv(
        hdfs_filepath,
        header=True,
        inferSchema=True
    )

    df = clean_flights(df)

    df.createOrReplaceTempView("flights")

    start_time = time.time()

    result = spark.sql("""
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
        GROUP BY op_unique_carrier, origin
    """)

    result.collect()

    execution_time = time.time() - start_time

    output = result._jdf.showString(20, 0, False)

    print(output)
    print(f"\nExecution time: {execution_time:.2f} seconds")

    utils.append_to_log(
        "SPARK SQL 3.1 - " + Path(hdfs_filepath).stem,
        output + f"\n\nExecution time: {execution_time:.2f} seconds",
        logfile="output/local/spark/spark_sql_local.txt"
    )

    return execution_time


# analisi 3.3 con spark sql con hdfs in locale
def analysis_3_3(spark, hdfs_filepath):

    df = spark.read.csv(
        hdfs_filepath,
        header=True,
        inferSchema=True
    )

    df = clean_flights(df)

    df.createOrReplaceTempView("flights")

    start_time = time.time()

    result = spark.sql("""
        WITH company_airport_stats AS (

            SELECT
                origin,
                op_unique_carrier,

                COUNT(*) AS num_flights,

                AVG(dep_delay) AS avg_dep_delay,
                AVG(arr_delay) AS avg_arr_delay,

                SUM(cancelled) / COUNT(*) AS cancellation_rate

            FROM flights
            GROUP BY origin, op_unique_carrier
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

            c.avg_dep_delay - a.airport_avg_dep_delay AS dep_delay_diff,

            RANK() OVER (
                PARTITION BY c.origin
                ORDER BY c.avg_dep_delay ASC
            ) AS rank

        FROM company_airport_stats c
        JOIN airport_avg_stats a
        ON c.origin = a.origin
    """)

    result.collect()

    execution_time = time.time() - start_time

    output = result._jdf.showString(20, 0, False)

    print(output)
    print(f"\nExecution time: {execution_time:.2f} seconds")

    utils.append_to_log(
        "SPARK SQL 3.3 - " + Path(hdfs_filepath).stem,
        output + f"\n\nExecution time: {execution_time:.2f} seconds",
        logfile="output/local/spark/spark_sql_local.txt"
    )

    return execution_time