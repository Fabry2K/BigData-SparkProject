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


# SPARK SQL: LOCALE
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