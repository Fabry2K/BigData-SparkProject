import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# -------------------------
# CONFIG
# -------------------------
BUCKET = "bigdata2026fabry"

INPUT_PREFIX = "input/"
OUTPUT_PREFIX = "output/spark_sql/"

# -------------------------
# SPARK SESSION
# -------------------------
spark = SparkSession.builder \
    .appName("Spark_SQL_Analyses") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
    .getOrCreate()


# -------------------------
# PATH HELPERS
# -------------------------
def input_path(analysis, dataset):
    return f"s3a://{BUCKET}/{INPUT_PREFIX}{analysis}/{analysis}_{dataset}.csv"


def output_path(analysis, dataset):
    return f"s3a://{BUCKET}/{OUTPUT_PREFIX}{analysis}/{dataset}/"



# -------------------------
# CLEANING
# -------------------------
def clean_flights(df):

    df = df \
        .withColumn("arr_delay", col("arr_delay").cast("double")) \
        .withColumn("dep_delay", col("dep_delay").cast("double")) \
        .withColumn("cancelled", col("cancelled").cast("double"))

    df = df.fillna({
        "arr_delay": 0.0,
        "dep_delay": 0.0,
        "cancelled": 0.0
    })


    df = df \
        .withColumn("arr_delay", when(col("arr_delay") < 0, 0).otherwise(col("arr_delay"))) \
        .withColumn("dep_delay", when(col("dep_delay") < 0, 0).otherwise(col("dep_delay")))

    return df


# -------------------------
# 3.1 SQL
# -------------------------
def run_3_1(dataset):

    print(f"\n===== ANALISI 3.1 SQL - {dataset} =====")

    path = input_path("analisi_3_1", dataset)
    start = time.time()

    df = spark.read.csv(path, header=True, inferSchema=True)
    df = clean_flights(df)

    # 🔥 FIX CHIAVE: ESCLUDI CANCELLED QUI
    df = df.filter(col("cancelled") == 0)

    df.createOrReplaceTempView("flights")

    result = spark.sql("""
        SELECT
            op_unique_carrier,
            origin,
            COUNT(*) AS num_flights,

            MIN(arr_delay) AS min_arr_delay,
            MAX(arr_delay) AS max_arr_delay,
            AVG(arr_delay) AS avg_arr_delay,

            SUM(cancelled) / COUNT(*) AS cancellation_rate

        FROM flights
        WHERE cancelled = 0
        GROUP BY op_unique_carrier, origin
    """)

    result.collect()

    exec_time = time.time() - start

    top10 = result.limit(10).collect()

    lines = [
        "========================================",
        f"ANALISI 3.1 SQL - {dataset}",
        f"Timestamp: {datetime.now()}",
        "========================================\n"
    ]

    for r in top10:
        lines.append(str(r))

    lines.append(f"\nEXEC TIME: {exec_time}")

    out_path = output_path("analisi_3_1", dataset)


    spark.sparkContext.parallelize(["\n".join(lines)]) \
        .coalesce(1) \
        .saveAsTextFile(out_path)

    return exec_time


# -------------------------
# 3.3 SQL
# -------------------------
def run_3_3(dataset):

    print(f"\n===== ANALISI 3.3 SQL - {dataset} =====")

    path = input_path("analisi_3_3", dataset)
    start = time.time()

    df = spark.read.csv(path, header=True, inferSchema=True)
    df = clean_flights(df)

    # 🔥 FIX CHIAVE: ESCLUDI CANCELLED QUI
    df = df.filter(col("cancelled") == 0)

    df.createOrReplaceTempView("flights")

    result = spark.sql("""
        WITH company_airport AS (

            SELECT
                origin,
                op_unique_carrier,
                COUNT(*) AS num_flights,
                AVG(dep_delay) AS avg_dep_delay,
                AVG(arr_delay) AS avg_arr_delay,
                SUM(cancelled) / COUNT(*) AS cancellation_rate

            FROM flights
            WHERE cancelled = 0
            GROUP BY origin, op_unique_carrier
        ),

        airport_avg AS (

            SELECT
                origin,
                AVG(dep_delay) AS airport_avg_dep_delay
            FROM flights
            WHERE cancelled = 0
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

        FROM company_airport c
        JOIN airport_avg a
        ON c.origin = a.origin
    """)

    result.collect()

    exec_time = time.time() - start

    top10 = result.limit(10).collect()

    lines = [
        "========================================",
        f"ANALISI 3.3 SQL - {dataset}",
        f"Timestamp: {datetime.now()}",
        "========================================\n"
    ]

    for r in top10:
        lines.append(str(r))

    lines.append(f"\nEXEC TIME: {exec_time}")

    out_path = output_path("analisi_3_3", dataset)


    spark.sparkContext.parallelize(["\n".join(lines)]) \
        .coalesce(1) \
        .saveAsTextFile(out_path)

    return exec_time


# -------------------------
# MAIN
# -------------------------


datasets = ["quarter", "half", "normal", "double", "quadruple"]

results = {"3_1": {}, "3_3": {}}

for d in datasets:
    try:
        results["3_1"][d] = run_3_1(d)
        results["3_3"][d] = run_3_3(d)
    except Exception as e:
        print(f"ERROR on {d}: {e}")

print("\nFINAL RESULTS:")
print(results)