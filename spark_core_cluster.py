import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# -------------------------
# CONFIG
# -------------------------
BUCKET = "bigdata2026fabry"

INPUT_PREFIX = "input/"
OUTPUT_PREFIX = "output/spark_core/"

ANALYSES = ["analisi_3_1", "analisi_3_3"]

# -------------------------
# SPARK SESSION (EMR)
# -------------------------
spark = SparkSession.builder \
    .appName("Spark_EMR_Analyses") \
    .getOrCreate()


# -------------------------
# PATH HELPERS
# -------------------------
def input_path(analysis, dataset):
    return f"s3a://{BUCKET}/{INPUT_PREFIX}{analysis}/{analysis}_{dataset}.csv"


def output_path(analysis, dataset):
    return f"s3a://{BUCKET}/{OUTPUT_PREFIX}{analysis}/{dataset}/"


# -------------------------
# CLEANING (CORRETTO)
# -------------------------
def clean_flight_df(df):

    df = df \
        .withColumn("arr_delay", col("arr_delay").cast("double")) \
        .withColumn("dep_delay", col("dep_delay").cast("double")) \
        .withColumn("cancelled", col("cancelled").cast("double"))

    df = df.fillna({
        "arr_delay": 0.0,
        "dep_delay": 0.0,
        "cancelled": 0.0
    })

    # SOLO NORMALIZZAZIONE
    df = df \
        .withColumn("arr_delay", when(col("arr_delay") < 0, 0).otherwise(col("arr_delay"))) \
        .withColumn("dep_delay", when(col("dep_delay") < 0, 0).otherwise(col("dep_delay")))

    return df


# -------------------------
# 3.1
# -------------------------
def run_3_1(dataset):

    print(f"\n===== ANALISI 3.1 - {dataset} =====")

    path = input_path("analisi_3_1", dataset)
    print(f"INPUT: {path}")

    df = spark.read.csv(path, header=True, inferSchema=True)
    df = clean_flight_df(df)

    df = df.filter(col("cancelled") == 0)

    start = time.time()

    result = df.groupBy("op_unique_carrier", "origin").agg(
        count("*").alias("num_flights"),
        min("arr_delay").alias("min_arr_delay"),
        max("arr_delay").alias("max_arr_delay"),
        avg("arr_delay").alias("avg_arr_delay"),
        (sum("cancelled") / count("*")).alias("cancellation_rate")
    )

    result.cache()
    result.count()

    exec_time = time.time() - start

    top10 = result.limit(10).collect()

    lines = [
        "========================================",
        f"ANALISI 3.1 - {dataset}",
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

    print(f"SAVED: {out_path}")

    return exec_time


# -------------------------
# 3.3
# -------------------------
def run_3_3(dataset):

    print(f"\n===== ANALISI 3.3 - {dataset} =====")

    path = input_path("analisi_3_3", dataset)
    print(f"INPUT: {path}")

    start = time.time()

    df = spark.read.csv(path, header=True, inferSchema=True)
    df = clean_flight_df(df)

    df = df.filter(col("cancelled") == 0)

    # compagnie
    company_airport = df.groupBy("origin", "op_unique_carrier").agg(
        count("*").alias("num_flights"),
        avg("dep_delay").alias("avg_dep_delay"),
        avg("arr_delay").alias("avg_arr_delay"),
        (sum("cancelled") / count("*")).alias("cancellation_rate")
        collect_set("month").alias("months_active")
    )

    # media aeroporto
    airport_avg = df.groupBy("origin").agg(
        avg("dep_delay").alias("airport_avg_dep_delay")
    )

    result = company_airport.join(airport_avg, "origin", "left")

    result = result.withColumn(
        "dep_delay_diff",
        col("avg_dep_delay") - col("airport_avg_dep_delay")
    )

    window = Window.partitionBy("origin").orderBy(col("avg_dep_delay").asc())

    result = result.withColumn("rank", rank().over(window))

    result.cache()
    result.count()

    exec_time = time.time() - start

    top10 = result.limit(10).collect()

    lines = [
        "========================================",
        f"ANALISI 3.3 - {dataset}",
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

    print(f"SAVED: {out_path}")

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