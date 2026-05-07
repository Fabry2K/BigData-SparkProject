from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, min, max, sum, collect_set

# SPARK CORE: LOCALE
def local_analysis(filepath):
    spark = SparkSession.builder \
        .appName("FlightAnalysis") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()

    spark_df = spark.read.csv(
        filepath,
        header=True,
        inferSchema=True
    )


    # result_3_1 = spark_df.groupBy("op_unique_carrier", "origin") \
    # .agg(
    #     count("*").alias("num_flights"),
    #     min("arr_delay").alias("min_arr_delay"),
    #     max("arr_delay").alias("max_arr_delay"),
    #     avg("arr_delay").alias("avg_arr_delay"),
    #     (sum("cancelled") / count("*")).alias("cancellation_rate"),
    #     collect_set("month").alias("months_active")
    # )

    # result_3_1.show(20, truncate=False)

    # Controllo
    spark_df.show(10)
    spark_df.printSchema()