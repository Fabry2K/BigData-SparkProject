from pyspark.sql import SparkSession

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

    # Controllo
    # spark_df.show(10)
    # spark_df.printSchema()