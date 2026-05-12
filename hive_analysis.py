import subprocess
import time

def hive_setup():
    query = """
    CREATE TABLE IF NOT EXISTS flights (
        fl_date STRING,
        month INT,
        op_unique_carrier STRING,
        op_carrier_fl_num STRING,
        origin STRING,
        dest STRING,
        dep_delay FLOAT,
        arr_delay FLOAT,
        cancelled INT,
        cancellation_code STRING,
        distance FLOAT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;
    """

    subprocess.run(["hive", "-e", query])


def hive_load(path):

    query = f"""
    LOAD DATA LOCAL INPATH '{path}'
    INTO TABLE flights;
    """

    subprocess.run(["hive", "-e", query])

    

def hive_query_3_1_local(output_path):

    query = """
    SELECT *
    FROM (
        SELECT
            op_unique_carrier,
            origin,
            COUNT(*) AS num_flights,
            MIN(GREATEST(arr_delay, 0)) AS min_arr_delay,
            MAX(GREATEST(arr_delay, 0)) AS max_arr_delay,
            AVG(GREATEST(arr_delay, 0)) AS avg_arr_delay,
            SUM(cancelled) / COUNT(*) AS cancellation_rate
        FROM flights
        GROUP BY op_unique_carrier, origin
    ) t
    LIMIT 10;
    """

    start = time.time()

    with open(output_path, "a", encoding="utf-8") as f:

        f.write("\n\n==============================\n")
        f.write("HIVE ANALYSIS 3.1\n")
        f.write("==============================\n\n")

        subprocess.run(
            ["hive", "-e", query],
            stdout=f,
            stderr=subprocess.STDOUT
        )

    end = time.time()

    execution_time = end - start

    with open(output_path, "a", encoding="utf-8") as f:
        f.write(f"\nExecution time: {execution_time:.2f} seconds\n")

    print(f"Hive execution time: {execution_time:.2f} seconds")

    return execution_time