from hadoop_cluster import cluster_executor
from hadoop_exec import save_log, parse_hadoop_metrics
import time
import os
import plot
import boto3
import gzip
from dotenv import load_dotenv
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

load_dotenv()

BUCKET = os.getenv("BUCKET")
CLUSTER = os.getenv("CLUSTER_ID")


def s3_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


# HADOOP output log
log_path = "output/cluster/hadoop_3_1/hadoop_logs.txt"

# elimina se esiste
if os.path.exists(log_path):
    os.remove(log_path)

# ricrea il file (vuoto)
os.makedirs(os.path.dirname(log_path), exist_ok=True)

# ----------------------------
# UPLOAD FUNCTIONS
# ----------------------------

def upload_mapper_reducer(mapper_file, reducer_file):

    mapper_key = f"{CLUSTER}/code/analisi_3_1/mapper.py"
    reducer_key = f"{CLUSTER}/code/analisi_3_1/reducer.py"

    if not s3_exists(BUCKET, mapper_key):
        s3.upload_file(mapper_file, BUCKET, mapper_key)

    if not s3_exists(BUCKET, reducer_key):
        s3.upload_file(reducer_file, BUCKET, reducer_key)

    return mapper_key, reducer_key


def upload_input(input_file):

    input_key = f"{CLUSTER}/input/{os.path.basename(input_file)}"

    if not s3_exists(BUCKET, input_key):
        s3.upload_file(input_file, BUCKET, input_key)

    return input_key


def upload_project():

    mapper_key, reducer_key = upload_mapper_reducer(
        "hadoop_3_1/mapper.py",
        "hadoop_3_1/reducer.py"
    )

    inputs = {
        "quarter": upload_input("files/analisi_3_1_quarter.csv"),
        "half": upload_input("files/analisi_3_1_half.csv"),
        "normal": upload_input("files/analisi_3_1.csv"),
        "double": upload_input("files/analisi_3_1_double.csv"),
        "quadruple": upload_input("files/analisi_3_1_quadruple.csv")
    }

    return mapper_key, reducer_key, inputs


# ----------------------------
# LOGS EMR (ROBUST VERSION)
# ----------------------------

def get_logs(step_id):

    stdout, stderr = "", ""

    prefix = f"emr-logs/{CLUSTER}/"

    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    for obj in response.get("Contents", []):

        key = obj["Key"]

        try:
            file_obj = s3.get_object(Bucket=BUCKET, Key=key)
            content = gzip.decompress(file_obj["Body"].read()).decode("utf-8")

            if "stdout" in key:
                stdout = content

            if "stderr" in key:
                stderr = content

        except Exception:
            continue

    return stdout, stderr


# ----------------------------
# ANALYSIS
# ----------------------------

def analyze_with_hadoop(inputs, mapper_key, reducer_key, log_output_local_path):

    results = {}
    execution_time = {}

    for name, input_key in inputs.items():

        output_key = f"{CLUSTER}/output/{name}/job_3_1"

        start_time = time.time()

        # RUN JOB
        step_id = cluster_executor(
            CLUSTER,
            BUCKET,
            input_key,
            output_key,
            mapper_key,
            reducer_key
        )

        # -------------------------
        # READ OUTPUT
        # -------------------------
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=output_key
        )

        output_data = ""

        for obj in response.get("Contents", []):
            if "part-" in obj["Key"]:
                file_obj = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                output_data += file_obj["Body"].read().decode("utf-8")

        end_time = time.time()
        execution_time[name] = end_time - start_time

        # -------------------------
        # LOGS + METRICS
        # -------------------------
        stdout, stderr = get_logs(step_id)

        full_log = stdout + "\n" + stderr
        metrics = parse_hadoop_metrics(full_log)

        save_log(
            output_data,
            execution_time[name],
            metrics,
            log_output_local_path
        )

        results[name] = output_data

    return results, execution_time


# ----------------------------
# MAIN
# ----------------------------

def analyze():

    mapper_key, reducer_key, inputs = upload_project()

    results, execution_time = analyze_with_hadoop(
        inputs,
        mapper_key,
        reducer_key,
        "output/cluster/hadoop_3_1/hadoop_logs.txt"
    )

    plot.plot_analisi(
        execution_time.get("quarter"),
        execution_time.get("half"),
        execution_time.get("normal"),
        execution_time.get("double"),
        execution_time.get("quadruple"),
        "Analisi 3.1 Hadoop Map Reduce",
        "output/cluster/hadoop_3_1/hadoop_analysis.png"
    )


analyze()