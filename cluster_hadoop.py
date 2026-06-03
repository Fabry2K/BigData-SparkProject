from hadoop_cluster import cluster_executor
from hadoop_exec import save_log
import re
from utils import path_existence
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


# ----------------------------
# DELETE S3 OUTPUT 
# ----------------------------

def delete_s3_prefix(prefix):

    paginator = s3.get_paginator("list_objects_v2")

    objects_to_delete = []

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):

        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            objects_to_delete.append({"Key": obj["Key"]})

    if not objects_to_delete:
        print(f"NO OBJECTS in {prefix}")
        return

    print(f"DELETING {len(objects_to_delete)} OBJECTS from {prefix}")

    for i in range(0, len(objects_to_delete), 1000):

        batch = objects_to_delete[i:i+1000]

        s3.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": batch}
        )


##########################################################################################################################################################################################################################################################

# ----------------------------
# UPLOAD FUNCTIONS
#-----------------------------
# - Mapper, reducer
# - Input
# ----------------------------

# mapper e reducer
def upload_mapper_reducer(analisi, mapper_file, reducer_file):

    mapper_key = f"{CLUSTER}/code/{analisi}/mapper.py"
    reducer_key = f"{CLUSTER}/code/{analisi}/reducer.py"

    if not s3_exists(BUCKET, mapper_key):
        print("UPLOADING MAPPER")
        s3.upload_file(mapper_file, BUCKET, mapper_key)

    if not s3_exists(BUCKET, reducer_key):
        print("UPLOADING REDUCER")
        s3.upload_file(reducer_file, BUCKET, reducer_key)

    return mapper_key, reducer_key

# input
def upload_input(input_file):

    input_key = f"{CLUSTER}/input/{os.path.basename(input_file)}"

    if not s3_exists(BUCKET, input_key):
        print(f"UPLOADING {input_file}")
        s3.upload_file(input_file, BUCKET, input_key)

    return input_key




def upload_project(analisi, mapper, reducer):

    mapper_key, reducer_key = upload_mapper_reducer(analisi, mapper, reducer)

    inputs = {
        "quarter": upload_input(f"files/{analisi}_quarter.csv"),
        "half": upload_input(f"files/{analisi}_half.csv"),
        "normal": upload_input(f"files/{analisi}.csv"),
        "double": upload_input(f"files/{analisi}_double.csv"),
        "quadruple": upload_input(f"files/{analisi}_quadruple.csv")
    }

    return mapper_key, reducer_key, inputs

##########################################################################################################################################################################################################################################################


# ----------------------------
# LOGS EMR
# ----------------------------

def get_logs(step_id):

    stdout = ""
    stderr = ""

    base_prefix = f"emr-logs/{CLUSTER}/steps/{step_id}/"

    for file_name in ["stdout", "stderr"]:

        key = f"{base_prefix}{file_name}.gz"

        try:
            file_obj = s3.get_object(Bucket=BUCKET, Key=key)
            raw = file_obj["Body"].read()

            try:
                content = gzip.decompress(raw).decode("utf-8")
            except:
                content = raw.decode("utf-8")

            if file_name == "stdout":
                stdout = content
            else:
                stderr = content

        except Exception:
            # file potrebbe non esistere
            continue

    return stdout, stderr


# metric extraction
def parse_hadoop_metrics(log_text):

    metrics = {}

    patterns = {
        # EMR / Hadoop varianti
        "map_time": [
            r"Total time spent by all maps.*?=(\d+)",
            r"Total time spent by all maps.*?in occupied slots.*?=(\d+)"
        ],

        "reduce_time": [
            r"Total time spent by all reduces.*?=(\d+)",
            r"Total time spent by all reduces.*?in occupied slots.*?=(\d+)"
        ],

        "map_tasks": [
            r"Launched map tasks=(\d+)",
            r"Maps Launched=(\d+)",
            r"Map tasks[=: ]+(\d+)"
        ],

        "reduce_tasks": [
            r"Launched reduce tasks=(\d+)",
            r"Reduces Launched=(\d+)",
            r"Reduce tasks[=: ]+(\d+)"
        ]
    }

    for key, regex_list in patterns.items():
        for pattern in regex_list:
            m = re.search(pattern, log_text, re.IGNORECASE)
            if m:
                metrics[key] = int(m.group(1))
                break

    return metrics

##########################################################################################################################################################################################################################################################
###########     HADOOP     ################################################################################################################################################################################
##########################################################################################################################################################################################################################################################

# ----------------------------
# ANALYSIS
# ----------------------------

def analyze_with_hadoop(inputs, analisi, mapper_key, reducer_key, output_path, log_output_local_path):

    results = {}
    execution_time = {}

    for name, input_key in inputs.items():

        output_key = f"{CLUSTER}/output/{analisi}/{name}"


        print(f"\n=== START JOB: {name} ===")

        step_id, execution_time = cluster_executor(
            CLUSTER,
            BUCKET,
            name,
            input_key,
            output_key,
            mapper_key,
            reducer_key,
            results,
            execution_time
        )

        # -------------------------
        # READ OUTPUT (PREVIEW)
        # -------------------------
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=output_key
        )

        output_data = []

        for obj in response.get("Contents", []):
            if "part-" in obj["Key"]:

                file_obj = s3.get_object(
                    Bucket=BUCKET,
                    Key=obj["Key"]
                )

                content = file_obj["Body"].read().decode("utf-8")

                output_data.extend(content.splitlines())

                if len(output_data) >= 10:
                    break

        output_data = output_data[:10]

        # -------------------------
        # LOGS + METRICS
        # -------------------------
        stdout, stderr = get_logs(step_id)

        full_log = stdout + "\n" + stderr
        metrics = parse_hadoop_metrics(full_log)

        save_log(
            "\n".join(output_data),
            execution_time[name],
            metrics,
            log_output_local_path
        )

        results[name] = output_data

        plot.plot_analisi(
        execution_time.get("quarter"),
        execution_time.get("half"),
        execution_time.get("normal"),
        execution_time.get("double"),
        execution_time.get("quadruple"),
        "Runtime Execution Hadoop Map Reduce",
        path_existence(f"{output_path}/hadoop_analysis.png")
    )

    return results, execution_time


##########################################################################################################################################################################################################################################################


# ----------------------------
# MAIN
# ----------------------------

def analyze(analisi, mapper, reducer, output_path):

    mapper_key, reducer_key, inputs = upload_project(analisi, mapper, reducer)

    # CLEAN OLD OUTPUT
    delete_s3_prefix(f"{CLUSTER}/output/")
    
    # analisi con HADOOP su cluster AWS
    analyze_with_hadoop(
        inputs,
        analisi,
        mapper_key,
        reducer_key,
        output_path,
        path_existence(f"{output_path}/logs.txt")
    )