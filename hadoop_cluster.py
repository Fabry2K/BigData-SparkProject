import boto3
import os
from dotenv import load_dotenv


#---------------------------
# UTILS
#---------------------------

s3 = boto3.client("s3")

# funzione che carica un file in locale su Amazon S3
def upload(local, bucket, key):
    s3.upload_file(local, bucket, key)

# funzione per scaricare un file da Amazon S3 in locale
def download_file(bucket, key, local_path):
    s3.download_file(bucket, key, local_path)


# funzione che legge output direttamente da S3
def read_text(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")




#---------------------------
# MAIN
#---------------------------

def run_cluster_job(cluster_id, bucket, input_key, output_key, mapper_key, reducer_key):

    load_dotenv()

    cluster_id = os.getenv("CLUSTER_ID")
    bucket = os.getenv("BUCKET")
    input_key = os.getenv("SSH_KEY_PATH")

    emr = boto3.client("emr")

    response = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "Hadoop Streaming Job",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "hadoop-streaming",

                        "-files",
                        f"s3://{bucket}/{mapper_key},s3://{bucket}/{reducer_key}",

                        "-mapper",
                        "python3 mapper.py",

                        "-reducer",
                        "python3 reducer.py",

                        "-input",
                        f"s3://{bucket}/{input_key}",

                        "-output",
                        f"s3://{bucket}/{output_key}"
                    ]
                }
            }
        ]
    )

    return response