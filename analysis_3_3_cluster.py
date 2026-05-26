from hadoop_cluster import cluster_executor
import os
import plot
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# collegamento al cluster AWS academy
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


# upload mapper e reducer nel bucket AWS 
def upload_mapper_reducer(mapper_file, reducer_file):

    mapper_key = f"{CLUSTER}/code/analisi_3_3/mapper.py"
    reducer_key = f"{CLUSTER}/code/analisi_3_3/reducer.py"

    if s3_exists(BUCKET, mapper_key):
        print(f"[SKIP] Mapper già esistente: {mapper_key}")
    else:
        s3.upload_file(mapper_file, BUCKET, mapper_key)
        print(f"[UPLOAD] Mapper caricato")

    if s3_exists(BUCKET, reducer_key):
        print(f"[SKIP] Reducer già esistente: {reducer_key}")
    else:
        s3.upload_file(reducer_file, BUCKET, reducer_key)
        print(f"[UPLOAD] Reducer caricato")

    print("Mapper e Reducer completato!")
    return mapper_key, reducer_key


# upload input files nel bucket AWS
def upload_input(input_file):

    input_key = f"{CLUSTER}/input/{os.path.basename(input_file)}"

    if s3_exists(BUCKET, input_key):
        print(f"[SKIP] Input già esistente: {input_key}")
    else:
        s3.upload_file(input_file, BUCKET, input_key)
        print(f"[UPLOAD] Input caricato")

    return input_key


def upload_project():

    mapper_key, reducer_key = upload_mapper_reducer("hadoop_3_3/mapper.py", "hadoop_3_3/reducer.py")

    inputs = {
        "quarter": upload_input("files/analisi_3_3_quarter.csv"),
        "half": upload_input("files/analisi_3_3_half.csv"),
        "normal": upload_input("files/analisi_3_3.csv"),
        "double": upload_input("files/analisi_3_3_double.csv"),
        "quadruple": upload_input("files/analisi_3_3_quadruple.csv")
    }

    return mapper_key, reducer_key, inputs

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def analyze():

    mapper_key, reducer_key, inputs = upload_project()
