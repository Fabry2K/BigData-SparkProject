from hadoop_cluster import cluster_executor
import os
import plot
import boto3
from dotenv import load_dotenv

# collegamento al cluster AWS academy
s3 = boto3.client("s3")


load_dotenv()

BUCKET = os.getenv("BUCKET")
CLUSTER = os.getenv("CLUSTER_ID")


# upload mapper e reducer nel bucket AWS 
def upload_mapper_reducer(mapper_file, reducer_file):

    # MAPPER
    mapper_key = f"{CLUSTER}/code/mapper.py"
    s3.upload_file(mapper_file, BUCKET, mapper_key)

    # REDUCER
    reducer_key = f"{CLUSTER}/code/reducer.py"
    s3.upload_file(reducer_file, BUCKET, reducer_key)

    print("Mapper e Reducer upload completato!")

    return mapper_key, reducer_key


# upload input files nel bucket AWS
def upload_input(input_file):

    # INPUT
    input_key = f"{CLUSTER}/input/{input_file.split('/')[-1]}"
    s3.upload_file(input_file, BUCKET, input_key)

    print("Input file upload completato!")

    return input_key




def upload_project():

    mapper_key, reducer_key = upload_mapper_reducer("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py")

    inputs = {
        "quarter": upload_input("files/analisi_3_1_quarter.csv"),
        #"half": upload_input("files/analisi_3_1_half.csv"),
        #"normal": upload_input("files/analisi_3_1.csv"),
        #"double": upload_input("files/analisi_3_1_double.csv"),
        #"quadruple": upload_input("files/analisi_3_1_quadruple.csv"),
    }

    return mapper_key, reducer_key, inputs

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def analysis_3_1():

    mapper_key, reducer_key, inputs = upload_project()

    results = {}

    for name, input_key in inputs.items():

        output_key = f"{CLUSTER}/output/{name}/job_3_1"

        cluster_executor(
            CLUSTER,
            BUCKET,
            input_key,
            output_key,
            mapper_key,
            reducer_key
        )

        # leggi output corretto (NO wildcard)
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=output_key
        )

        output_data = ""

        for obj in response.get("Contents", []):
            if "part-" in obj["Key"]:
                file_obj = s3.get_object(
                    Bucket=BUCKET,
                    Key=obj["Key"]
                )
                output_data += file_obj["Body"].read().decode("utf-8")

        results[name] = output_data
        print(f"\n=== {name.upper()} ===")
        print(output_data)

    return results

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ 

analysis_3_1()