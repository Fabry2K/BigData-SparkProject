import boto3

emr = boto3.client("emr")


def cluster_executor(cluster_id, bucket, input_key, output_key, mapper_key, reducer_key):

    response = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "MapReduce Job",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "hadoop-streaming",

                        "-D",
                        "mapreduce.job.reduces=1",

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