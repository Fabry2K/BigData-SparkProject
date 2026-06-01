import boto3
import time 

emr = boto3.client("emr")


def cluster_executor(cluster_id, bucket, name, input_key, output_key, mapper_key, reducer_key, results, execution_time):

    response = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": f"Hadoop_{name}",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "hadoop-streaming",
                             "-D",
                            "mapreduce.job.reduces=1",
                            "-files",
                            f"s3://{bucket}/{mapper_key}#mapper.py,s3://{bucket}/{reducer_key}#reducer.py",
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

    step_id = response["StepIds"][0]

    print("STEP ID:", step_id)

    # WAIT JOB
    while True:

        step = emr.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )

        state = step["Step"]["Status"]["State"]

        print(f"STATUS: {state}")

        if state in ["COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"]:
            break

        time.sleep(20)

    if state == "COMPLETED":
        print(f"JOB {name} COMPLETED")
        results[name] = output_key
    else:
        print(f"JOB {name} FAILED")


    start = step["Step"]["Status"]["Timeline"]["StartDateTime"]
    end = step["Step"]["Status"]["Timeline"]["EndDateTime"]

    execution_time[name] = (end - start).total_seconds()

    return results, execution_time