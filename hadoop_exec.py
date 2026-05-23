import os
import time
from datetime import datetime
import subprocess


# ----------------------------
# UTILS
# ----------------------------

def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("Command failed")
    return result.stdout



def hdfs_exists(path):
    res = subprocess.run(f"hdfs dfs -test -e {path}", shell=True)
    return res.returncode == 0

def hdfs_put(local_path, hdfs_path):
    run_cmd(f"hdfs dfs -put {local_path} {hdfs_path}")

def hdfs_rm(path):
    run_cmd(f"hdfs dfs -rm -r -f {path}")



# funzione che estrae l'output dall'hdfs
def hdfs_cat(output_path):
    cmd = f"hdfs dfs -cat {output_path}/part-*"
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)

    if result.returncode != 0:
        raise RuntimeError(result.stderr)

    return result.stdout

# funzione che salva il file di output in una directory locale
def save_to_local_file(data, output_file):
    if output_file is None:
        return
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(data)



# funzione che crea un file di log dove memorizzare i risultati
def save_log(output_data, execution_time, log_file):

    # crea directory se non esiste
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # prende solo le prime 10 righe NON vuote
    lines = [line for line in output_data.splitlines() if line.strip()]
    top_10 = lines[:10]

    with open(log_file, "a", encoding="utf-8") as f:

        f.write("\n")
        f.write("=" * 70)
        f.write("\n")

        f.write(f"Execution timestamp: {datetime.now()}\n")
        f.write(f"Execution time: {execution_time:.2f} seconds\n")

        f.write("\nTOP 10 RESULTS\n")
        f.write("-" * 70)
        f.write("\n")

        for line in top_10:
            f.write(line + "\n")

        f.write("\n")


# ----------------------------
# MAIN FUNCTIONS
# ----------------------------
#
#   - HADOOP EXECUTOR
#   - LOG FILE
#
# ----------------------------

# funzione che esegue il job su HADOOP
def hadoop_executor(mapper_file, reducer_file, input_file, local_output_file_path, input_path, output_path):

    mapper_hdfs = f"/tmp/{os.path.basename(mapper_file)}"
    reducer_hdfs = f"/tmp/{os.path.basename(reducer_file)}"

    run_cmd("hdfs dfs -mkdir -p /input")
    run_cmd("hdfs dfs -mkdir -p /output")
    run_cmd("hdfs dfs -mkdir -p /tmp")


    print("Checking HDFS files...")

    # upload mapper
    if not hdfs_exists(mapper_hdfs):
        print("Uploading mapper")
        hdfs_put(mapper_file, mapper_hdfs)

    # upload reducer
    if not hdfs_exists(reducer_hdfs):
        print("Uploading reducer")
        hdfs_put(reducer_file, reducer_hdfs)

    # upload input (optional override)
    if not hdfs_exists(input_path):
        print("Uploading input")
        hdfs_put(input_file, input_path)

    # remove old output
    if hdfs_exists(output_path):
        print("Removing old output")
        hdfs_rm(output_path)


    # run job
    print("Running Hadoop job...")

    cmd = f"""
    hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.4.1.jar \
    -file {mapper_file} \
    -file {reducer_file} \
    -mapper "python3 {os.path.basename(mapper_file)}" \
    -reducer "python3 {os.path.basename(reducer_file)}" \
    -input {input_path} \
    -output {output_path}
    """

    start_time = time.time()
    run_cmd(cmd)
    end_time = time.time()

    print("Job completed!")

    execution_time = end_time - start_time


    # leggere output da HDFS
    output_data = hdfs_cat(output_path)

    # salvare in locale
    save_to_local_file(output_data, local_output_file_path)
    #save_log(output_data, execution_time, log_output_file_path)

    return execution_time, output_data