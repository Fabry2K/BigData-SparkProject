import os
import re
import time
from datetime import datetime
import subprocess
from dotenv import load_dotenv
from utils import run_cmd


# ----------------------------
# UTILS
# ----------------------------

def hdfs_exists(path):
    res = subprocess.run(f"hdfs dfs -test -e {path}", shell=True)
    return res.returncode == 0

def hdfs_put(local_path, hdfs_path):
    run_cmd(f"hdfs dfs -put {local_path} {hdfs_path}")

def hdfs_rm(path):
    run_cmd(f"hdfs dfs -rm -r -f {path}")



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
def save_log(output_data, execution_time, metrics, log_file):

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

        f.write("\nHADOOP METRICS\n")
        f.write("-" * 80 + "\n")

        for k, v in metrics.items():
            f.write(f"{k}: {v}\n")

        f.write("\nTOP 10 RESULTS\n")
        f.write("-" * 70)
        f.write("\n")

        for line in top_10:
            f.write(line + "\n")

        f.write("\n")


def parse_hadoop_metrics(log_text):

    metrics = {}

    patterns = {
        "map_time": r"Total time spent by all maps.*?=(\d+)",
        "reduce_time": r"Total time spent by all reduces.*?=(\d+)",
        "map_tasks": r"Launched map tasks=(\d+)",
        "reduce_tasks": r"Launched reduce tasks=(\d+)"
    }

    for k, p in patterns.items():
        m = re.search(p, log_text)
        if m:
            metrics[k] = int(m.group(1))

    return metrics


# ----------------------------
# MAIN FUNCTIONS
# ----------------------------
#
#   - HADOOP EXECUTOR
#   - LOG FILE
#
# ----------------------------

# funzione che esegue il job su HADOOP
def hadoop_executor(mapper_file, reducer_file, input_file, local_output_file_path, analisi, output_path, log_output_file_path):

    execution_time = {}

    # generazione directories hdfs
    run_cmd("hdfs dfs -mkdir -p /output")
    run_cmd("hdfs dfs -mkdir -p /tmp")



    # ----------------------------------
    # UPLOADING MAPPER E REDUCER TO HDFS
    # ----------------------------------

    mapper_hdfs = f"/tmp/{os.path.basename(mapper_file)}"
    reducer_hdfs = f"/tmp/{os.path.basename(reducer_file)}"


    if not hdfs_exists(mapper_hdfs):
        print("Uploading mapper")
        hdfs_put(mapper_file, mapper_hdfs)

    if not hdfs_exists(reducer_hdfs):
        print("Uploading reducer")
        hdfs_put(reducer_file, reducer_hdfs)


    # -----------------------
    # UPLOADING INPUT TO HDFS
    # -----------------------
    for name, file in input_file.items():

        input_hdfs = f"/input/{analisi}{os.path.basename(file)}"
        output_hdfs = f"/output/{analisi}/{os.path.basename(file)}"


        # upload input (optional override)
        if not hdfs_exists(input_hdfs):
            print("Uploading input")
            hdfs_put(file, input_hdfs)

        # remove old output
        if hdfs_exists(output_hdfs):
            print("Removing old output")
            hdfs_rm(output_hdfs)


        # -----------------------
        # HADOOP JOB RUN
        # -----------------------

        print("Running Hadoop job...")

        # caricamento del path per il jar di Hadoop
        load_dotenv()
        path_hadoop_jar = os.getenv("path_hadoop_jar")

        cmd = f"""
        hadoop jar {path_hadoop_jar} \
        -file {mapper_file} \
        -file {reducer_file} \
        -mapper "python3 {os.path.basename(mapper_file)}" \
        -reducer "python3 {os.path.basename(reducer_file)}" \
        -input {input_hdfs} \
        -output {output_hdfs}
        """

        start_time = time.time()
        stdout, stderr = run_cmd(cmd)
        end_time = time.time()

        print("Job completed!")

        # -------------------------
        # LOGS + METRICS
        # -------------------------

        execution_time[name] = end_time - start_time

        # log completo Hadoop
        full_log = stdout + "\n" + stderr

        # parsing metrics
        metrics = parse_hadoop_metrics(full_log)

        # leggere output da HDFS
        output_data = hdfs_cat(output_hdfs)

        # salvare in locale
        #save_to_local_file(output_data, local_output_file_path)
        save_log(output_data, execution_time[name], metrics, log_output_file_path)

    return execution_time