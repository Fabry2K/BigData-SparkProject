import paramiko
import os
import time
from dotenv import load_dotenv

from hadoop_exec import save_to_local_file, save_log, parse_hadoop_metrics


#------------------------------
# UTILS
#------------------------------

def run_command(ssh, command):
    stdin, stdout, stderr = ssh.exec_command(command)

    exit_code = stdout.channel.recv_exit_status()

    return (
        exit_code,
        stdout.read().decode(),
        stderr.read().decode()
    )


def file_exists_remote(ssh, remote_path):
    cmd = f"test -f {remote_path}"
    exit_code, _, _ = run_command(ssh, cmd)
    return exit_code == 0


def hdfs_exists(ssh, hdfs_path):
    cmd = f"hdfs dfs -test -e {hdfs_path}"
    exit_code, _, _ = run_command(ssh, cmd)
    return exit_code == 0


# funzione che estrae l'output dall'hdfs
def hdfs_cat(ssh, output_path):
    cmd = f"hdfs dfs -cat {output_path}/part-*"

    exit_code, stdout, stderr = run_command(ssh, cmd)

    if exit_code != 0:
        raise RuntimeError(stderr)

    return stdout


#------------------------------
# CONNECTION
#------------------------------

def connect_cluster():
    load_dotenv()

    hostname = os.getenv("HOSTNAME")
    username = os.getenv("USERNAME")
    key_path = os.getenv("SSH_KEY_PATH")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(
        hostname=hostname,
        username=username,
        key_filename=key_path
    )

    return ssh


#------------------------------
# HADOOP JOB
#------------------------------

def run_hadoop_job(
    ssh,
    mapper_path,
    reducer_path,
    input_path,
    output_path,
    local_output_file_path,
    log_output_file_path
):

    print("Avvio Hadoop Streaming Job...")

    # elimina output se esiste
    run_command(ssh, f"hdfs dfs -rm -r -f {output_path}")

    command = f"""
    hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -file {mapper_path} \
    -file {reducer_path} \
    -mapper "python3 {os.path.basename(mapper_path)}" \
    -reducer "python3 {os.path.basename(reducer_path)}" \
    -input {input_path} \
    -output {output_path}
    """

    # execution time
    start_time = time.time()

    exit_code, out, err = run_command(ssh, command)

    end_time = time.time()

    execution_time = end_time - start_time

    print("Job completed!")

    print(err)

    if exit_code == 0:
        print("Job completato con successo.")
    else:
        print("Errore durante il job Hadoop.")

    # parsing metrics
    metrics = parse_hadoop_metrics(err)

    # leggere output reale da HDFS
    output_data = hdfs_cat(ssh, output_path)

    print("===== MAPREDUCE OUTPUT =====")
    print(output_data)

    # salva output locale
    save_to_local_file(output_data, local_output_file_path)

    # salva log Hadoop
    save_log(err, execution_time, metrics, log_output_file_path)

    return execution_time


#------------------------------
# MAIN FUNCTION
#------------------------------

def cluster_executor(
    mapper_file,
    reducer_file,
    input_file,
    local_output_file_path,
    input_path,
    output_path,
    log_output_file_path
):

    # connessione cluster
    ssh = connect_cluster()

    # nomi file
    input_name = os.path.basename(input_file)
    mapper_name = os.path.basename(mapper_file)
    reducer_name = os.path.basename(reducer_file)

    # filesystem remoto Linux
    remote_input = f"/home/hadoop/{input_name}"
    remote_mapper = f"/home/hadoop/{mapper_name}"
    remote_reducer = f"/home/hadoop/{reducer_name}"

    # HDFS
    input_hdfs = f"/input/{input_name}"
    output_hdfs = f"/output/{os.path.basename(output_path)}"

    # crea directory HDFS
    run_command(ssh, "hdfs dfs -mkdir -p /input")
    run_command(ssh, "hdfs dfs -mkdir -p /output")

    print("Checking files...")

    sftp = ssh.open_sftp()

    # upload input su master node
    if not file_exists_remote(ssh, remote_input):
        print("Upload input...")
        sftp.put(input_file, remote_input)

    # upload mapper
    if not file_exists_remote(ssh, remote_mapper):
        print("Upload mapper...")
        sftp.put(mapper_file, remote_mapper)

    # upload reducer
    if not file_exists_remote(ssh, remote_reducer):
        print("Upload reducer...")
        sftp.put(reducer_file, remote_reducer)

    sftp.close()

    # upload input su HDFS
    if not hdfs_exists(ssh, input_hdfs):
        print("Upload file su HDFS...")

        run_command(
            ssh,
            f"hdfs dfs -put {remote_input} {input_hdfs}"
        )

    # run job
    execution_time = run_hadoop_job(
        ssh=ssh,
        mapper_path=remote_mapper,
        reducer_path=remote_reducer,
        input_path=input_hdfs,
        output_path=output_hdfs,
        local_output_file_path=local_output_file_path,
        log_output_file_path=log_output_file_path
    )

    ssh.close()

    return execution_time