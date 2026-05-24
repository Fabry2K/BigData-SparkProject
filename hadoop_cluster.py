import paramiko
import os
from dotenv import load_dotenv


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


def ensure_hdfs_dir(ssh, hdfs_dir):
    if not hdfs_exists(ssh, hdfs_dir):
        print(f"Creo directory HDFS: {hdfs_dir}")
        run_command(ssh, f"hdfs dfs -mkdir -p {hdfs_dir}")
    else:
        print(f"Directory HDFS già presente: {hdfs_dir}")


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

def run_hadoop_job(ssh, mapper_path, reducer_path, input_path, output_path):

    print("Avvio Hadoop Streaming Job...")

    # elimina output se esiste
    run_command(ssh, f"hdfs dfs -rm -r -f {output_path}")

    command = f"""
    hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -file {mapper_path} -mapper {os.path.basename(mapper_path)} \
    -file {reducer_path} -reducer {os.path.basename(reducer_path)} \
    -input {input_path} \
    -output {output_path}
    """

    exit_code, out, err = run_command(ssh, command)

    print(out)
    print(err)

    if exit_code == 0:
        print("Job completato con successo.")
    else:
        print("Errore durante il job Hadoop.")


#------------------------------
# MAIN FUNCTION
#------------------------------

def cluster_executor(local_file, mapper_path, reducer_path, output_path):

    ssh = connect_cluster()

    filename = os.path.basename(local_file)

    remote_file = f"/home/hadoop/{filename}"
    hdfs_file = f"/input/{filename}"


    # -------------------------
    # 1. MASTER NODE
    # -------------------------

    if file_exists_remote(ssh, remote_file):
        print("File già presente sul master node.")
    else:
        print("Upload file sul master node...")

        sftp = ssh.open_sftp()
        sftp.put(local_file, remote_file)
        sftp.close()

        print("Upload completato.")


    # -------------------------
    # 2. HDFS SETUP
    # -------------------------

    ensure_hdfs_dir(ssh, "/input")
    ensure_hdfs_dir(ssh, "/output")


    # -------------------------
    # 3. HDFS FILE
    # -------------------------

    if hdfs_exists(ssh, hdfs_file):
        print("File già presente su HDFS.")
    else:
        print("Carico file su HDFS...")

        run_command(
            ssh,
            f"hdfs dfs -put {remote_file} {hdfs_file}"
        )


    # -------------------------
    # 4. RUN MAPREDUCE JOB
    # -------------------------

    run_hadoop_job(
        ssh,
        mapper_path=mapper_path,
        reducer_path=reducer_path,
        input_path=hdfs_file,
        output_path=output_path
    )


    ssh.close()