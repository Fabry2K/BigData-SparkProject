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


def upload_project(input_file, mapper_file, reducer_file):

    # INPUT
    input_key = f"{CLUSTER}/input/{input_file.split('/')[-1]}"
    s3.upload_file(input_file, BUCKET, input_key)

    # MAPPER
    mapper_key = f"{CLUSTER}/code/mapper.py"
    s3.upload_file(mapper_file, BUCKET, mapper_key)

    # REDUCER
    reducer_key = f"{CLUSTER}/code/reducer.py"
    s3.upload_file(reducer_file, BUCKET, reducer_key)

    print("Upload completato")

    return input_key, mapper_key, reducer_key



def analysis_3_1():
    # HADOOP AWS cluster output log
    log_path = "output/cluster/log_hadooop_3_1.txt"

    # elimina se esiste
    if os.path.exists(log_path):
        os.remove(log_path)

    # ricrea il file (vuoto)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

#   # Esecuzione Hadoop MapReduce su un quarto, metà, intera, doppia e quadrupla dimensione del file di input

#   # file 1/4x
    timer_hadoop_3_1_quarter = cluster_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_quarter", None, "analisi_3_1.csv", "hadoop_3_1_output", log_path)
    
#   # file 1/2x
    timer_hadoop_3_1_half = cluster_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_half", None, "analisi_3_1.csv", "hadoop_3_1_output", log_path)

#   # file 1x
    timer_hadoop_3_1 = cluster_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1", "output/hadoop_3_1_output", "analisi_3_1.csv", "hadoop_3_1_output", log_path)

#   # file 2x
    timer_hadoop_3_1_double = cluster_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_double", None, "analisi_3_1.csv", "hadoop_3_1_output", log_path)

#   # file 4x
    timer_hadoop_3_1_quadruple = cluster_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_quadruple", None, "analisi_3_1.csv", "hadoop_3_1_output", log_path)

#   # plot dei tempi HADOOP 
    plot.plot_analisi(timer_hadoop_3_1_quarter, timer_hadoop_3_1_half, timer_hadoop_3_1, timer_hadoop_3_1_double, timer_hadoop_3_1_quadruple, "Analisi 3.1 Hadoop AWS cluster Map Reduce", "output/cluster/hadoop_analysis_3_1.png")

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
