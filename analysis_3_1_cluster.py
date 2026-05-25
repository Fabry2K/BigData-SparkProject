from hadoop_cluster import cluster_executor
import os
import plot

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
    timer_hadoop_3_1_quarter = cluster_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_quarter", None, "analisi_3_1.csv", "cluster/quarter/hadoop_3_1_output", log_path)
    
#   # file 1/2x
    timer_hadoop_3_1_half = cluster_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_half", None, "analisi_3_1.csv", "cluster/half/hadoop_3_1_output", log_path)

#   # file 1x
    timer_hadoop_3_1 = cluster_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1", "output/hadoop_3_1_output", "analisi_3_1.csv", "cluster/hadoop_3_1_output", log_path)

#   # file 2x
    timer_hadoop_3_1_double = cluster_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_double", None, "analisi_3_1.csv", "cluster/double/hadoop_3_1_output", log_path)

#   # file 4x
    timer_hadoop_3_1_quadruple = cluster_executor("hadoop_3_1/mapper.py", "hadoop_3_1/reducer.py", "files/analisi_3_1_quadruple", None, "analisi_3_1.csv", "cluster/quadruple/hadoop_3_1_output", log_path)

#   # plot dei tempi HADOOP 
    plot.plot_analisi(timer_hadoop_3_1_quarter, timer_hadoop_3_1_half, timer_hadoop_3_1, timer_hadoop_3_1_double, timer_hadoop_3_1_quadruple, "Analisi 3.1 Hadoop AWS cluster Map Reduce", "output/cluster/hadoop_analysis_3_1.png")

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
