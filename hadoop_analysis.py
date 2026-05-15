import subprocess
import time
import sys

# Analisi 3.1 con Hadoop in Locale
def local_analysis_3_1(filepath, output_path):

    start_time = time.time()

    result = subprocess.run(
        [
            sys.executable,
            "mapreduce_3_1.py",
            filepath
        ],
        capture_output=True,
        text=True
    )

    output_lines = result.stdout.splitlines()

    # prendi solo le prime 10 righe NON vuote
    top_10 = [line for line in output_lines if line.strip()][:10]

    end_time = time.time()
    execution_time = end_time - start_time

    with open(output_path, "a", encoding="utf-8") as f:

        f.write("\n\n==============================\n")
        f.write("HADOOP MAPREDUCE ANALYSIS (TOP 10)\n")
        f.write("==============================\n\n")

        for line in top_10:
            f.write(line + "\n")

        f.write(f"\nExecution time: {execution_time:.2f} seconds\n")

    print(f"Hadoop execution time: {execution_time:.2f} seconds")

    return execution_time


# Analisi 3.3 con Hadoop in Locale
def local_analysis_3_3(filepath, output_path):

    start_time = time.time()

    result = subprocess.run(
        [
            sys.executable,
            "mapreduce_3_3.py",
            filepath
        ],
        capture_output=True,
        text=True
    )

    output_lines = result.stdout.splitlines()

    # prendi solo le prime 10 righe NON vuote
    top_10 = [line for line in output_lines if line.strip()][:10]

    end_time = time.time()
    execution_time = end_time - start_time

    with open(output_path, "a", encoding="utf-8") as f:

        f.write("\n\n==============================\n")
        f.write("HADOOP MAPREDUCE ANALYSIS 3.3 (TOP 10)\n")
        f.write("==============================\n\n")

        for line in top_10:
            f.write(line + "\n")

        f.write(f"\nExecution time: {execution_time:.2f} seconds\n")

    print(f"Hadoop execution time: {execution_time:.2f} seconds")

    return execution_time