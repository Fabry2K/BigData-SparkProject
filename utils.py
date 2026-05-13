from pathlib import Path
import pandas as pd

def append_to_log(title, content, logfile="output/log.txt"):

    Path("output").mkdir(exist_ok=True)

    with open(logfile, "a", encoding="utf-8") as f:

        f.write("\n")
        f.write("=" * 80)
        f.write("\n")

        f.write(title)

        f.write("\n")
        f.write("=" * 80)
        f.write("\n")

        f.write(content)

        f.write("\n")

# per ora li salva in partizioni, non so se mantenere così
def generate_scaled_datasets_local(file_path):

    input_path = Path(file_path)

    original_name = input_path.stem
    output_dir = input_path.parent

    quarter_path = output_dir / f"{original_name}_quarter.csv"
    half_path = output_dir / f"{original_name}_half.csv"
    double_path = output_dir / f"{original_name}_double.csv"
    quadruple_path = output_dir / f"{original_name}_quadruple.csv"

    # Lettura dataset originale
    df = pd.read_csv(file_path)

    original_size = len(df)
    print(f"Dataset originale: {original_size} righe")

    # DATASET 1/4
    print("\nCreazione dataset 1/4...")

    df_quarter = df.sample(
        frac=0.25,
        random_state=42
    )

    df_quarter.to_csv(
        quarter_path,
        index=False
    )

    print(f"Creato: {quarter_path}")


    # DATASET 1/2
    print("\nCreazione dataset 1/2...")

    df_half = df.sample(
        frac=0.50,
        random_state=42
    )

    df_half.to_csv(
        half_path,
        index=False
    )

    print(f"Creato: {half_path}")



    # DATASET 2x
    print("\nCreazione dataset doppio...")

    df_double = pd.concat(
        [df, df],
        ignore_index=True
    )

    df_double.to_csv(
        double_path,
        index=False
    )

    print(f"Creato: {double_path}")


    # DATASET 4x
    print("\nCreazione dataset quadruplo...")

    df_quadruple = pd.concat(
        [df, df, df, df],
        ignore_index=True
    )

    df_quadruple.to_csv(
        quadruple_path,
        index=False
    )

    print(f"Creato: {quadruple_path}")



# per ora li salva in partizioni, non so se mantenere così
def generate_scaled_datasets_cluster(spark, file_path):

    input_path = Path(file_path)

    original_name = input_path.stem
    output_dir = input_path.parent

    quarter_path = output_dir / f"{original_name}_quarter"
    half_path = output_dir / f"{original_name}_half"
    double_path = output_dir / f"{original_name}_double"
    quadruple_path = output_dir / f"{original_name}_quadruple"

    # Lettura dataset originale
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    original_size = df.count()
    print(f"Dataset originale: {original_size} righe")

    # 1/4
    df.sample(False, 0.25, seed=42) \
        .write.mode("overwrite").csv(str(quarter_path), header=True)

    print(f"Creato: {quarter_path}")

    # 1/2
    df.sample(False, 0.50, seed=42) \
        .write.mode("overwrite").csv(str(half_path), header=True)

    print(f"Creato: {half_path}")

    # 2x
    df.union(df) \
        .write.mode("overwrite").csv(str(double_path), header=True)

    print(f"Creato: {double_path}")

    # 4x
    df.union(df).union(df).union(df) \
        .write.mode("overwrite").csv(str(quadruple_path), header=True)

    print(f"Creato: {quadruple_path}")
