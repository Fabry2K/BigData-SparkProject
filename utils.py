import pandas as pd
from pathlib import Path


def generate_scaled_datasets(file_path):

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

    # ==========================================================
    # DATASET 1/4
    # ==========================================================

    if not quarter_path.exists():

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

    else:
        print(f"\nEsiste già: {quarter_path}")

    # ==========================================================
    # DATASET 1/2
    # ==========================================================

    if not half_path.exists():

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

    else:
        print(f"\nEsiste già: {half_path}")

    # ==========================================================
    # DATASET 2x
    # ==========================================================

    if not double_path.exists():

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

    else:
        print(f"\nEsiste già: {double_path}")

    # ==========================================================
    # DATASET 4x
    # ==========================================================

    if not quadruple_path.exists():

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

    else:
        print(f"\nEsiste già: {quadruple_path}")