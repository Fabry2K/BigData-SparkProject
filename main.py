import pandas as pd
import analysis


df = pd.read_csv("files/flight_data_2024.csv", dtype=str)

analysis.analyze_dataframe(df)

analysis.check_duplicates(df)