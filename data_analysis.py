import pandas as pd
import missingno as msno
import matplotlib.pyplot as plt

df = pd.read_csv("files/flight_data_2024.csv", low_memory=False)

# ordina per DepTime

# prima_riga_nulla = df_sorted[df_sorted.isnull().any(axis=1)].head(1)
# print(prima_riga_nulla)







cols = [
    "air_time",
    "dep_time",
    "dep_delay",
    "taxi_out",
    "wheels_off",
    "wheels_on",
    "taxi_in",
    "arr_time",
    "arr_delay",
    "actual_elapsed_time"
]

# tieni solo colonne utili + cancelled
df_clean = df[["cancelled"] + cols]

# =========================
# 1. NULL GLOBALI
# =========================
null_all = df_clean[cols].isnull().mean() * 100
print("NULL % GLOBALI")
print(null_all.sort_values(ascending=False))

# =========================
# 2. NULL SOLO CANCELLED
# =========================
df_cancelled = df_clean[df_clean["cancelled"] == 1]

null_cancelled = df_cancelled[cols].isnull().mean() * 100
print("\nNULL % CANCELLED")
print(null_cancelled.sort_values(ascending=False))

# =========================
# 3. DIFFERENZA (cosa volevi tu)
# =========================
diff = null_cancelled - null_all
print("\nDIFFERENZA (cancelled - all)")
print(diff.sort_values(ascending=False))



df_flights = df[df["cancelled"] == 0].copy()
print(df_flights[cols].isnull().sum())

df_unique = df_flights.drop_duplicates(subset=["fl_date", "op_unique_carrier", "op_carrier_fl_num"])
print(df_unique[cols].isnull().sum())

# df_sorted = df_clean.sort_values(by="cancelled", ascending=False)

# # matrice missingno sull'ordinato
# plt.figure(figsize=(6, 6))
# msno.matrix(df_sorted[["cancelled"] + cols])
# plt.show()