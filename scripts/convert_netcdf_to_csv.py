import os
import xarray as xr
import pandas as pd

RAW_DIR = "data/raw"
OUTPUT_CSV = os.path.join(RAW_DIR, "climate_full_timeseries.csv")
ALL_FOLDERS = ["Atmospheric_variable", "Surface_variables"]

df_all = []

for folder in ALL_FOLDERS:
    folder_path = os.path.join(RAW_DIR, folder)
    if not os.path.isdir(folder_path):
        continue

    for file in os.listdir(folder_path):
        if file.endswith(".nc"):
            file_path = os.path.join(folder_path, file)
            print(f"Processing {file_path}...")

            try:
                ds = xr.open_dataset(file_path)
                var_name = list(ds.data_vars)[0]
                df = ds[var_name].to_dataframe(name="value").reset_index()
                df["variable"] = var_name

                df_all.append(df.dropna())
            except Exception as e:
                print(f"Failed to read {file_path}: {e}")

if df_all:
    df_combined = pd.concat(df_all)
    df_combined.to_csv(OUTPUT_CSV, index=False)
    print(f"\nCombined and normalized climate data saved to: {OUTPUT_CSV}")
else:
    print("No usable NetCDF files found.")
