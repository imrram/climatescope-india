import os
import subprocess

def run_command(command, description):
    print(f"\n {description}...")
    full_command = f"PYTHONPATH={os.getcwd()} {command}"
    result = subprocess.run(full_command, shell=True)

    if result.returncode != 0:
        print(f"Failed: {description}")
        exit(1)

    print(f"Completed: {description}")


def ensure_dataset_and_convert():
    if not os.path.exists("data/raw/IMDAA_merged_1.08_1990_2020.nc"):
        print("Dataset not found. Downloading via kagglehub...")
        run_command("python3.10 scripts/download_bharatbench.py", "Downloading BharatBench Dataset")

    if not os.path.exists("data/raw/climate_full_timeseries.csv"):
        run_command("python3.10 scripts/convert_netcdf_to_csv.py", "Converting NetCDF to CSV")
    else:
        print("CSV already exists. Skipping conversion.")

def main():
    print("Starting ClimateScope Pipeline...")
    ensure_dataset_and_convert()
    run_command("spark-submit spark_jobs/preprocess.py", "Spark Preprocessing")
    run_command("spark-submit spark_jobs/compute_trends.py", "Compute Trends")
    run_command("spark-submit spark_jobs/benchmarking.py", "Benchmarking Spark vs Pandas")
    run_command("python3.10 dashboard/app.py", "Launching Dashboard")

if __name__ == "__main__":
    main()
