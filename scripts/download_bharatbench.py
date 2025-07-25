import os
import shutil
import kagglehub

RAW_PATH = "data/raw"

os.makedirs(RAW_PATH, exist_ok=True)

already_downloaded = len(os.listdir(RAW_PATH)) > 0

if not already_downloaded:
    print("Downloading BharatBench dataset via kagglehub...")

    path = kagglehub.dataset_download("maslab/bharatbench")

    for filename in os.listdir(path):
        src = os.path.join(path, filename)
        dst = os.path.join(RAW_PATH, filename)
        shutil.move(src, dst)

    shutil.rmtree(path)

    print(f"Dataset moved to: {RAW_PATH}")
    print(f"Cleaned kagglehub cache at: {path}")
else:
    print(f"Dataset already exists in: {RAW_PATH}. Skipping download.")
