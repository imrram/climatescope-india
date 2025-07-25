import os
import shutil
import kagglehub

RAW_PATH = "data/raw"

# Ensure data/raw folder exists
os.makedirs(RAW_PATH, exist_ok=True)

# Check if already downloaded (based on any content)
already_downloaded = len(os.listdir(RAW_PATH)) > 0

if not already_downloaded:
    print("📥 Downloading BharatBench dataset via kagglehub...")

    # Downloads to ~/.kagglehub/maslab/bharatbench/
    path = kagglehub.dataset_download("maslab/bharatbench")

    # Move all contents into data/raw
    for filename in os.listdir(path):
        src = os.path.join(path, filename)
        dst = os.path.join(RAW_PATH, filename)
        shutil.move(src, dst)

    # Delete the original kagglehub cache folder
    shutil.rmtree(path)

    print(f"✅ Dataset moved to: {RAW_PATH}")
    print(f"🧹 Cleaned kagglehub cache at: {path}")
else:
    print(f"✅ Dataset already exists in: {RAW_PATH}. Skipping download.")
