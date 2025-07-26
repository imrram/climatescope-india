# ClimateScope India

Analyze historical climate trends using BharatBench dataset via Apache Spark and Dash.

## Features

- Spark ETL for temperature and rainfall trends
- Benchmarking: Spark vs Pandas
- Interactive dashboard for state-level climate insights

## Setup

1. Download BharatBench dataset
2. Run:
   ```bash
   spark-submit spark_jobs/preprocess.py
   spark-submit spark_jobs/compute_trends.py
   spark-submit spark_jobs/benchmarking.py
   python dashboard/app.py

---

