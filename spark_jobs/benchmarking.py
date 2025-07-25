from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, col, to_timestamp
import pandas as pd
import time

def create_spark_session(app_name="ClimateScopeBenchmark"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

TARGET_VARIABLE = "APCP_sfc" 

# Pandas Benchmark
print("\n Running Pandas Benchmark...")

start_pd = time.time()

df_pd = pd.read_csv("data/raw/climate_full_timeseries.csv")

df_filtered = df_pd[df_pd["variable"] == TARGET_VARIABLE]

# Correct datetime parsing based on your format: 01/01/90 0:00
df_filtered["year"] = pd.to_datetime(df_filtered["time"]).dt.year

# Group by year and calculate mean
result_pd = df_filtered.groupby("year")["value"].mean()

end_pd = time.time()

print(f"\n Pandas Results for {TARGET_VARIABLE}:\n", result_pd)
print(f" Pandas benchmark took {end_pd - start_pd:.2f} seconds")

# Spark Benchmark
print("\n⚡ Running Spark Benchmark...")

spark = create_spark_session()

start_spark = time.time()

df_sp = spark.read.csv("data/raw/climate_full_timeseries.csv", header=True, inferSchema=True)

df_sp_filtered = df_sp.filter(col("variable") == TARGET_VARIABLE)

df_sp_filtered = df_sp_filtered.withColumn("parsed_time", to_timestamp("time", "dd/MM/yy H:mm"))

df_sp_filtered = df_sp_filtered.withColumn("year", year("parsed_time"))

result_sp = df_sp_filtered.groupBy("year").agg(avg("value").alias("avg_value"))

end_spark = time.time()

print(f"\n Spark Results for {TARGET_VARIABLE}:")
result_sp.orderBy("year").show()

print(f" Spark benchmark took {end_spark - start_spark:.2f} seconds")

spark.stop()
