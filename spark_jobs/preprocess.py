from utils.helpers import create_spark_session
from pyspark.sql.functions import col, year

spark = create_spark_session()

df = spark.read.csv("data/raw/climate_full_timeseries.csv", header=True, inferSchema=True)
df.printSchema()

# Extract year
df = df.withColumn("year", year("time"))

# Optional: bin lat/lon to reduce spatial resolution
df = df.withColumn("lat_bin", (col("latitude") * 2).cast("int") / 2)
df = df.withColumn("lon_bin", (col("longitude") * 2).cast("int") / 2)

# Save for further processing
df.write.csv("output/cleaned_data.csv", header=True, mode="overwrite")

spark.stop()
