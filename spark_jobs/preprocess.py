from utils.helpers import create_spark_session
from pyspark.sql.functions import col, year

spark = create_spark_session()

df = spark.read.csv("data/raw/climate_full_timeseries.csv", header=True, inferSchema=True)
df.printSchema()

df = df.withColumn("year", year("time"))

df = df.withColumn("lat_bin", (col("latitude") * 2).cast("int") / 2)
df = df.withColumn("lon_bin", (col("longitude") * 2).cast("int") / 2)

df.write.csv("output/cleaned_data.csv", header=True, mode="overwrite")

spark.stop()
