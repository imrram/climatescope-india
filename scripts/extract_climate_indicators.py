from utils.helpers import create_spark_session
from pyspark.sql.functions import avg, col, year

spark = create_spark_session()

df = spark.read.csv("output/cleaned_data.csv", header=True, inferSchema=True)

# Extract year from time
df = df.withColumn("year", year(col("time").cast("date")))

# ✅ Overall trend by variable
trend_df = df.groupBy("variable", "year").agg(avg("value").alias("avg_value"))
trend_df.write.csv("output/trend_by_variable.csv", header=True, mode="overwrite")

# 🌡 Temperature trend only
temp_df = df.filter(col("variable") == "TMP_2m") \
            .groupBy("year") \
            .agg(avg("value").alias("avg_temp"))
temp_df.write.csv("output/temperature_trend.csv", header=True, mode="overwrite")

# 🌧 Rainfall trend only
rain_df = df.filter(col("variable") == "APCP_sfc") \
            .groupBy("year") \
            .agg(avg("value").alias("avg_rainfall"))
rain_df.write.csv("output/rainfall_trend.csv", header=True, mode="overwrite")

# 🔥 Heatwave event detection (e.g., days > 45°C)
heat_df = df.filter((col("variable") == "TMP_2m") & (col("value") > 318.15))  # >45°C in Kelvin
heatwave_days = heat_df.groupBy("year").count().withColumnRenamed("count", "heatwave_days")

# Save as single CSV using pandas
heatwave_days.toPandas().to_csv("output/heatwave_days.csv", index=False)

spark.stop()
