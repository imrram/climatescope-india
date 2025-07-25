from utils.helpers import create_spark_session
from pyspark.sql.functions import avg, col, year, to_date

spark = create_spark_session()

df = spark.read.csv("output/cleaned_data.csv", header=True, inferSchema=True)

# Extract year from time
df = df.withColumn("year", year(col("time").cast("date")))

# ✅ Overall trend by variable
trend_df = df.groupBy("variable", "year").agg(avg("value").alias("avg_value"))
trend_df.toPandas().to_csv("output/trend_by_variable.csv", index=False)

# 🌡 Temperature trend only
temp_df = df.filter(col("variable") == "TMP_2m") \
            .groupBy("year") \
            .agg(avg("value").alias("avg_temp"))
temp_df.toPandas().to_csv("output/temperature_trend.csv", index=False)

# 🌧 Rainfall trend only
rain_df = df.filter(col("variable") == "APCP_sfc") \
            .groupBy("year") \
            .agg(avg("value").alias("avg_rainfall"))
rain_df.toPandas().to_csv("output/rainfall_trend.csv", index=False)

# 🔥 Heatwave detection: count number of days > 313.15 K per year
# Filter temperature values above 40°C (313.15 K)
heat_df = df.filter((col("variable") == "TMP_2m") & (col("value") > 313.15))

# Extract date (so we count distinct heatwave *days*)
heat_df = heat_df.withColumn("date", to_date("time"))
heat_df = heat_df.select("date").distinct().withColumn("year", year("date"))

# Count number of heatwave days per year
heatwave_by_year = heat_df.groupBy("year").count().withColumnRenamed("count", "heatwave_days")

# Save to CSV
heatwave_by_year.toPandas().to_csv("output/heatwave_days.csv", index=False)

spark.stop()
