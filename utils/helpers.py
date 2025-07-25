import json
from pyspark.sql import SparkSession

def load_config(path="config/spark_config.json"):
    with open(path) as f:
        return json.load(f)

def create_spark_session():
    config = load_config()
    return SparkSession.builder.appName(config["app_name"]).master(config["master"]).getOrCreate()
