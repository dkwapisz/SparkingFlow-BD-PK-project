import json

from airflow.models import Variable
from pyspark.sql import SparkSession, functions as fun

spark = SparkSession.builder.master('local').appName("GetSample").getOrCreate()

df = spark.read.option("header", "true").csv("dags/datasets/steam_reviews.csv")

selected_df = df.select("app_id", "app_name", "language")

cleaned_df = selected_df.na.drop()

random_sample_df = cleaned_df.orderBy(fun.rand()).limit(50)

sample_data_json = random_sample_df.toJSON().collect()

print(f"Sample data JSON: {sample_data_json}")

Variable.set("sample_data", json.dumps(sample_data_json))

spark.stop()
