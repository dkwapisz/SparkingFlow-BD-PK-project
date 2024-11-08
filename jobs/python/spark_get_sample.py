import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
from airflow.models import Variable

# Initialize Spark session
spark = SparkSession.builder.master('local').appName("GetSample").getOrCreate()

# Load CSV file with schema
df = spark.read.option("header", "true").csv("dags/datasets/steam_reviews.csv")

# Select specific columns
selected_columns_df = df.select("app_id", "app_name", "language")

# Sample approximately 1000 random rows
sample_df = df.orderBy(rand()).limit(1000)

# Convert DataFrame to JSON and save to Airflow Variable
sample_data_json = sample_df.toJSON().collect()
json_string = json.dumps(sample_data_json)
Variable.set("sample_data", json_string)

# Stop Spark session
spark.stop()
