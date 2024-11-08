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

print("Sample dataframe:")
selected_columns_df.show()

# Sample approximately 50 random rows
sample_df = selected_columns_df.orderBy(rand()).limit(50)

# Filter out rows where all columns are null
filtered_sample_df = sample_df.dropna(how='all')

# Convert DataFrame to list of JSON strings
sample_data_json = filtered_sample_df.toJSON().collect()
print(f"Sample data JSON: {sample_data_json}")

# Store JSON data in Airflow variable
Variable.set("sample_data", json.dumps(sample_data_json))

# Stop Spark session
spark.stop()