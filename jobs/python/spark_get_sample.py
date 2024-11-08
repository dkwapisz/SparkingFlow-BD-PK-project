from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import rand
from airflow.models import Variable

# Initialize Spark session
spark = SparkSession.builder.master('local').appName("GetSample").getOrCreate()

# Define schema
schema = StructType([
    StructField("app_id", StringType(), True),
    StructField("app_name", StringType(), True),
    StructField("language", StringType(), True)
])

# Load CSV file
df = (spark.read.format("csv")
      .option("header", "true")
      .load('/opt/airflow/dags/datasets/steam_reviews.csv')
      .select("app_id", "app_name", "language"))

# Sample 1000 random rows
sample_df = df.orderBy(rand()).limit(1000)

# Convert DataFrame to JSON and save to Airflow Variable
sample_data = sample_df.toJSON().collect()
Variable.set("sample_data", sample_data)

# Stop Spark session
spark.stop()