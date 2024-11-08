import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from airflow.models import Variable

# Initialize Spark session
spark = SparkSession.builder.master('local').appName("PrintSample").getOrCreate()

# Retrieve data from Airflow variable
data = json.loads(Variable.get("sample_data", default_var="[]"))

print("Retrieved data:")
print(data)

# Check if data is retrieved and is not empty
if not data:
    raise ValueError("No data found in Airflow variable 'sample_data'")

# Define schema
schema = StructType([
    StructField("app_id", StringType(), True),
    StructField("app_name", StringType(), True),
    StructField("language", StringType(), True)
])

# Create DataFrame from list of dictionaries
df = spark.read.json(spark.sparkContext.parallelize(data), schema=schema)

print("Final dataframe:")
df.show()

# Stop Spark session
spark.stop()