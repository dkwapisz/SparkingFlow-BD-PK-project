from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from airflow.models import Variable
import json

# Initialize Spark session
spark = SparkSession.builder.master('local').appName("PrintSample").getOrCreate()

# Retrieve data from Airflow variable
data = Variable.get("sample_data", default_var=None)

# Check if data is retrieved and is not empty
if not data:
    raise ValueError("No data found in Airflow variable 'sample_data'")

# Filter out invalid JSON strings
valid_data = []
for item in data:
    try:
        json.loads(item)
        valid_data.append(item)
    except json.JSONDecodeError:
        print(f"Invalid JSON string found {item}")
        continue

# Define schema
schema = StructType([
    StructField("app_id", StringType(), True),
    StructField("app_name", StringType(), True),
    StructField("language", StringType(), True)
])

# Create DataFrame from data
df = spark.read.json(spark.sparkContext.parallelize(data), schema=schema)

print("Final JSON data:")
print(data)

# Print data
print("Final dataframe:")
df.show()

# Stop Spark session
spark.stop()