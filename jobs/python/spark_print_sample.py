import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from airflow.models import Variable

spark = SparkSession.builder.master('local').appName("PrintSample").getOrCreate()

sample_data = Variable.get("sample_data", default_var="[]")
print(f"Retrieved raw data from Airflow variable: {sample_data}")

if not sample_data or sample_data == "[]":
    raise ValueError("No data found in Airflow variable 'sample_data'")

data = json.loads(sample_data)
print("Parsed data:")
print(data)

schema = StructType([
    StructField("app_id", StringType(), True),
    StructField("app_name", StringType(), True),
    StructField("language", StringType(), True)
])

df = spark.read.json(spark.sparkContext.parallelize(data), schema=schema)

print("Final dataframe:")
for row in df.collect():
    print(row)

spark.stop()