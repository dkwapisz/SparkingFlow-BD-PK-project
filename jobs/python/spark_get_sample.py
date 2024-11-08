import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
from airflow.models import Variable

spark = SparkSession.builder.master('local').appName("GetSample").getOrCreate()

df = spark.read.option("header", "true").csv("dags/datasets/steam_reviews.csv")

selected_columns_df = df.select("app_id", "app_name", "language")

print("Sample dataframe:")
selected_columns_df.show()

sample_df = selected_columns_df.orderBy(rand()).limit(50)

filtered_sample_df = sample_df.dropna(how='all')

sample_data_json = filtered_sample_df.toJSON().collect()
print(f"Sample data JSON: {sample_data_json}")

Variable.set("sample_data", json.dumps(sample_data_json))

spark.stop()