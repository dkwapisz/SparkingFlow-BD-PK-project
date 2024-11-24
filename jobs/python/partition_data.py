import argparse
import os
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IncludeGamesData").getOrCreate()

parser = argparse.ArgumentParser(description="Load Games to bronze layer")
parser.add_argument("--bronze_path", type=str, required=True, help="Path to the bronze layer")
parser.add_argument("--silver_path", type=str, required=True, help="Path to the silver layer")

args = parser.parse_args()

input_path = args.bronze_path + "steam_reviews"
output_path = args.silver_path + "steam_reviews"

allowed_languages = [
    'english', 'polish', 'russian', 'schinese', 'turkish', 
    'spanish', 'koreana', 'latam', 'brazilian', 'french', 
    'german', 'ukrainian', 'hungarian', 'tchinese', 'bulgarian', 
    'czech', 'italian', 'greek', 'thai', 'dutch', 'vietnamese', 
    'finnish', 'japanese', 'korean', 'portuguese', 'swedish',
    'norwegian', 'danish', 'romanian']

for file_name in os.listdir(input_path):
    if file_name.endswith(".csv"):
        input_file_path = os.path.join(input_path, file_name)

        print(f"Processing file: {input_file_path}")

        df = spark.read.csv(input_file_path, header=True, multiLine=True, quote='"', escape='"')

        if 'language' in df.columns:
            # df_filtered = df.filter(col("language").isin(allowed_languages))
            # df_filtered = df_filtered.coalesce(1)
            # df_filtered.write.partitionBy("language").csv(output_path, header=True, quote='"', escape='"', mode="overwrite")

            df = df.coalesce(1)
            df.write.partitionBy("language").csv(output_path, header=True, quote='"', escape='"', mode="overwrite")

        else:
            print(f"File {input_file_path} does not contain 'language' column. Skipping.")

print("Repartitioning completed")
spark.stop()