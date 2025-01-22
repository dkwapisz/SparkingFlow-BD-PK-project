import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("IncludeGamesData").getOrCreate()

parser = argparse.ArgumentParser(description="Partition data by language")
parser.add_argument("--bronze_path", type=str, required=True, help="Path to the bronze layer")
parser.add_argument("--silver_path", type=str, required=True, help="Path to the silver layer")
args = parser.parse_args()

input_path = args.bronze_path + "/steam_reviews/*.parquet"
output_path = args.silver_path + "/steam_reviews"

allowed_languages = [
    "english", "polish", "russian", "schinese", "turkish", "spanish", "koreana",
    "latam", "brazilian", "french", "german", "ukrainian", "hungarian", "tchinese",
    "bulgarian", "czech", "italian", "greek", "thai", "dutch", "vietnamese",
    "finnish", "japanese", "korean", "portuguese", "swedish", "norwegian",
    "danish", "romanian",
]

print(f"Loading files from {input_path}")
df = spark.read.parquet(input_path)

df = df.filter(F.col("language").isin(allowed_languages)) \
       .withColumn("language_partition", F.col("language"))

df = df.repartition(128, "language_partition")

print(f"Saving data to {output_path}")
df.write.partitionBy("language_partition").parquet(output_path, mode="overwrite")

print("Processing completed")
spark.stop()
