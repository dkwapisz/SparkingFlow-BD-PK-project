import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("IncludeGamesData").getOrCreate()

parser = argparse.ArgumentParser(description="Load Games to bronze layer")
parser.add_argument(
    "--bronze_path", type=str, required=True, help="Path to the bronze layer"
)
parser.add_argument(
    "--silver_path", type=str, required=True, help="Path to the silver layer"
)
args = parser.parse_args()

input_path = args.bronze_path + "games"
output_path = args.silver_path + "games"

games_df = spark.read.parquet(
    input_path
)
normalized_games = games_df.select(
    col("AppID").alias("game_id"),
    col("Name").alias("game_name"),
    col("Release date").alias("release_date"),
    col("Price").cast("float").alias("price"),
    col("Required age").cast("int").alias("required_age"),
    col("Estimated owners").alias("estimated_owners"),
    col("Genres").alias("genres"),
    col("Publishers").alias("publishers"),
)

normalized_games.repartition(10).write.parquet(
    output_path, mode="overwrite"
)
spark.stop()
