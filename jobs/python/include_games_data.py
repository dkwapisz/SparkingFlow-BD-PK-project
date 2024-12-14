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

# input_path = args.bronze_path + "steam_reviews"
# output_path = args.silver_path + "steam_reviews"
# input_games_path = args.bronze_path + "games.csv"

# games_df = spark.read.csv(
#     input_games_path, header=True, multiLine=True, quote='"', escape='"'
# )
# normalized_games = games_df.select(
#     col("app_id").cast("int").alias("app_id"),
#     col("title").alias("name"),
#     col("publisher"),
#     col("developer"),
#     col("date_release").cast("date"),
#     col("genre"),
# )
# for file_name in os.listdir(input_path):
#     if file_name.endswith(".csv"):
#         input_file_path = os.path.join(input_path, file_name)

#         print(f"Processing file: {input_file_path}")

#         df = spark.read.csv(
#             input_file_path, header=True, multiLine=True, quote='"', escape='"'
#         )


spark.stop()
