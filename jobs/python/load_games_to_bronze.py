import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadGamesToBronze").getOrCreate()

parser = argparse.ArgumentParser(description="Load Games to bronze layer")
parser.add_argument("--source_path", type=str, required=True, help="Path to the source directory")
parser.add_argument("--bronze_path", type=str, required=True, help="Path to the output directory")

args = parser.parse_args()

input_path = args.source_path + "games.csv"
output_path = args.bronze_path + "games"

df = spark.read.csv(input_path, header=True, multiLine=True, quote='"', escape='"')

df = df.coalesce(1)

df.write.csv(output_path, header=True, quote='"', escape='"', mode="overwrite")

print("Loaded games to Bronze layer")

spark.stop()