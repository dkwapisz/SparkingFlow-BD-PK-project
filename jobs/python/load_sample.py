import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadSample").getOrCreate()

parser = argparse.ArgumentParser(description="Split Parquet into smaller partitions")
parser.add_argument(
    "--source_path", type=str, required=True, help="Path to the source directory"
)
parser.add_argument(
    "--num_partition", type=int, default=80, help="Number of partitions"
)
parser.add_argument(
    "--bronze_path", type=str, required=True, help="Path to the bronze directory"
)

args = parser.parse_args()

input_path = args.source_path + "steam_reviews.csv"
output_path = args.bronze_path + "steam_reviews"
num_partition = args.num_partition

print(f"Starting to split {input_path} into {num_partition} partitions")
print(f"Output directory: {output_path}")

df = spark.read.csv(input_path, header=True, multiLine=True, quote='"', escape='"')

df_repartitioned = df.repartition(num_partition)

first_partition = df_repartitioned.rdd.glom().filter(lambda x: len(x) > 0).take(1)

df_first_partition = spark.createDataFrame(first_partition[0], df_repartitioned.schema)

df_first_partition = df_first_partition.coalesce(1)

df_first_partition.write.parquet(
    output_path, mode="overwrite"
)

print("Repartitioning completed")

spark.stop()
