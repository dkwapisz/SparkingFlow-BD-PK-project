import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SplitCSV").getOrCreate()

parser = argparse.ArgumentParser(description="Split CSV into smaller partitions")
parser.add_argument("--input_path", type=str, required=True, help="Path to the input CSV file")
parser.add_argument("--num_partition", type=int, default=10, help="Number of partitions")
parser.add_argument("--output_path", type=str, required=True, help="Path to the output directory")

args = parser.parse_args()

input_path = args.input_path
output_path = args.output_path
num_partition = args.num_partition

print(f"Starting to split {input_path} into {num_partition} partitions")
print(f"Output directory: {output_path}")

df = spark.read.csv(input_path, header=True, quote='"', escape="\\")

df_repartitioned = df.repartition(num_partition)

df_repartitioned.write.csv(output_path, header=True, mode="overwrite")

print("Repartitioning completed")

spark.stop()