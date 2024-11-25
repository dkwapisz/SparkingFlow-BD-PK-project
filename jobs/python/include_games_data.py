import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IncludeGamesData").getOrCreate()

parser = argparse.ArgumentParser(description="Load Games to bronze layer")
parser.add_argument("--bronze_path", type=str, required=True, help="Path to the bronze layer")
parser.add_argument("--silver_path", type=str, required=True, help="Path to the silver layer")



spark.stop()
