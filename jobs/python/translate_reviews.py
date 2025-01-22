import argparse
import os
import requests
import logging
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit

logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder.appName("TranslateReviews").getOrCreate()

# Argument Parsing
parser = argparse.ArgumentParser(
    description="Load reviews to silver layer for translation"
)
parser.add_argument(
    "--model_name", type=str, required=True, help="Language model name for translation"
)
parser.add_argument(
    "--silver_path", type=str, required=True, help="Path to the silver layer"
)

args = parser.parse_args()

model_name = args.model_name
input_path = args.silver_path + "steam_reviews"
output_path = args.silver_path + "steam_reviews_translated"


# Translation function (worker-safe)
def translate_review_batch(partition, api_url, api_headers):
    translated_reviews = []
    for row in partition:
        # Convert Row to dict for easier manipulation
        row_dict = row.asDict()
        text = row_dict.get("review")
        if text:
            # data = {
            #     "model": "llama3.1:8b",
            #     "messages": [
            #         {
            #             "role": "user",
            #             "content": f"only translate to english without any additional explanation: {text}",
            #         }
            #     ],
            #     "stream": False,
            # }
            try:
                # response = requests.post(api_url, json=data, headers=api_headers).json()
                # translated_text = response["message"]["content"]
                response = {"message": {"content": text}}           # turn off translation
                translated_text = response["message"]["content"]    # turn off translation
                row_dict["translated_review"] = translated_text
            except Exception as e:
                logger.error(f"Translation failed for text: {text}, Error: {e}")
                row_dict["translated_review"] = None
        else:
            row_dict["translated_review"] = None

        translated_reviews.append(Row(**row_dict))

    return iter(translated_reviews)


# API configurations
api_url = "http://host.docker.internal:11434/api/chat"
api_headers = {"Accept": "application/json", "Content-Type": "application/json"}

# Process each language folder
languages = [
    d for d in os.listdir(input_path) if os.path.isdir(os.path.join(input_path, d))
]

for language in languages:
    language_path = os.path.join(input_path, language)
    parquet_files = [f for f in os.listdir(language_path) if f.endswith(".parquet")]
    for parquet_file in parquet_files:
        parquet_path = os.path.join(language_path, parquet_file)
        df = spark.read.parquet(
            parquet_path
        )

        # Translate reviews and retain all original columns
        translated_reviews_rdd = df.rdd.mapPartitions(
            lambda partition: translate_review_batch(partition, api_url, api_headers)
        )

        # Convert back to DataFrame
        translated_df = translated_reviews_rdd.toDF()

        # Save the translated file
        translated_df.write.parquet(
            f"{output_path}/original_{language}", mode="overwrite"
        )

spark.stop()
