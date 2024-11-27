import argparse
import os

import transformers
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("TranslateReviews").getOrCreate()

parser = argparse.ArgumentParser(description="Load reviews to silver layer for translation")
parser.add_argument("--model_name", type=str, required=True, help="Language model name for translation")
parser.add_argument("--silver_path", type=str, required=True, help="Path to the silver layer")

args = parser.parse_args()

model_name = args.model_name
input_path = args.silver_path + "steam_reviews"
output_path = args.silver_path + "steam_reviews_translated"

tokenizer = transformers.MarianTokenizer.from_pretrained(model_name)
model = transformers.MarianMTModel.from_pretrained(model_name)


def print_language_info(language, csv_path):
    print("-" * 50)
    print(f"Translating reviews for {language} language. Path: {csv_path}")
    print("-" * 50)


def translate_review(text):
    if text:
        inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
        translated = model.generate(**inputs)
        translated_text = tokenizer.decode(translated[0], skip_special_tokens=True)
        return translated_text
    return None


translate_udf = udf(translate_review, StringType())

languages = [d for d in os.listdir(input_path) if os.path.isdir(os.path.join(input_path, d))]

for language in languages:
    language_path = os.path.join(input_path, language)
    csv_files = [f for f in os.listdir(language_path) if f.endswith(".csv")]
    for csv_file in csv_files:
        csv_path = os.path.join(language_path, csv_file)
        df = spark.read.csv(csv_path, header=True, multiLine=True, quote='"', escape='"')

        print_language_info(language, csv_path)

        df_reviews = df.select("app_id", "review")

        df_translated = df_reviews.withColumn("translated_reviews", translate_udf(df_reviews["review"]))

        df_translated.write.csv(f"{output_path}/original_{language}", header=True, quote='"', escape='"', mode="overwrite")

spark.stop()
