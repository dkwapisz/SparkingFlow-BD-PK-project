import argparse
import transformers

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IncludeGamesData").getOrCreate()

print("Transformers version:", transformers.__version__)

parser = argparse.ArgumentParser(description="Load Games to bronze layer")
parser.add_argument("--model_name", type=str, required=True, help="Language model name for translation")
parser.add_argument("--silver_path", type=str, required=True, help="Path to the silver layer")

args = parser.parse_args()

model_name = args.model_name
input_path = args.silver_path + "steam_reviews/*/*.csv"
output_path = args.silver_path + "steam_reviews_translated"

tokenizer = transformers.MarianTokenizer.from_pretrained(model_name)
model = transformers.MarianMTModel.from_pretrained(model_name)


def translate_review(text):
    if not text:
        return None
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
    translated = model.generate(**inputs)
    return tokenizer.decode(translated[0], skip_special_tokens=True)


df = spark.read.csv(input_path, header=True, multiLine=True, quote='"', escape='"')

rdd = df.rdd.map(lambda row: (row["app_id"], row["review"], translate_review(row["review"])))

df_translated = rdd.toDF(["app_id", "review", "translated_reviews"])

df_translated.write.partitionBy("app_id").csv(output_path, header=True, quote='"', escape='"', mode="overwrite")

spark.stop()
