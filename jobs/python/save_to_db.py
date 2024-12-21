from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import (
    monotonically_increasing_id,
    col,
    explode,
    split,
    row_number,
    trim,
)
from pyspark.sql.window import Window
import argparse
import os


def show_debug_info(df, message):
    print(f"DEBUG: {message}")
    df.show(truncate=False)


spark = (
    SparkSession.builder.appName("Save to db")
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.2.29.jre7.jar")
    .config(
        "spark.driver.extraClassPath",
        "/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )
    .getOrCreate()
)

parser = argparse.ArgumentParser(description="Load reviews to db")
parser.add_argument(
    "--silver_path", type=str, required=True, help="Path to the silver layer"
)

args = parser.parse_args()

input_path = args.silver_path + "steam_reviews"
input_path_games = args.silver_path + "games"
jdbc_url = "jdbc:postgresql://postgres/airflow"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
}


def save_to_postgresql(df: DataFrame, table_name: str):
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)


def create_reference_tables():
    regions_schema = StructType(
        [
            StructField("region_id", IntegerType(), False),
            StructField("language", StringType(), True),
        ]
    )
    regions_df = spark.createDataFrame([], schema=regions_schema)
    regions_df.write.jdbc(
        url=jdbc_url, table="regions", mode="overwrite", properties=properties
    )
    genres_schema = StructType(
        [
            StructField("genre_id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )
    genres_df = spark.createDataFrame([], schema=genres_schema)
    genres_df.write.jdbc(
        url=jdbc_url, table="genres", mode="overwrite", properties=properties
    )
    publishers_schema = StructType(
        [
            StructField("publisher_id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )
    publishers_df = spark.createDataFrame([], schema=publishers_schema)
    publishers_df.write.jdbc(
        url=jdbc_url, table="publishers", mode="overwrite", properties=properties
    )


def normalize_silver_to_gold():
    languages = [
        d for d in os.listdir(input_path) if os.path.isdir(os.path.join(input_path, d))
    ]

    games = [
        os.path.join(input_path_games, f)
        for f in os.listdir(input_path_games)
        if f.endswith(".csv")  # Include only CSV files
    ]

    create_reference_tables()
    print(f"Silver Path: {args.silver_path}")
    print(f"Input Path Games: {input_path_games}")
    print("Games folder: {}".format(games))

    silver_data = spark.read.csv(
        games, header=True, multiLine=True, quote='"', escape='"'
    )

    # Normalize Publishers
    publishers_table = (
        silver_data.select(explode(split(col("publishers"), ",")).alias("name"))
        .withColumn("name", trim(col("name")))  # Remove leading/trailing spaces
        .distinct()
        .withColumn(
            "publisher_id", row_number().over(Window.orderBy("name"))
        )  # Assign unique IDs
    )
    show_debug_info(publishers_table, "")
    save_to_postgresql(publishers_table, "publishers")

    # Normalize Genres
    genres_table = (
        silver_data.select(explode(split(col("genres"), ",")).alias("name"))
        .withColumn("name", trim(col("name")))  # Remove leading/trailing spaces
        .distinct()
        .withColumn(
            "genre_id", row_number().over(Window.orderBy("name"))
        )  # Assign unique IDs
    )
    save_to_postgresql(genres_table, "genres")

    # Normalize Games
    # Enrich with normalized IDs for genres and publishers
    games_table = silver_data.select(
        col("game_id").alias("app_id"),
        col("game_name").alias("app_name"),
        col("genres"),
        col("publishers"),
    ).distinct()

    # Optional: Split and explode genres and publishers for easier joins later
    games_table = (
        games_table.withColumn("genre", explode(split(col("genres"), ",")))
        .withColumn("publisher", explode(split(col("publishers"), ",")))
        .withColumn("genre", trim(col("genre")))
        .withColumn("publisher", trim(col("publisher")))
    )
    show_debug_info(games_table, "")
    save_to_postgresql(games_table, "games")

    regions_table = (
        spark.read.csv(
            os.path.join(input_path, "*/*.csv"), header=True, multiLine=True, quote='"', escape='"'
        )
        .select("language")
        .distinct()
        .withColumn("region_id", row_number().over(Window.orderBy("language")))
    )
    save_to_postgresql(regions_table, "regions")

    csv_paths = []
    for language in languages:
        language_path = os.path.join(input_path, language)
        csv_files = [f for f in os.listdir(language_path) if f.endswith(".csv")]
        for csv_file in csv_files:
            csv_paths.append(os.path.join(language_path, csv_file))

    silver_data = (
        spark.read.csv(
            csv_paths, header=True, multiLine=True, quote='"', escape='"'
        )
        .withColumnRenamed("author.steamid", "author_steamid")
        .withColumnRenamed("author.num_games_owned", "author_num_games_owned")
        .withColumnRenamed("author.num_reviews", "author_num_reviews")
        .withColumnRenamed("author.playtime_forever", "author_playtime_forever")
        .withColumnRenamed(
            "author.playtime_last_two_weeks", "author_playtime_last_two_weeks"
        )
        .withColumnRenamed(
            "author.playtime_at_review", "author_playtime_at_review"
        )
        .withColumnRenamed("author.last_played", "author_last_played")
    )

    users_table = silver_data.select(
        silver_data["author_steamid"].alias("user_id"),
        silver_data["author_num_games_owned"].alias("num_games_owned"),
        silver_data["author_num_reviews"].alias("num_reviews"),
        silver_data["author_playtime_forever"].alias("playtime_forever"),
        silver_data["author_playtime_last_two_weeks"].alias(
            "playtime_last_two_weeks"
        ),
        silver_data["author_last_played"].alias("last_played"),
    ).distinct()
    save_to_postgresql(users_table, "users")

    regions_df = spark.read.jdbc(
        url=jdbc_url, table="regions", properties=properties
    )

    reviews_table = (
        silver_data.join(
            regions_df,
            silver_data["language"] == regions_df["language"],
            "left",
        )
        .select(
            "review_id",
            "app_id",
            "review",
            "recommended",
            "votes_helpful",
            "votes_funny",
            "weighted_vote_score",
            "comment_count",
            "steam_purchase",
            "received_for_free",
            "written_during_early_access",
            "timestamp_created",
            "timestamp_updated",
            regions_df["region_id"].alias("region_id"),
        )
        .distinct()
    )
    save_to_postgresql(reviews_table, "reviews")


normalize_silver_to_gold()
