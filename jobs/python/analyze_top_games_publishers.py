import argparse

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from jobs.python.analyze_games_chart import generate_games_chart

parser = argparse.ArgumentParser()
parser.add_argument(
    "--gold_path", type=str, required=True, help="Path to the gold layer"
)

args = parser.parse_args()

output_path = args.gold_path

spark = (
    SparkSession.builder.appName("Save to db")
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.2.29.jre7.jar")
    .config(
        "spark.driver.extraClassPath",
        "/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
    )
    .getOrCreate()
)


def save_to_postgresql(df: DataFrame, table_name: str):
    jdbc_url = "jdbc:postgresql://postgres/airflow"
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver",
    }
    df.write.jdbc(
        url=jdbc_url, table=table_name, mode="overwrite", properties=properties
    )


def aggregate_gold_layer():
    jdbc_url = "jdbc:postgresql://postgres/airflow"
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver",
    }

    reviews_table = spark.read.jdbc(
        url=jdbc_url, table="reviews", properties=properties
    )
    games_table = spark.read.jdbc(url=jdbc_url, table="games", properties=properties)
    users_table = spark.read.jdbc(url=jdbc_url, table="users", properties=properties)
    # genres_table = spark.read.jdbc(url=jdbc_url, table="genres", properties=properties)
    regions_table = spark.read.jdbc(
        url=jdbc_url, table="regions", properties=properties
    )

    reviews_table.createOrReplaceTempView("reviews")
    games_table.createOrReplaceTempView("games")
    users_table.createOrReplaceTempView("users")
    # genres_table.createOrReplaceTempView("genres")
    regions_table.createOrReplaceTempView("regions")

    generate_top_games_publishers()


def generate_top_games_publishers():
    print("Top games publishers:")
    publishers = spark.sql(
        """
        SELECT 
            g.publisher,
            CASE 
                WHEN SUM(CASE WHEN r.recommended = 'False' THEN 1 ELSE 0 END) = 0 
                THEN NULL
                ELSE SUM(CASE WHEN r.recommended = 'True' THEN 1 ELSE 0 END) / SUM(CASE WHEN r.recommended = 'False' THEN 1 ELSE 0 END)
            END AS counted
        FROM games g
        JOIN reviews r ON g.app_id = r.app_id
        GROUP BY g.publisher
        HAVING (SUM(CASE WHEN r.recommended = 'True' THEN 1 ELSE 0 END) + 
            SUM(CASE WHEN r.recommended = 'False' THEN 1 ELSE 0 END)) >= 10
        ORDER BY counted DESC
        LIMIT 10;
    """
    )
    publishers.toPandas().to_parquet(
        output_path + "Top games publishers.parquet"
    )
    generate_games_chart(
        title="Top 3 Best Games Publishers - Podium",
        ylabel="Top games publishers",
        output_path=output_path + "Top games publishers",
        games=publishers.limit(3),
        selected_name="publisher"
    )


aggregate_gold_layer()
