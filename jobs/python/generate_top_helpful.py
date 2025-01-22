import argparse
from analytics import initialize_spark


def generate_top_helpful_reviews(spark, output_path):
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
    most_helpful = spark.sql(
        """
        SELECT r.translated_review, g.app_name, r.votes_helpful AS counted
        FROM reviews r
        JOIN games g ON r.app_id = g.app_id
        ORDER BY counted DESC
        LIMIT 10
        """
    )
    most_helpful.show()
    most_helpful.toPandas().to_csv(
        output_path + "/Top Helpful Reviews.csv", header=True
    )


parser = argparse.ArgumentParser()
parser.add_argument(
    "--gold_path", type=str, required=True, help="Path to the gold layer"
)
args = parser.parse_args()
output_path = args.gold_path
spark = initialize_spark()
generate_top_helpful_reviews(spark, output_path)
