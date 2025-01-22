import argparse
from analytics import initialize_spark, generate_games_chart


def generate_top_games_casual(spark, output_path):
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
    casual = spark.sql(
        """
        SELECT g.app_name, SUM(CASE WHEN r.recommended='True' AND CAST(u.playtime_last_two_weeks AS FLOAT) < 10 THEN 1 ELSE 0 END) AS counted
        FROM reviews r
        JOIN users u ON r.user_id = u.user_id
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY counted DESC
        LIMIT 10
        """
    )
    casual.show()
    casual.toPandas().to_csv(output_path + "/Top Games Casual.csv", header=True)

    generate_games_chart(
        title="Top 3 Games for Casual Gamers - Podium",
        ylabel="Recommendations by Casual Gamers",
        output_path=output_path + "/Top Games Casual.png",
        games=casual.limit(3),
        selected_name="app_name",
    )


parser = argparse.ArgumentParser()
parser.add_argument(
    "--gold_path", type=str, required=True, help="Path to the gold layer"
)
args = parser.parse_args()
output_path = args.gold_path
spark = initialize_spark()
generate_top_games_casual(spark, output_path)
