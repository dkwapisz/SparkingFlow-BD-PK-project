import argparse
from analytics import initialize_spark, generate_games_chart


def generate_top_games_publishers(spark, output_path):
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
        LIMIT 10
        """
    )
    publishers.show()
    publishers.toPandas().to_csv(output_path + "/Top Games Publishers.csv", header=True)

    generate_games_chart(
        title="Top 3 Game Publishers - Podium",
        ylabel="Recommendation Ratio",
        output_path=output_path + "/Top Games Publishers.png",
        games=publishers.limit(3),
        selected_name="publisher",
    )


parser = argparse.ArgumentParser()
parser.add_argument(
    "--gold_path", type=str, required=True, help="Path to the gold layer"
)
args = parser.parse_args()
output_path = args.gold_path
spark = initialize_spark()
generate_top_games_publishers(spark, output_path)
