import argparse
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import matplotlib.pyplot as plt
import os

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
    .config("spark.executor.memory", "16g")
    .config("spark.driver.memory", "16g")
    .config("spark.memory.offHeap.enabled", True)
    .config("spark.memory.offHeap.size", "16g")
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


    generate_best_games()
    generate_most_reviewed_games()
    generate_most_reviews_by_user()
    generate_top_games_non_casual()
    generate_top_games_casual()
    generate_top_helpful_reviews()
    generate_top_games_publishers()

    # print("Reviews by Genre:")
    # spark.sql(
    #     """
    #     SELECT g.name AS genre_name, COUNT(r.review_id) AS review_count
    #     FROM reviews r
    #     JOIN games gm ON r.app_id = gm.app_id
    #     JOIN genres g ON gm.genre_id = g.genre_id
    #     GROUP BY g.name
    #     ORDER BY review_count DESC
    # """
    # ).show()

    # print("Reviews by Region:")
    # spark.sql(
    #     """
    #     SELECT rg.name AS region_name, COUNT(r.review_id) AS review_count
    #     FROM reviews r
    #     JOIN users u ON r.user_id = u.user_id
    #     JOIN regions rg ON u.region_id = rg.region_id
    #     GROUP BY rg.name
    #     ORDER BY review_count DESC
    # """
    # ).show()




def generate_best_games():
    best_games = spark.sql(
        """
        SELECT g.app_name, SUM(CASE WHEN recommended='True' THEN 1 ELSE 0 END) AS counted
        FROM reviews r
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY counted DESC
        LIMIT 10
    """
    )

    print("Top 10 Best Games:")
    best_games.toPandas().to_parquet(output_path + "Best Games.parquet")
    generate_games_chart(
        title="Top 3 Best Games - Podium",
        ylabel="Total Recommended Votes",
        output_path=output_path + "Best Games",
        games=best_games.limit(3),
        selected_name="app_name"
    )

def generate_most_reviewed_games():
    most_res = spark.sql(
        """
        SELECT g.app_name, COUNT(r.review_id) AS counted
        FROM reviews r
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY counted DESC
        LIMIT 10
    """
    )
    most_res.toPandas().to_parquet(
        output_path + "Most reviewed Games.parquet"
    )
    generate_games_chart(
        title="Top 3 Most Revied Games - Podium",
        ylabel="Total Reviewes",
        output_path=output_path + "Most reviewed Games",
        games=most_res.limit(3),
        selected_name="app_name"
    )

def generate_most_reviews_by_user():
    most_user = spark.sql(
        """
        SELECT u.user_id, COUNT(r.review_id) AS counted
        FROM reviews r
        JOIN users u ON r.user_id = u.user_id
        GROUP BY u.user_id
        ORDER BY counted DESC
        LIMIT 10
    """
    )
    most_user.toPandas().to_parquet(
        output_path + "Most reviewes by user.parquet"
    )
    generate_games_chart(
        title="Top 3 Most reviewes by User - Podium",
        ylabel="Most Reviews",
        output_path=output_path + "Most reviewes by user",
        games=most_user.limit(3),
        selected_name="user_id"
    )

def generate_top_games_non_casual():
    print("Top games non-casual users:")
    non_cas = spark.sql(
        """
        SELECT g.app_name, SUM(CASE WHEN r.recommended='True' AND CAST(u.playtime_last_two_weeks AS FLOAT) > 10 THEN 1 ELSE 0 END) AS counted
        FROM reviews r
        JOIN users u ON r.user_id = u.user_id
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY counted DESC
        LIMIT 10
    """
    )
    non_cas.toPandas().to_parquet(
        output_path + "Most popular non casual gamers.parquet"
    )
    generate_games_chart(
        title="Top 3 Most popular non casual gamers - Podium",
        ylabel="Most non casual gamers",
        output_path=output_path + "Most popular non casual gamers",
        games=non_cas.limit(3),
        selected_name="app_name"
    )

def generate_top_games_casual():
    print("Top games casual users:")
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
    casual.toPandas().to_parquet(
        output_path + "Most popular casual gamers.parquet"
    )
    generate_games_chart(
        title="Top 3 Most popular casual gamers - Podium",
        ylabel="Most casual gamers",
        output_path=output_path + "Most popular casual gamers",
        games=casual.limit(3),
        selected_name="app_name"
    )

def generate_top_helpful_reviews():
    print("Top games casual users:")
    most_helpful = spark.sql(
        """
        SELECT r.translated_review, r.review AS review, g.app_name, r.votes_helpful AS counted
        FROM reviews r
        JOIN games g ON r.app_id = g.app_id
        ORDER BY counted DESC
        LIMIT 10
    """
    )
    most_helpful.toPandas().to_parquet(
        output_path + "Most helpful reviews.parquet"
    )
    # generate_games_chart(
    #     title="Top 3 Most helpful reviews - Podium",
    #     ylabel="Most helpful reviews",
    #     output_path=output_path + "Most helpful reviews",
    #     games=most_helpful.limit(3),
    #     selected_name="review"
    # )

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

def generate_games_chart(title, ylabel, output_path, games, selected_name):
    directory = os.path.dirname(output_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Utworzono katalog: {directory}")

    game_names = games.select(selected_name).rdd.flatMap(lambda x: x).collect()
    votes = games.select("counted").rdd.flatMap(lambda x: x).collect()
    votes = [float(vote) for vote in votes]
    positions = [2, 1, 3]
    colors = ["#FFD700", "#C0C0C0", "#CD7F32"]

    _, ax = plt.subplots(figsize=(8, 6))

    bars = ax.bar(positions, votes, color=colors, tick_label=game_names)

    for bar in bars:
        yval = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2, yval, int(yval), ha="center", va="bottom"
        )

    ax.set_xlabel("Game Position")
    ax.set_ylabel(ylabel)
    ax.set_title(title)

    min_vote = min(votes)
    max_vote = max(votes)
    lower_limit = min_vote * 0.75
    upper_limit = max_vote * 1.25
    ax.set_ylim(lower_limit, upper_limit)


    ax.set_xticks(positions)
    ax.set_xticklabels(game_names)

    plt.savefig(output_path)
    plt.close()

    print(f"Wykres zapisany w: {output_path}")


aggregate_gold_layer()
