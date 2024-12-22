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

    # top_rated_games = (
    #     reviews_table.groupBy("app_id")
    #     .agg({"votes_helpful": "sum"})
    #     .join(games_table, "app_id")
    #     .orderBy("sum(votes_helpful)", ascending=False)
    #     .limit(10)
    # )
    # print(top_rated_games)

    best_games = spark.sql(
        """
        SELECT g.app_name, SUM(CASE WHEN recommended='True' THEN 1 ELSE 0 END) AS counted
        FROM reviews r
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY counted DESC
        LIMIT 3
    """
    )

    print("Top 3 Rated Games:")
    best_games.show()

    top_res = spark.sql(
        """
        SELECT g.app_name, SUM(CASE WHEN recommended='True' THEN 1 ELSE 0 END) AS total_recommended
        FROM reviews r
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY total_recommended DESC
        LIMIT 10
    """
    )
    top_res.show()
    top_res.toPandas().to_csv(output_path + "Top rated Games.csv", header=True)

    most_res = spark.sql(
        """
        SELECT g.app_name, COUNT(r.review_id) AS counted
        FROM reviews r
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY counted DESC
        LIMIT 3
    """
    )
    most_res.show()
    most_res.toPandas().to_csv(
        output_path + "Most reviewed Games.csv",
        header=True,
    )

    most_user = spark.sql(
        """
        SELECT u.user_id, COUNT(r.review_id) AS reviews_written
        FROM reviews r
        JOIN users u ON r.user_id = u.user_id
        GROUP BY u.user_id
        ORDER BY reviews_written DESC
        LIMIT 10
    """
    )
    most_user.show()
    most_user.toPandas().to_csv(
        output_path + "Most reviewes users.csv",
        header=True,
    )

    print("Top games non-casual users:")
    non_cas = spark.sql(
        """
        SELECT g.app_name, SUM(CASE WHEN r.recommended='True' AND CAST(u.playtime_last_two_weeks AS FLOAT) > 10 THEN 1 ELSE 0 END) AS total_recommended
        FROM reviews r
        JOIN users u ON r.user_id = u.user_id
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY total_recommended DESC
        LIMIT 10
    """
    )
    non_cas.show()
    non_cas.toPandas().to_csv(
        output_path + "Most popular non casual gamers.csv",
        header=True,
    )

    casual = spark.sql(
        """
        SELECT g.app_name, SUM(CASE WHEN r.recommended='True' AND CAST(u.playtime_last_two_weeks AS FLOAT) < 10 THEN 1 ELSE 0 END) AS total_recommended
        FROM reviews r
        JOIN users u ON r.user_id = u.user_id
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY total_recommended DESC
        LIMIT 10
    """
    )
    casual.show()
    casual.toPandas().to_csv(
        output_path + "Most popular casual gamers.csv",
        header=True,
    )

    most_helpful = spark.sql(
        """
        SELECT r.translated_review, r.review, g.app_name, r.votes_helpful
        FROM reviews r
        JOIN games g ON r.app_id = g.app_id
        ORDER BY r.votes_helpful DESC
        LIMIT 10
    """
    )
    most_helpful.show()
    most_helpful.toPandas().to_csv(
        output_path + "Most helpful reviews.csv", header=True
    )
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

    generate_games_chart(
        title="Top 3 Rated Games - Podium",
        ylabel="Total Recommended Votes",
        output_path=output_path + "BestGames",
        games=best_games,
    )
    generate_games_chart(
        title="Top 3 Rated Games - Podium",
        ylabel="Total Reviewes",
        output_path=output_path + "MostReviedGames",
        games=most_res,
    )


def generate_games_chart(title, ylabel, output_path, games):
    directory = os.path.dirname(output_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Utworzono katalog: {directory}")

    game_names = games.select("app_name").rdd.flatMap(lambda x: x).collect()
    votes = games.select("counted").rdd.flatMap(lambda x: x).collect()

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

    ax.set_xticks(positions)
    ax.set_xticklabels(game_names)

    plt.savefig(output_path)
    plt.close()

    print(f"Wykres zapisany w: {output_path}")


aggregate_gold_layer()
