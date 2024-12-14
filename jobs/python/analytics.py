from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

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

    top_rated_games = (
        reviews_table.groupBy("app_id")
        .agg({"votes_helpful": "sum"})
        .join(games_table, "app_id")
        .orderBy("sum(votes_helpful)", ascending=False)
        .limit(10)
    )
    print(top_rated_games)

    print("Top-rated Games:")
    spark.sql(
        """
        SELECT g.app_name, SUM(r.votes_helpful) AS total_helpful_votes
        FROM reviews r
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY total_helpful_votes DESC
        LIMIT 10
    """
    ).show()

    print("Most Reviewed Games:")
    spark.sql(
        """
        SELECT g.app_name, COUNT(r.review_id) AS review_count
        FROM reviews r
        JOIN games g ON r.app_id = g.app_id
        GROUP BY g.app_name
        ORDER BY review_count DESC
        LIMIT 10
    """
    ).show()

    # print("Most Active Users:")
    # spark.sql(
    #     """
    #     SELECT u.user_id, COUNT(r.review_id) AS reviews_written
    #     FROM reviews r
    #     JOIN users u ON r.user_id = u.user_id
    #     GROUP BY u.user_id
    #     ORDER BY reviews_written DESC
    #     LIMIT 10
    # """
    # ).show()

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


aggregate_gold_layer()
