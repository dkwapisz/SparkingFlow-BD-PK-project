import argparse
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import matplotlib.pyplot as plt
import os


def initialize_spark():
    return (
        SparkSession.builder.appName("Generate Best Games")
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.2.29.jre7.jar")
        .config(
            "spark.driver.extraClassPath",
            "/opt/airflow/jars/postgresql-42.2.29.jre7.jar",
        )
        .getOrCreate()
    )


def generate_games_chart(title, ylabel, output_path, games, selected_name):
    directory = os.path.dirname(output_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

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

    plt.savefig(output_path)
    plt.close()
