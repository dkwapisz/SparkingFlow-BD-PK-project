import os

import matplotlib.pyplot as plt


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
