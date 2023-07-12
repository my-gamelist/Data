import pandas as pd

from db import Database


def compute(raw_games: list, columns: list):
    df = pd.DataFrame(raw_games, columns=columns)
    selected_columns = [
        'id', 'name', 'category',
        'cover', 'created_at', 'first_release_date',
        'slug', 'summary', 'updated_at', 'url', 'checksum'
    ]

    df = df[selected_columns]


    # # convert empty lists to -1
    # df = df.applymap(lambda x: -1 if isinstance(x, list) and len(x) == 0 else x)

    # convert dataframes to list of tuples
    games = [tuple(row) for row in df.values]
    return games

if __name__ == '__main__':
    db = Database()
    raw_games = db.get_all_raw_games()
    # columns = db.get_columns('raw_game')
    # convert from list of tuples to list of strings
    # columns = [column[0] for column in columns]
    fields = [
        "id",
        "name",
        "genres",
        "slug",
        "summary",
        "age_ratings",
        "artworks",
        "category",
        "cover",
        "created_at",
        "external_games",
        "first_release_date",
        "game_engines",
        "game_modes",
        "involved_companies",
        "multiplayer_modes",
        "platforms",
        "release_dates",
        "screenshots",
        "similar_games",
        "tags",
        "themes",
        "updated_at",
        "url",
        "websites",
        "checksum",
        "language_supports"
    ]

    games = compute(raw_games, fields)
    db.add_game(games)