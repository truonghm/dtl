import pandas as pd
import numpy as np
import datetime


def transform_person(df):
    def get_stars_url(top_cast, top_cast_url, stars):
        return [v for k, v in zip(top_cast, top_cast_url) if k in stars]

    df["stars_url"] = df.apply(
        lambda x: get_stars_url(x.top_cast, x.top_cast_url, x.stars), axis=1
    )

    writers_df = pd.DataFrame(
        {
            "name": [item for sublist in list(df["writers"]) for item in sublist],
            "url": [item for sublist in list(df["writers_url"]) for item in sublist],
        }
    )
    writers_df["url"] = writers_df["url"].str.replace(
        "(?<=\?)(.*)|(\?)", "", regex=True
    )
    writers_df.drop_duplicates(inplace=True)
    # writers_df.set_index('url', inplace=True)

    directors_df = pd.DataFrame(
        {
            "name": [item for sublist in list(df["directors"]) for item in sublist],
            "url": [item for sublist in list(df["directors_url"]) for item in sublist],
        }
    )
    directors_df["url"] = directors_df["url"].str.replace(
        "(?<=\?)(.*)|(\?)", "", regex=True
    )
    directors_df.drop_duplicates(inplace=True)
    # directors_df.set_index('url', inplace=True)

    actors_df = pd.DataFrame(
        {
            "name": [item for sublist in list(df["top_cast"]) for item in sublist],
            "url": [item for sublist in list(df["top_cast_url"]) for item in sublist],
        }
    )
    actors_df["url"] = actors_df["url"].str.replace("(?<=\?)(.*)|(\?)", "", regex=True)
    actors_df.drop_duplicates(inplace=True)

    stars_df = pd.DataFrame(
        {
            "name": [item for sublist in list(df["stars"]) for item in sublist],
            "url": [item for sublist in list(df["stars_url"]) for item in sublist],
        }
    )
    stars_df["url"] = stars_df["url"].str.replace("(?<=\?)(.*)|(\?)", "", regex=True)
    stars_df.drop_duplicates(inplace=True)

    # actors_df.set_index('url', inplace=True)

    return directors_df, actors_df, writers_df, stars_df


def transform_lookup(df):

    top_cast_df = df[["id", "top_cast_url"]].explode(["top_cast_url"])
    top_cast_df.columns = ["movie_id", "actor_url"]
    stars_df = df[["id", "stars_url"]].explode(["stars_url"])
    stars_df.columns = ["movie_id", "actor_url"]
    stars_df["is_star"] = True
    actor_movie_df = top_cast_df.merge(
        stars_df, how="left", on=["movie_id", "actor_url"]
    )
    actor_movie_df["is_star"] = actor_movie_df["is_star"].fillna(False)
    director_movie_df = df[["id", "directors_url"]].explode(["directors_url"])
    writers_movie_df = df[["id", "writers_url"]].explode(["writers_url"])
    genres_movie_df = df[["id", "genres"]].explode(["genres"])
    country_movie_df = df[["id", "country"]].explode(["country"])
    language_movie_df = df[["id", "language"]].explode(["language"])

    return (
        actor_movie_df,
        director_movie_df,
        writers_movie_df,
        genres_movie_df,
        country_movie_df,
        language_movie_df,
    )


def transform_movies(df):
    return df[
        [
            "id",
            "url",
            "name",
            "popularity",
            "rating",
            "rating_url",
            "rating_count",
            "user_review_count",
            "critic_review_count",
            "budget",
            "revenue_usa",
            "revenue_usa_opening",
            "revenue_world",
            "runtime",
            "opening_date",
            "release_date",
        ]
    ]
