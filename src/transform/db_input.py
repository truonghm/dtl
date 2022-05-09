import pandas as pd
import numpy as np
import datetime
import ast
import re
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(sys.path[0])))

from src.database.executor import SQLiteDatabase


def load_raw_data(file_path: str = None):
    df = pd.read_csv(file_path)
    df['url'] = df['url'].str.replace("(?<=\?)(.*)|(\?)|(title)|(ratings)|(name)|(\/)", "", regex=True)
    # df.rename(columns={'id':'rank'}, inplace=True)

    list_cols = (
        "director",
        "directors",
        "stars",
        "star",
        "top_cast",
        "writers",
        "writer",
        "top_cast_url",
        "director_url",
        "directors_url",
        "writer_url",
        "writers_url",
        "stars_url",
        "star_url",
    )

    url_cols = (        
        "top_cast_url",
        "director_url",
        "directors_url",
        "writer_url",
        "writers_url",
        "stars_url",
        "star_url",
    )
    for col in df.columns:
        if col in list_cols:
            try:
                df[col] = df[col].apply(ast.literal_eval)
            except ValueError:
                df[col] = df[col]
                
        if col in url_cols:
            new_col = []
            for row in df[col]:
                new_col.append([re.sub("(?<=\?)(.*)|(\?)|(name)|(\/)", "", s) for s in row])

            df[col] = new_col
    return df


def _get_stars_url(top_cast, top_cast_url, stars):
    return [v for k, v in zip(top_cast, top_cast_url) if k in stars]


def transform_writers(df):
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

    return writers_df


def transform_directors(df):
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

    return directors_df


def transform_actors(df):
    actors_df = pd.DataFrame(
        {
            "name": [item for sublist in list(df["top_cast"]) for item in sublist],
            "url": [item for sublist in list(df["top_cast_url"]) for item in sublist],
        }
    )
    actors_df["url"] = actors_df["url"].str.replace("(?<=\?)(.*)|(\?)", "", regex=True)
    actors_df.drop_duplicates(inplace=True)

    return actors_df


def transform_stars(df):

    df["stars_url"] = df.apply(
        lambda x: _get_stars_url(x.top_cast, x.top_cast_url, x.stars), axis=1
    )

    stars_df = pd.DataFrame(
        {
            "name": [item for sublist in list(df["stars"]) for item in sublist],
            "url": [item for sublist in list(df["stars_url"]) for item in sublist],
        }
    )
    stars_df["url"] = stars_df["url"].str.replace("(?<=\?)(.*)|(\?)", "", regex=True)
    stars_df.drop_duplicates(inplace=True)

    return stars_df


def transform_actors_lookup(df):

    top_cast_df = df[["url", "top_cast_url"]].explode(["top_cast_url"])
    top_cast_df.columns = ["movie_url", "actor_url"]
    stars_df = df[["url", "stars_url"]].explode(["stars_url"])
    stars_df.columns = ["movie_url", "actor_url"]
    stars_df["is_star"] = True
    actor_movie_df = top_cast_df.merge(
        stars_df, how="left", on=["movie_url", "actor_url"]
    )
    actor_movie_df["is_star"] = actor_movie_df["is_star"].fillna(False)
    actor_movie_df.reset_index(drop=True, inplace=True)
    actor_movie_df.reset_index(drop=False, inplace=True)

    return actor_movie_df


def transform_directors_lookup(df):
    director_movie_df = df[["url", "directors_url"]].explode(["directors_url"])
    director_movie_df.columns = ["movie_url", "director_url"]
    director_movie_df.reset_index(drop=True, inplace=True)
    director_movie_df.reset_index(drop=False, inplace=True)
    return director_movie_df


def transform_writers_lookup(df):
    writers_movie_df = df[["url", "writers_url"]].explode(["writers_url"])
    writers_movie_df.columns = ["movie_url", "writer_url"]
    writers_movie_df.reset_index(drop=True, inplace=True)
    writers_movie_df.reset_index(drop=False, inplace=True)
    return writers_movie_df


def transform_genres_lookup(df):
    genres_movie_df = df[["url", "genres"]].explode(["genres"])
    genres_movie_df.columns = ["movie_url", "genre"]
    genres_movie_df.reset_index(drop=True, inplace=True)
    genres_movie_df.reset_index(drop=False, inplace=True)
    return genres_movie_df

def transform_country_lookup(df):
    country_movie_df = df[["url", "country"]].explode(["country"])
    country_movie_df.columns = ["movie_url", "country"]
    country_movie_df.reset_index(drop=True, inplace=True)
    country_movie_df.reset_index(drop=False, inplace=True)
    return country_movie_df


def transform_language_lookup(df):
    language_movie_df = df[["url", "language"]].explode(["language"])
    language_movie_df.columns = ["movie_url", "language"]
    language_movie_df.reset_index(drop=True, inplace=True)
    language_movie_df.reset_index(drop=False, inplace=True)
    return language_movie_df


def transform_movies(df):

    return df[
        [
            "rank",
            "url",
            "name",
            "popularity",
            "rating",
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

def _check_role(person:str, role:list):
    if person.strip() in [star.strip() for star in role]:
        return True
    else:
        return False

def transform_filmo(actor_filmo, director_filmo, writer_filmo, actors):

    actor_filmo['actor_name'] = pd.merge(actor_filmo[['url']], actors, how='left', on = 'url')['name']

    actor_filmo['is_star'] = actor_filmo.apply(lambda x: _check_role(x.actor_name, x.stars), axis=1)
    # director_filmo['is_star'] = False
    # writer_filmo['is_star'] = False

    cols = ['name','year','cert','runtime','genres','rating','rating_count','revenue_world','url']

    actor_filmo = actor_filmo[cols + ['is_star']]
    actor_filmo.reset_index(drop=True, inplace=True)
    actor_filmo.reset_index(drop=False, inplace=True)
    director_filmo = director_filmo[cols]
    director_filmo.reset_index(drop=True, inplace=True)
    director_filmo.reset_index(drop=False, inplace=True)
    writer_filmo = writer_filmo[cols]
    writer_filmo.reset_index(drop=True, inplace=True)
    writer_filmo.reset_index(drop=False, inplace=True)
    
    return actor_filmo, director_filmo, writer_filmo

def fill_db(**dataframes):
    db = SQLiteDatabase()
    for table_name, df in dataframes.items():
        print(table_name)
        db.bulk_insert(df=df, table_name=table_name, if_exists="append", fill_nan=True, convert_to_str=True)

    db.close()