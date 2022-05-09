# from src.db import Database
from src.database import Base, engine, generate_diagram, bulk_drop, all_tables
from src.database.models import *
# from src.database.executor import SQLiteDatabase
from src.transform.db_input import *
from src.config import Setting
# import pandas as pd

# db = SQLiteDatabase()

if __name__ == "__main__":
    # df = pd.read_csv("./cache/movies.csv")
    # df['release_date'] = pd.to_datetime(df['release_date'], format="%Y-%m-%d")
    # df['opening_date'] = pd.to_datetime(df['opening_date'], format="%Y-%m-%d")

    # db = SQLiteDatabase()
    # db.bulk_insert(df, 'F_MOVIES', fill_nan=True, if_exists='replace')
    bulk_drop(all_tables)
    
    Base.metadata.create_all(engine)
    generate_diagram()

    df = load_raw_data("./cache/movies.csv")
    rating_dist = load_raw_data('./cache/rating_dist.csv')
    rating_dist.reset_index(drop=True, inplace=True)
    rating_dist.reset_index(drop=False, inplace=True)
    rating_demo = load_raw_data('./cache/rating_demo.csv')
    rating_demo.reset_index(drop=True, inplace=True)
    rating_demo.reset_index(drop=False, inplace=True)
    writers = transform_writers(df)
    directors = transform_directors(df)
    actors = transform_actors(df)
    stars = transform_stars(df)
    actor_movie = transform_actors_lookup(df)
    director_movie = transform_directors_lookup(df)
    writer_movie = transform_writers_lookup(df)
    genre_movie = transform_genres_lookup(df)
    country_movie = transform_country_lookup(df)
    language_movie = transform_language_lookup(df)
    movies_final = transform_movies(df)
    actor_filmo = load_raw_data('./cache/actor_filmo.csv')
    director_filmo = load_raw_data('./cache/director_filmo.csv')
    writer_filmo = load_raw_data('./cache/writer_filmo.csv')

    actor_filmo, director_filmo, writer_filmo = transform_filmo(actor_filmo, director_filmo, writer_filmo, actors)

    data = {
        'movies':movies_final,
        'rating_dist':rating_dist,
        'rating_demo':rating_demo,
        'writers':writers,
        'directors':directors,
        'actors':actors,
        'stars':stars,
        'actor_movie':actor_movie,
        'director_movie':director_movie,
        'writer_movie':writer_movie,
        'genre_movie':genre_movie,
        'country_movie':country_movie,
        'language_movie':language_movie,
        'movies_final':movies_final,
        'actor_filmo':actor_filmo,
        'director_filmo':director_filmo,
        'writer_filmo':writer_filmo,
    }

    fill_db(**data)