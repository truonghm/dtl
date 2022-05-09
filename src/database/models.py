import datetime
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    DateTime,
    UnicodeText,
    Float,
    Date,
)
from sqlalchemy import UniqueConstraint, Sequence
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from . import Base, engine, SessionLocal


class MoviesTable(Base):
    __tablename__ = "movies"
    movie_rank = Column(Integer, default=None, unique=True)
    movie_id = Column(String(128), primary_key=True)
    name = Column(String(128), default=None)
    popularity = Column(Integer, default=None)
    rating = Column(Float, default=None)
    rating_count = Column(Integer, default=None)
    user_review_count = Column(Integer, default=None)
    critic_review_count = Column(Integer, default=None)
    budget = Column(Float, default=None)
    revenue_usa = Column(Float, default=None)
    revenue_usa_opening = Column(Float, default=None)
    revenue_world = Column(Float, default=None)
    runtime = Column(Integer, default=None)
    opening_date = Column(Date, default=None)
    release_date = Column(Date, default=None)


class ActorsTable(Base):
    __tablename__ = "actors"
    name = Column(String(128), nullable=False)
    actor_id = Column(String(128), primary_key=True)


class DirectorsTable(Base):
    __tablename__ = "directors"
    name = Column(String(128), nullable=False)
    director_id = Column(String(128), primary_key=True)


class WritersTable(Base):
    __tablename__ = "writers"
    name = Column(String(128), nullable=False)
    writer_id = Column(String(128), primary_key=True)


class ActorMovieTable(Base):
    __tablename__ = "actor_movie"
    uid = Column(Integer, primary_key=True)
    movie_id = Column(String(128), ForeignKey("movies.movie_id"), nullable=False)
    actor_url = Column(String(128), ForeignKey("actors.actor_id"), nullable=False)
    is_star = Column(Boolean, default=False)


class DirectorMovieTable(Base):
    __tablename__ = "director_movie"
    uid = Column(Integer, primary_key=True)
    movie_id = Column(String(128), ForeignKey("movies.movie_id"), nullable=False)
    director_url = Column(
        String(128),
        ForeignKey("directors.director_id"),
        nullable=False,
        primary_key=True,
    )


class WriterMovieTable(Base):
    __tablename__ = "writer_movie"
    uid = Column(Integer, primary_key=True)
    movie_id = Column(String(128), ForeignKey("movies.movie_id"), nullable=False)
    writer_url = Column(String(128), ForeignKey("writers.writer_id"), nullable=False)


class GenreMovieTable(Base):
    __tablename__ = "genre_movie"
    uid = Column(Integer, primary_key=True)
    movie_id = Column(String(128), ForeignKey("movies.movie_id"), nullable=False)
    genre = Column(String(128), nullable=False)


class CountryMovieTable(Base):
    __tablename__ = "country_movie"
    uid = Column(Integer, primary_key=True)
    movie_id = Column(String(128), ForeignKey("movies.movie_id"), nullable=False)
    country = Column(String(128), nullable=False)


class LanguageMovieTable(Base):
    __tablename__ = "language_movie"
    uid = Column(Integer, primary_key=True)
    movie_id = Column(String(128), ForeignKey("movies.movie_id"), nullable=False)
    language = Column(String(128), nullable=False)


class RatingDistTable(Base):
    __tablename__ = "rating_dist"
    uid = Column(Integer, primary_key=True)
    rating = Column(Integer, nullable=False)
    vote_count = Column(Integer, nullable=False)
    movie_id = Column(String(128), ForeignKey("movies.movie_id"), nullable=False)


class RatingDemoTable(Base):
    __tablename__ = "rating_demo"
    uid = Column(Integer, primary_key=True)
    demo_group = Column(String(128), nullable=False)
    demo = Column(String(128), nullable=False)
    weighted_avg_rating = Column(Float, nullable=False)
    vote_count = Column(Integer, nullable=False)
    movie_id = Column(String(128), ForeignKey("movies.movie_id"), nullable=False)


class ActorFilmo(Base):
    __tablename__ = "actor_filmo"
    movie_id = Column(Integer, primary_key=True)
    name = Column(String(128), default=None)
    year = Column(Integer, default=None)
    cert = Column(String(128), default=None)
    runtime = Column(Integer, default=None)
    genres = Column(String(128), default=None)
    rating = Column(Float, default=None)
    rating_count = Column(Integer, default=None)
    revenue_world = Column(Integer, default=None)
    crew_url = Column(String(128), ForeignKey("actors.actor_id"), nullable=False)
    is_star = Column(Boolean, nullable=False, default=False)


class DirectorFilmo(Base):
    __tablename__ = "director_filmo"
    movie_id = Column(Integer, primary_key=True)
    name = Column(String(128), default=None)
    year = Column(Integer, default=None)
    cert = Column(String(128), default=None)
    runtime = Column(Integer, default=None)
    genres = Column(String(128), default=None)
    rating = Column(Float, default=None)
    rating_count = Column(Integer, default=None)
    revenue_world = Column(Integer, default=None)
    crew_url = Column(String(128), ForeignKey("directors.director_id"), nullable=False)


class WriterFilmo(Base):
    __tablename__ = "writer_filmo"
    movie_id = Column(Integer, primary_key=True)
    name = Column(String(128), default=None)
    year = Column(Integer, default=None)
    cert = Column(String(128), default=None)
    runtime = Column(Integer, default=None)
    genres = Column(String(128), default=None)
    rating = Column(Float, default=None)
    rating_count = Column(Integer, default=None)
    revenue_world = Column(Integer, default=None)
    crew_url = Column(String(128), ForeignKey("writers.writer_id"), nullable=False)
