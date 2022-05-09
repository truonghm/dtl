from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(sys.path[0])))

from src.config import Setting

db_path = "sqlite:///" + Setting.DATABASE
graph_path = "./src/database/db_diagram.png"
all_tables = ["movies", "rating_dist","rating_demo", "directors", "actors", "writers", "director_movie", "actor_movie", "writer_movie", "genre_movie", "country_movie", "language_movie"]

engine = create_engine(db_path, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

Base.metadata.create_all(engine)

def bulk_drop(table_list):
    with engine.connect() as conn:
        for tb_name in table_list:
            conn.execute(str(f"DROP TABLE IF EXISTS [{tb_name}]"))

    
def generate_diagram():
    graph = create_schema_graph(
        metadata=MetaData(db_path),
        show_datatypes=True,
        )
    graph.write_png(graph_path)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class BaseDatabase(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def select(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def truncate_table(self):
        pass

    @abstractmethod
    def mapping_dtype(self):
        pass

    @abstractmethod
    def bulk_insert(self):
        pass

    @abstractmethod
    def insert(self):
        pass
