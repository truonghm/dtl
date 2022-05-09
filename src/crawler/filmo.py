import urllib3
from bs4 import BeautifulSoup
import requests
import json
import re
import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
import time

from . import BaseCrawler, BaseBulkCrawler, Setting
from .movie import BulkMovieCrawler, MovieCrawler


class BaseFilmoCrawler(BaseCrawler):
    def __init__(self, url: str):
        # raise NotImplementedError
        self.url = url
        self.soup = self.get_soup(url)

    def crawl(self, write_to_cache: bool = True, file_name: str = None):
        filmo = []
        for movie in self.soup.select("div.lister-item-content"):
            row = dict()
            try:
                name = movie.select_one("a").text.strip()
                row["name"] = name
            except AttributeError:
                row["name"] = None

            try:
                year = movie.select_one(
                    "span.lister-item-year.text-muted.unbold"
                ).text.strip()
                row["year"] = re.sub("(\()|(\))|(\|)", "", year)
            except AttributeError:
                row["year"] = None

            try:
                cert = movie.select_one("span.certificate").text.strip()
                row["cert"] = cert
            except AttributeError:
                row["cert"] = None

            try:
                runtime = movie.select_one("span.runtime").text.strip()
                row["runtime"] = int(runtime.replace(" min", ""))
            except AttributeError:
                row["runtime"] = None

            try:
                genres = movie.select_one("span.genre").text.strip()
                row["genres"] = genres
            except AttributeError:
                row["genres"] = None

            try:
                rating = movie.select_one(
                    "div.inline-block.ratings-imdb-rating > strong"
                ).text.strip()
                row["rating"] = float(rating)
            except AttributeError:
                row["rating"] = None
            try:
                label = movie.select("p.text-muted.text-small")

                for l in label:
                    if "Director" in l.text:
                        small_text = [
                            re.sub("(\\n)", "", t).strip().split(":")[1].split(",")
                            for t in l.text.split("| ")
                        ]
                        break
                try:
                    row["directors"] = small_text[0]
                except:
                    row["directors"] = None
                try:
                    row["stars"] = small_text[1]
                except:
                    row["stars"] = None
                # print(small_text)
            except AttributeError:
                row["director"] = None
                row["stars"] = None

            try:
                vote_count = [nv.attrs.get("data-value", None) for nv in movie.find_all("span", {"name": "nv"})][0]
                row["rating_count"] = int(vote_count.replace(",", ""))
            except:
                row["rating_count"] = None

            try:
                revenue_world = [nv.attrs.get("data-value", None) for nv in movie.find_all("span", {"name": "nv"})][1]
                # row["revenue"] = float(re.sub("(\$)|(.)|(M)", "", revenue)) * 10e6
                row["revenue_world"] = int(revenue_world.replace(",", ""))
            except Exception as e:
                # print(repr(e))
                row["revenue_world"] = None

            row["url"] = self.url

            filmo.append(row)
            # print(row)
            # break

        df = pd.DataFrame(filmo)

        if write_to_cache:
            self.save_cache(df, file_name)

        return df


class StarFilmoCrawler(BaseFilmoCrawler):
    def __init__(self, url: str):
        self.url = url
        self.job_type = "actor"

        url = re.sub("(name)|(\/)", "", url)
        full_url = f"filmosearch/?explore=title_type&role={url}&ref_=filmo_ref_job_typ&sort=num_votes,desc&mode=detail&page=1&job_type={self.job_type}&title_type=movie"

        self.soup = self.get_soup(full_url)


class WriterFilmoCrawler(BaseFilmoCrawler):
    def __init__(self, url: str):
        self.url = url
        self.job_type = "writer"

        url = re.sub("(name)|(\/)", "", url)
        full_url = f"filmosearch/?explore=title_type&role={url}&ref_=filmo_ref_job_typ&sort=num_votes,desc&mode=detail&page=1&job_type={self.job_type}&title_type=movie"

        self.soup = self.get_soup(full_url)


class DirectorFilmoCrawler(BaseFilmoCrawler):
    def __init__(self, url: str):
        self.url = url
        self.job_type = "director"

        url = re.sub("(name)|(\/)", "", url)
        full_url = f"filmosearch/?explore=title_type&role={url}&ref_=filmo_ref_job_typ&sort=num_votes,desc&mode=detail&page=1&job_type={self.job_type}&title_type=movie"

        self.soup = self.get_soup(full_url)


class BulkFilmoCrawler(BaseBulkCrawler):
    def __init__(self, job_type, load_from_cache: bool = True, stop_at: int = None):
        # self.soup = self.get_soup(url)
        self.stop_at = stop_at

        if job_type == "actor":
            cache_path = Setting.STARS_CACHE
        elif job_type == "director":
            cache_path = Setting.DIRECTORS_CACHE
        elif job_type == "writer":
            cache_path = Setting.WRITERS_CACHE
        else:
            raise ValueError("Invalid input for job_type")

        self.job_type = job_type

        def do_load_from_cache(download_first: bool = True):
            if download_first:
                crawler = BulkMovieCrawler(load_from_cache=True, stop_at=stop_at)
                res = crawler.bulk_craw(
                    MovieCrawler, write_to_cache=True, file_name=Setting.MOVIE_CACHE
                )

            cache = pd.read_csv(cache_path, usecols=["url"])
            self.urls = list(cache["url"])

        if load_from_cache:
            try:
                do_load_from_cache(download_first=False)
            except FileNotFoundError:
                do_load_from_cache(download_first=True)
        else:
            do_load_from_cache(download_first=True)

    def crawl(self, crawler, index=None, status: bool = True):

        df = crawler.crawl(write_to_cache=False)
        return df
