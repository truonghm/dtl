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


class RatingCrawler(BaseCrawler):
    def __init__(self, url):
        self.url = url
        print(url)
        self.soup = self.get_soup(url)
        self.all_tables = self.soup.select("table")

    def get_dist_table(self):
        table = self.soup.select("table")[0]
        rows = table.find_all("tr")

        col0 = []
        col1 = []
        for i in range(1, len(rows)):
            rating_label = [
                int(re.sub("(\\n)|(\\xa0)|(\s)|(,)", "", r.text))
                for r in rows[i].select("div.rightAligned")
            ]
            rating_count = [
                int(re.sub("(\\n)|(\\xa0)|(\s)|(,)", "", r.text))
                for r in rows[i].select("div.leftAligned")
            ]
            col0.extend(rating_label)
            col1.extend(rating_count)

        df = pd.DataFrame({"rating": col0, "vote_count": col1})
        # df['dist2'] = df['vote_count']/sum(df['vote_count'])
        df["url"] = self.url

        return df

    def get_demographic_table(self):
        table = self.soup.select("table")[1]
        rows = table.find_all("tr")
        demographics = [h.text for h in rows[0].select("div.tableHeadings")]

        col0 = []
        col1 = []
        col2 = []
        col3 = []

        for i in range(1, len(rows)):
            group = [r.text for r in rows[i].select("div.leftAligned")]
            rating_label = [
                float(re.sub("(\\n)|(\\xa0)|(\s)|(,)", "", r.text))
                for r in rows[i].select("div.bigcell")
            ]
            rating_count = [
                int(re.sub("(\\n)|(\\xa0)|(\s)|(,)", "", r.text))
                for r in rows[i].select("div.smallcell")
            ]
            col0.extend(group * len(demographics))
            col1.extend(demographics)
            col2.extend(rating_label)
            col3.extend(rating_count)

        df = pd.DataFrame(
            {"group": col0, "demographic": col1, "rating": col2, "voting_count": col3}
        )
        df["url"] = self.url

        return df

    def crawl(self):
        rating_dist = self.get_dist_table()
        rating_demo = self.get_demographic_table()

        return rating_dist, rating_demo


class BulkRatingCrawler(BaseBulkCrawler):
    def __init__(self, load_from_cache: bool = True, stop_at: int = None):
        # self.soup = self.get_soup(url)
        self.stop_at = stop_at

        def do_load_from_cache(download_first: bool = True):
            if download_first:
                crawler = BulkMovieCrawler(load_from_cache=True, stop_at=stop_at)
                res = crawler.bulk_craw(
                    MovieCrawler, write_to_cache=True, file_name=Setting.MOVIE_CACHE
                )

            cache = pd.read_csv(
                Setting.CACHE + Setting.MOVIE_CACHE + ".csv",
                usecols=["url"],
            )
            self.urls = [url + "ratings" for url in cache["url"]]

        if load_from_cache:
            try:
                do_load_from_cache(download_first=False)
            except:
                do_load_from_cache(download_first=True)
        else:
            do_load_from_cache(download_first=True)

        # print(self.urls)

    def crawl(self, crawler, index=None, status: bool = True):

        rating_dist, rating_demo = crawler.crawl()
        return rating_dist, rating_demo
