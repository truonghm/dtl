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

from . import BaseCrawler, BaseBulkCrawler
from src.config import Setting

class RatingCrawler(BaseCrawler):
    def __init__(self, url):
        self.soup = self.get_soup(url)
        self.all_tables = self.soup.select("table")

    def get_dist_table(self):
        table = self.soup.select('table')[0]
        rows = table.find_all('tr')

        col0 = []
        col1 = []
        for i in range(1, len(rows)):
            rating_label = [int(re.sub("(\\n)|(\\xa0)|(\s)|(,)", "", r.text)) for r in rows[i].select("div.rightAligned")]
            rating_count = [int(re.sub("(\\n)|(\\xa0)|(\s)|(,)", "", r.text)) for r in rows[i].select("div.leftAligned")]
            col0.extend(rating_label)
            col1.extend(rating_count)

        df = pd.DataFrame({
            "rating":col0,
            "vote_count": col1
        })
        df['dist2'] = df['vote_count']/sum(df['vote_count'])

        return df

    def get_demographic_table(self):
        table = self.soup.select('table')[1]
        rows = table.find_all('tr')
        demographics = [h.text for h in rows[0].select("div.tableHeadings")]

        col0 = []
        col1 = []
        col2 = []
        col3 = []

        for i in range(1, len(rows)):
            group = [r.text for r in rows[i].select("div.leftAligned")]
            rating_label = [float(re.sub("(\\n)|(\\xa0)|(\s)|(,)", "", r.text)) for r in rows[i].select("div.bigcell")]
            rating_count = [int(re.sub("(\\n)|(\\xa0)|(\s)|(,)", "", r.text)) for r in rows[i].select("div.smallcell")]
            col0.extend(group * len(demographics))
            col1.extend(demographics)
            col2.extend(rating_label)
            col3.extend(rating_count)

        df = pd.DataFrame({
            "group": col0,
            "demographic":col1,
            "rating": col2,
            "voting_count": col3
        })

        return df

    def crawl(self):
        rating_dist = self.get_dist_table()
        rating_demo = self.get_demographic_table()

        return rating_dist, rating_demo

class BulkRatingCrawler(BaseBulkCrawler):

    def __init__(self, url:str=None, load_from_cache:bool=True):
        # self.soup = self.get_soup(url)

        if load_from_cache:
            data = pd.read_csv('./cache/movies.csv', usecols=['id','rating_url'])
        else:
            raise NotImplementedError

        self.urls = list(data['rating_url'])
        # print(self.urls)

    def _single_crawl(self, crawler, index, status:bool=True):
        
        rating_dist, rating_demo = crawler.crawl()

        rating_dist['url'] = self.urls[index]
        rating_dist['movie_id'] = index
        rating_demo['url'] = self.urls[index]
        rating_demo['movie_id'] = index

        return rating_dist, rating_demo