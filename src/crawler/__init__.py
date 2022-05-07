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

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(sys.path[0])))

from src.config import Setting 

class BaseCrawler():
    def __init__(self):
        pass

    def get_soup(self, path_url):
        try:
            full_url = Setting.BASE_URL + path_url
            headers = {'User-Agent': Setting.USER_AGENT}

            time.sleep(Setting.INTERVAL_DELAY)
            res = requests.get(full_url, headers)
            soup = BeautifulSoup(res.text, 'html.parser')
            return soup
        except Exception as e:
            raise e

    def crawl(self):
        pass

class BaseBulkCrawler(BaseCrawler):

    def __init__(self, url):
        self.soup = self.get_soup(url)

        self.urls = []

    def bulk_craw(self, CrawlerObject, stop_at:int=None, save_to_cache:bool=True):
        
        result_list = []
        # fail_ids = []
        success_urls = []
        retry_count = 0

        while len(success_urls) < len(self.urls):

            if retry_count > Setting.MAX_RETRY:
                break
            
            if retry_count > 0:
                print(f"Attempting retry number {retry_count}")

            for index, url in enumerate(self.urls):
                if url in success_urls:
                    continue

                try:
                    crawler = CrawlerObject(url)
                    result = self._single_crawl(crawler, index, True)
                    result_list.append(result)
                    success_urls.append(url)
                    print(url, index)
                except Exception as e:

                    print(url, repr(e))
                    # raise e  
                
                if type(stop_at) == int and stop_at is not None:
                    if index >= stop_at:
                        return result_list

            retry_count += 1

        return result_list

    def _single_crawl(self, crawler, index, status:bool=True):

        raise NotImplementedError("This method is not implemented.")

def write_to_cache(movie_list:list):

    def get_stars_url(top_cast, top_cast_url, stars):
        return [v for k,v in zip(top_cast, top_cast_url) if k in stars]

    df = pd.DataFrame.from_dict(movie_list)

    writers_df = pd.DataFrame({
        "name": [item for sublist in list(df['writers']) for item in sublist], 
        "url": [item for sublist in list(df['writers_url']) for item in sublist]
    })
    writers_df['url'] = writers_df['url'].str.replace("(?<=\?)(.*)|(\?)", "", regex=True)
    writers_df.drop_duplicates(inplace=True)
    # writers_df.set_index('url', inplace=True)

    directors_df = pd.DataFrame({
        "name": [item for sublist in list(df['directors']) for item in sublist], 
        "url": [item for sublist in list(df['directors_url']) for item in sublist]
    })
    directors_df['url'] = directors_df['url'].str.replace("(?<=\?)(.*)|(\?)", "", regex=True)
    directors_df.drop_duplicates(inplace=True)
    # directors_df.set_index('url', inplace=True)

    actors_df = pd.DataFrame({
        "name": [item for sublist in list(df['top_cast']) for item in sublist], 
        "url": [item for sublist in list(df['top_cast_url']) for item in sublist]
    })
    actors_df['url'] = actors_df['url'].str.replace("(?<=\?)(.*)|(\?)", "", regex=True)
    actors_df.drop_duplicates(inplace=True)
    # actors_df.set_index('url', inplace=True)

    df['stars_url'] = df.apply(lambda x: get_stars_url(x.top_cast, x.top_cast_url, x.stars), axis=1)

    top_cast_df = df[['id', 'top_cast_url']].explode(['top_cast_url'])
    top_cast_df.columns = ['movie_id', 'actor_url']
    stars_df = df[['id', 'stars_url']].explode(['stars_url'])
    stars_df.columns = ['movie_id', 'actor_url']
    stars_df['is_star'] = True
    actor_movie_df = top_cast_df.merge(stars_df, how='left', on=['movie_id', 'actor_url'])
    actor_movie_df['is_star'] = actor_movie_df['is_star'].fillna(False)
    director_movie_df = df[['id', 'directors_url']].explode(['directors_url'])
    writers_movie_df = df[['id', 'writers_url']].explode(['writers_url'])
    genres_movie_df = df[['id', 'genres']].explode(['genres'])
    country_movie_df = df[['id', 'country']].explode(['country'])
    language_movie_df = df[['id', 'language']].explode(['language'])

    df = df[['id','url','name','popularity','rating','rating_url','rating_count','user_review_count','critic_review_count','budget','revenue_usa','revenue_usa_opening','revenue_world','runtime','opening_date','release_date']]

    writers_df.to_csv('./cache/writers.csv', index=False)
    directors_df.to_csv('./cache/directors.csv', index=False)
    actors_df.to_csv('./cache/actors.csv', index=False)
    actor_movie_df.to_csv('./cache/actor_movie.csv', index=False)
    director_movie_df.to_csv('./cache/director_movie.csv', index=False)
    writers_movie_df.to_csv('./cache/writers_movie.csv', index=False)
    genres_movie_df.to_csv('./cache/genres_movie.csv', index=False)
    country_movie_df.to_csv('./cache/country_movie.csv', index=False)
    language_movie_df.to_csv('./cache/language_movie.csv', index=False)
    df.to_csv('./cache/movies.csv', index=False)