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
# from src.config import Setting

class MovieCrawler(BaseCrawler):
    
    def __init__(self, path_url):

        self.movie = dict(
            name = None,
            popularity = None,
            rating = None,
            rating_url = None,
            rating_count = None,
            user_review_count = None,
            critic_review_count = None,
            # metascore = None,
            directors = [],
            directors_url = [],
            writers = [],
            writers_url = [],
            top_cast = [],
            top_cast_url = [],
            stars = [],
            genres = [],
            country = [],
            language = [],
            budget = None,
            revenue_usa = None,
            revenue_usa_opening = None,
            revenue_world = None,
            runtime = None,
            opening_date = None,
            release_date = None,
        )

        try:
            self.soup = self.get_soup(path_url=path_url)
        except Exception as e:
            self.soup = None
            print(repr(e))

    def extract_movie_info(self, c, label_http, url_http):
        if label_http:
            label = label_http[0].text
            all_keys = self.mapping_key(label)

            if len(all_keys) == 0:
                return
            elif len(all_keys) == 1:
                if  not (self.movie[all_keys[0]] is None or len(self.movie[all_keys[0]]) == 0):
                    return

            else:
                check = any([self.movie[ak] is None or len(self.movie[ak]) == 0 for ak in all_keys])
                if not check:
                    return
            
            if label in ("Director", "Directors", "Writer", "Writers"):
                url_key, url_val = self.extract_crew_url(label, url_http)
                # print(url_key, url_val)
                if url_key in ('directors_url','writers_url'):
                    if len(self.movie[url_key]) == 0:
                        self.movie[url_key] = url_val

            # content = c.text.replace(label, "")
            http = c.select("a.ipc-metadata-list-item__list-content-item.ipc-metadata-list-item__list-content-item--link")
            inner_content = [s.text for s in http]
            # print(label, inner_content)
            key, value = self.extract_list(label, inner_content)
            if key is not None:
                if (self.movie[key] is None or len(self.movie[key]) == 0):
                    self.movie[key] = value
                    return
            else:
                http = c.select("span.ipc-metadata-list-item__list-content-item")
                if len(http) != 0:
                    inner_content = [s.text for s in http]

                    for content in inner_content:
                        # print(content)
                        key, value = self.extract_list_item(label, content)
                        if key is not None and (self.movie[key] is None or len(self.movie[key]) == 0):
                            self.movie[key] = value
                else:
                    http = c.select_one("div.ipc-metadata-list-item__content-container")
                    if http is not None:
                        content = http.text
                        # print(content)
                        key, value = self.extract_list_item(label, content)
                        if key is not None and (self.movie[key] is None or len(self.movie[key]) == 0):
                            self.movie[key] = value

    def crawl(self):
        if self.soup is None:
            return self.movie
            
        try:
            self.movie['name'] = self.soup.select_one("h1.sc-b73cd867-0.eKrKux").text
        except AttributeError:
            pass
        
        try:
            popularity = self.soup.select_one("div.sc-edc76a2-1.gopMqI").text.replace(",", ".")
            self.movie['popularity'] = float(popularity)
        except AttributeError:
            pass
        except ValueError:
            self.movie['popularity'] = popularity

        self.movie['top_cast'] = [actor.text for actor in self.soup.select("a.sc-11eed019-1.jFeBIw")]
        self.movie['top_cast_url'] = [actor['href'] for actor in self.soup.select("a.sc-11eed019-1.jFeBIw")]
        rating_http = self.soup.select("a.ipc-button ipc-button--single-padding ipc-button--center-align-content ipc-button--default-height ipc-button--core-baseAlt ipc-button--theme-baseAlt ipc-button--on-textPrimary ipc-text-button sc-f6306ea-2 dfHGIi".replace(" ", "."))

        rating = rating_http[0].select("span.sc-7ab21ed2-1.jGRxWM")[0].text
        try:
            self.movie['rating'] = float(rating)
        except ValueError:
            self.movie['rating'] = rating

        self.movie['rating_url'] = rating_http[0]['href']

        # rating_count = get_rating_count(rating_http[0]['href'])
        # if rating_count.isnumeric():
        #     self.movie['rating_count'] = int(rating_count)

        user_review_url = self.soup.select("a.ipc-link ipc-link--baseAlt ipc-link--touch-target sc-124be030-2 eshTwQ isReview".replace(" ", "."))[0]['href']
        
        user_review_count = self.get_review_count(user_review_url)
        if user_review_count.isnumeric():
            self.movie['user_review_count'] = int(user_review_count)

        critic_review_count = self.soup.select("a.ipc-link ipc-link--baseAlt ipc-link--touch-target sc-124be030-2 eshTwQ isReview".replace(" ", "."))[1].text.replace(",","").replace("Critic reviews", "")
        if critic_review_count.isnumeric():
            self.movie['critic_review_count'] = int(critic_review_count)
        
        # metascore = self.soup.select("a.ipc-link ipc-link--baseAlt ipc-link--touch-target sc-124be030-2 eshTwQ isReview".replace(" ", "."))[2].text.replace(",","").replace("Metascore", "")
        # if metascore.isnumeric():
        #     self.movie['metascore'] = int(metascore)

        http1 = self.soup.select("li.ipc-metadata-list__item")
        http2 = self.soup.select("li.ipc-metadata-list__item.ipc-metadata-list-item--link")

        for c in http1:
            label_http = c.select("span.ipc-metadata-list-item__label")
            url_http = c.select("a.ipc-metadata-list-item__list-content-item.ipc-metadata-list-item__list-content-item--link")
            self.extract_movie_info(c, label_http, url_http)

        for c in http2:
            label_http = c.select("a.ipc-metadata-list-item__label.ipc-metadata-list-item__label--link")
            url_http = c.select("a.ipc-metadata-list-item__list-content-item.ipc-metadata-list-item__list-content-item--link")
            self.extract_movie_info(c, label_http, url_http)

        return self.movie

    @staticmethod
    def extract_runtime(text):
        for time_format in ('%H hours %M minutes', '%H hour %M minutes', '%H hours %M minute', '%H hour %M minute'):
            try:
                timeobj = datetime.datetime.strptime(text, time_format).time()
                delta = datetime.datetime.combine(datetime.date.min, timeobj) - datetime.datetime.min
                return int(delta.total_seconds()/60)
            except ValueError:
                continue

        return None

    @staticmethod
    def extract_opening_date(text):
        months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        patterns = "(" + ")|(".join(months) + ")"
        full_patterns = f"(.*)(?={patterns})"
        return datetime.datetime.strptime(re.sub(full_patterns, "", text), '%b %d, %Y')

    @staticmethod
    def extract_release_date(text):
        text = re.sub(" \(.*?\)", "", text)
        for time_format in ('%B %d, %Y', '%Y', '%b %d, %Y'):
            try:
                return datetime.datetime.strptime(text, time_format)
            except ValueError:
                continue

        return pd.NaT

    @staticmethod
    def extract_revenue_usa_opening(text):
        months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        patterns = "(" + ")|(".join(months) + ")"
        full_patterns = f"(.*)(?={patterns})"
        new_text = re.sub("[\$,]", "", re.sub(re.sub(full_patterns, "", text), "", text))
        if new_text.isnumeric():
            return int(new_text)
        else:
            return None

    def get_rating_count(self, rating_url):
        soup = self.get_soup(rating_url)
        
        text = soup.select_one("div.allText > div.allText").text.split("\n")[1].strip()
        text_num = "".join(re.findall("[0-9]+", text))
        return text_num
    
    def get_review_count(self, user_review_url):
        soup = self.get_soup(user_review_url)
        
        text = soup.select("div.header > div > span")[0].text
        text_num = "".join(re.findall("[0-9]+", text))
        return text_num.replace(",", "").strip()

    @staticmethod
    def mapping_key(label):
        res = []

        if label in ("Director", "Directors"):
            res = ["directors"]
        elif label in ("Writer", "Writers"):
            res = ["writers"]
        elif label in ("Star", "Stars"):
            res = ["stars"]
        elif label in ("Genre", "Genres"):
            res = ["genres"]
        elif label in ("Country of origin", "Countries of origin"):
            res = ["country"]
        elif label in ("Language", "Languages"):
            res = ["language"]
        elif label == "Budget":
            res = ["budget"]
        elif label == "Gross US & Canada":
            res = ["revenue_usa"]
        elif label == "Opening weekend US & Canada":
                res = ["revenue_usa_opening", "opening_date"]
        elif label == "Gross worldwide":
            res = ["revenue_world"]
        elif label == "Runtime":
            res = ["runtime"]
        elif label == "Release date":
            res = ["release_date"]

        return res


    def extract_list_item(self, label, content):

        res = None, None

        if label in ("Director", "Directors"):
            res = "directors", re.sub("\(.*?\)", ",", content).strip(",").split(",")
        elif label in ("Writer", "Writers"):
            res = "writers", re.sub("\(.*?\)", ",", content).strip(",").split(",")
        elif label in ("Star", "Stars"):
            res = "stars", re.sub("\(.*?\)", ",", content).strip(",").split(",")
        elif label in ("Genre", "Genres"):
            res = "genres", content
        elif label in ("Country of origin", "Countries of origin"):
            res = "country", content  
        elif label in ("Language", "Languages"):
            res = "language", content    
        elif label == "Budget":
            text = re.sub("(\(.*?\))|(\$)|(,)", "", content).strip()
            if text.isnumeric():
                res = "budget", int(text)
        elif label == "Gross US & Canada":
            text = re.sub("(\(.*?\))|(\$)|(,)", "", content).strip()
            if text.isnumeric():
                res = "revenue_usa", int(text)
        elif label == "Opening weekend US & Canada":
            if "$" in content:
                res = "revenue_usa_opening", self.extract_revenue_usa_opening(content)
            else:
                res = "opening_date", self.extract_opening_date(content)
        elif label == "Gross worldwide":
            text = re.sub("(\(.*?\))|(\$)|(,)", "", content).strip()
            if text.isnumeric():
                res = "revenue_world", int(text)
        elif label == "Runtime":
            res = "runtime", self.extract_runtime(content)
        elif label == "Release date":
            res = "release_date", self.extract_release_date(content)

        return res

    def extract_list(self, label, inner_content):
        
        res = None, None

        if len(inner_content) == 1:
            res = self.extract_list_item(label, inner_content[0])
        else:
            label_key = None
            content_list = []
            for content in inner_content:
                label_key, content_value = self.extract_list_item(label, content)
                if type(content_value) != list:
                    content_list.append(content_value)
                else:
                    content_list.extend(content_value)
                    
            res = label_key, content_list

        return res

    @staticmethod
    def extract_crew_url(label, url_http):
        if url_http:
            crew_urls = [k['href'] for k in url_http]

            if label in ("Director", "Directors"):
                res = "directors_url", crew_urls
            elif label in ("Writer", "Writers"):
                res = "writers_url", crew_urls
            else:
                res = None, []

            return res
        else:
            return None, []

class BulkMovieCrawler(BaseBulkCrawler):

    def __init__(self, url):
        self.soup = self.get_soup(url)

        self.urls = [element.attrs.get('href') for element in self.soup.select('td.titleColumn a')]
        # print(len(self.urls))

        self.movie_names = [re.findall("(?<=>)(.*?)(?=<)", str(t))[0] for t in self.soup.select('td.titleColumn')]
        # print(len(self.movie_names))

        self.movie_rating_counts = [element.attrs.get('data-value') for element in self.soup.select('td.posterColumn span[name=nv]')]
        # print(len(self.movie_rating_counts))

    def _single_crawl(self, crawler, index, status:bool=True):
        if status:
            movie_dict = crawler.crawl()
        else:
            movie_dict = crawler.movie

        if movie_dict['name'] is None:
            movie_dict['name'] = self.movie_names[index]
        if movie_dict['rating_count'] is None:
            if self.movie_rating_counts[index].isnumeric():
                movie_dict['rating_count'] = self.movie_rating_counts[index]
        movie_dict['id'] = index
        movie_dict['url'] = self.urls[index]

        return movie_dict

class BulkMovieCrawlerSimple(BaseBulkCrawler):

    def __init__(self, url):
        self.soup = self.get_soup(url)

        self.urls = [element.attrs.get('href') for element in self.soup.select('td.titleColumn a')]
        # print(len(self.urls))

        self.movie_names = [re.findall("(?<=>)(.*?)(?=<)", str(t))[0] for t in self.soup.select('td.titleColumn')]
        # print(len(self.movie_names))

        self.movie_rating_counts = [element.attrs.get('data-value') for element in self.soup.select('td.posterColumn span[name=nv]')]
        # print(len(self.movie_rating_counts))

    def _single_crawl(self, crawler, index, status:bool=True):
        if status:
            movie_dict = crawler.crawl()
        else:
            movie_dict = crawler.movie

        if movie_dict['name'] is None:
            movie_dict['name'] = self.movie_names[index]
        if movie_dict['rating_count'] is None:
            if self.movie_rating_counts[index].isnumeric():
                movie_dict['rating_count'] = self.movie_rating_counts[index]
        movie_dict['id'] = index
        movie_dict['url'] = self.urls[index]

        return movie_dict