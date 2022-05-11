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
from abc import ABC, abstractmethod
from typing import Union, List
import logging
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(sys.path[0])))

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("logs/crawler_log.log")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(handler)

from src.transform.db_input import transform_actors, transform_directors, transform_writers, transform_stars
from src.config import Setting


def get_soup(path_url):
    try:
        full_url = Setting.BASE_URL + path_url
        # print(full_url)
        headers = {"User-Agent": Setting.USER_AGENT}

        time.sleep(Setting.INTERVAL_DELAY)
        res = requests.get(full_url, headers)
        soup = BeautifulSoup(res.text, "html.parser")
        return soup
    except Exception as e:
        raise e


class BaseCrawler(ABC):
    @abstractmethod
    def __init__(self):
        pass

    def get_soup(self, path_url):
        return get_soup(path_url=path_url)

    @abstractmethod
    def crawl(self) -> pd.DataFrame:
        pass

    def save_cache(self, crawler_output, file_name: Union[List, str] = None):
    

        if file_name is not None:
            # print(type(file_name))
            if not isinstance(file_name, list) and not isinstance(file_name, str):
                raise TypeError("Invalid file name type")

        # print(type(crawler_output))
        if isinstance(crawler_output, pd.DataFrame):
            # print('df')
            if file_name is None:
                file_name = Setting.CACHE + self.__class__.__name__
            elif type(file_name) == str:
                file_name = Setting.CACHE + file_name
            elif type(file_name) == list:
                # file_name = [Setting.CACHE + fn for fn in file_name]
                raise TypeError("Invalid file name type")
            
            file_name = f"{file_name}.csv"
            print(file_name)
            crawler_output.to_csv(file_name, index=False)
        elif isinstance(crawler_output, tuple):
            # print('list')
            if file_name is None or type(file_name) == str:
                if file_name is None:
                    file_name = Setting.CACHE + self.__class__.__name__
                elif type(file_name) == str:
                    file_name = Setting.CACHE + file_name

                for n, o in enumerate(crawler_output):
                    if isinstance(o, pd.DataFrame):
                        new_file_name = f"{file_name}_{n}.csv"
                        print(new_file_name)
                        o.to_csv(new_file_name, index=False)

            elif type(file_name) == list:
                file_name = [Setting.CACHE + fn for fn in file_name]
                for fn, o in zip(file_name, crawler_output):
                    if isinstance(o, pd.DataFrame):
                        new_fn = f"{fn}.csv"
                        print(new_fn)
                        o.to_csv(new_fn, index=False)
                # raise TypeError("Invalid file name type")

        elif isinstance(crawler_output, (list, dict)):
            if file_name is None:
                file_name = Setting.CACHE + self.__class__.__name__
            elif type(file_name) == str:
                file_name = Setting.CACHE + file_name
            elif type(file_name) == list:
                # file_name = [Setting.CACHE + fn for fn in file_name]
                raise TypeError("Invalid file name type")

            file_name = f"{file_name}.json"
            print(file_name)
            with open(file_name, "w+") as f:
                f.write(json.dumps(crawler_output, default=str))

        else:
            raise TypeError("Invaid crawler output")

        return file_name


class BaseBulkCrawler(BaseCrawler):
    @abstractmethod
    def __init__(self, url, load_from_cache: bool = True, stop_at: int = None):
        # raise NotImplementedError
        self.stop_at = stop_at

    def bulk_craw(
        self, CrawlerObject, write_to_cache: bool = True, file_name: str = None
    ):

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
                
                if type(self.stop_at) == int and self.stop_at is not None:
                    if index >= self.stop_at:

                        if len(result_list) > 0:
                            if isinstance(result_list[0], pd.DataFrame):
                                res = pd.concat(result_list, axis=0)
                            elif isinstance(result_list[0], tuple):
                                res = tuple([
                                    pd.concat([r[i] for r in result_list], axis=0) for i in result_list[0]
                                ])
                            if write_to_cache:
                                self.save_cache(res, file_name)

                        # print(type(res))
                            return res  
                        else:
                            return None
                try:
                    crawler = CrawlerObject(url)
                    result = self.crawl(crawler, index, True)
                    result_list.append(result)
                    success_urls.append(url)
                    print(url, index)
                except Exception as e:

                    # print(url, repr(e))
                    logger.debug(url)
                    logger.error(repr(e))
                    # raise e
                    # raise e

            retry_count += 1

        if isinstance(result_list[0], pd.DataFrame):
            res = pd.concat(result_list, axis=0)
        elif isinstance(result_list[0], tuple):
            res = tuple([
                pd.concat([r[i] for r in result_list], axis=0) for i in range(len(result_list[0]))
            ])
        if write_to_cache:
            self.save_cache(res, file_name)

        # print(type(res))
        return res

    @abstractmethod
    def crawl(self, crawler, index, status: bool = True):
        pass
