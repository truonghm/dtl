from src.crawler.movie import BulkMovieCrawler, MovieCrawler
from src.crawler.rating import BulkRatingCrawler, RatingCrawler
from src.crawler import write_to_cache
import json
import time
import sys
import pandas as pd

def crawl_movies(stop_at:int=None):
    crawler = BulkMovieCrawler("/chart/top")
    movie_list = crawler.bulk_craw(MovieCrawler, stop_at=stop_at)

    file_name = './cache/raw_movie_data.json'
    with open(file_name, 'w+') as f:
        f.write(json.dumps(movie_list, default=str))

    write_to_cache(movie_list=movie_list)

def crawl_ratings(stop_at:int=None):
    crawler = BulkRatingCrawler()
    rating_list = crawler.bulk_craw(RatingCrawler, stop_at=stop_at)
    
    rating_dist = pd.concat([data[0] for data in rating_list], axis=0)
    rating_dist.to_csv('./cache/rating_dist.csv', index=False)

    rating_demo = pd.concat([data[1] for data in rating_list], axis=0)
    rating_demo.to_csv('./cache/rating_demo.csv', index=False)

if __name__ == "__main__":

    start = time.time()

    try:
        arg = sys.argv[1]

        if arg == 'movie':
            crawl_movies()

        elif arg == 'rating':
            crawl_ratings()

        else:
            print("Start crawling movies")
            crawl_movies()
            print("Start crawling ratings")
            crawl_ratings()
    except IndexError:
        print("Start crawling movies")
        crawl_movies()
        print("Start crawling ratings")
        crawl_ratings()

    finally:
        print("Runtime: ", time.time() - start, "seconds")
        print("Done")
