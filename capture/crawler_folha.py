import time
import datetime
import logging
from logging.handlers import RotatingFileHandler
import random

import pymongo
from bs4 import BeautifulSoup
import requests

import settings

from crawler_utils import download_article

###########################
#  Setting up Logging
###########################
logger = logging.getLogger("OGlobo")
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = RotatingFileHandler('/tmp/mediacloud_oglobo.log', maxBytes=5e6, backupCount=3)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add ch to logger
#logger.addHandler(ch)  # uncomment for console output of messages
logger.addHandler(fh)

client = pymongo.MongoClient(settings.MONGOHOST, 27017)
MCDB = client.MCDB
ARTICLES = MCDB.articles  # Article Collection
ARTICLES.ensure_index("source")


def find_articles():
    INDEX_URL = "http://www1.folha.uol.com.br/ultimas-noticias/index.shtml"

    index = requests.get(INDEX_URL).content
    soup = BeautifulSoup(index)
    news_index = soup.find(**{'class': 'news-index'}).find('ol')
    news_urls = set([n.attrs['href'] for n in news_index.find_all('a')])

    return news_urls

def get_published_time(soup):
    time_tag = soup.find('time')
    try:
        if time_tag is not None:
            published_time_str = time_tag.attrs['datetime']
            published_time = datetime.datetime.strptime(published_time_str,
                    '%Y-%m-%d %H:%M')

            return published_time

        else:
            time_tag = soup.find(id='articleDate')
            published_time_str = time_tag.text.replace('\n', '').replace(' ', '')
            published_time = datetime.datetime.strptime(published_time_str,
                    '%d/%m/%Y-%Hh%M')
            return published_time
    except Exception:
        return None

for idx, url in enumerate(find_articles()):
    exists = list(ARTICLES.find({"link": url}))
    if not exists:
        article = download_article(url, 'crawler_folha', get_published_time,
                logger)
        if article['published'] is None:
            # This means we ran into the random IOS app ad page. Just ignore it
            # and let this article be saved another time.
            continue
        ARTICLES.insert(article, w=1)
    time.sleep(random.random() + 5)
