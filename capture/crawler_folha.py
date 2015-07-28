import sys
import pymongo
import logging
import settings
import datetime
import requests

from goose import Goose
from bs4 import BeautifulSoup
from logging.handlers import RotatingFileHandler


###########################
#  Setting up Logging
###########################

logger = logging.getLogger("Folha de Sao Paulo")
logger.setLevel(logging.DEBUG)

# create stream handler and set level to debug
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler('/tmp/mediacloud_folha_sao_paulo.log',
                          maxBytes=5e6,
                          backupCount=3)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - \
                               %(levelname)s - %(message)s')

# add formatter to stream_handler
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# add stream_handler to logger
logger.addHandler(stream_handler)  # uncomment for console output of messages
logger.addHandler(file_handler)

# client = pymongo.MongoClient(settings.MONGOHOST, 27017)
# mcdb = client.MCDB
# ARTICLES = mcdb.articles  # Article Collection
# ARTICLES.ensure_index("source")


def find_articles(page=None):

    base_url = "http://www1.folha.uol.com.br/ultimas-noticias/"
    if page is None:
        INDEX_URL = base_url + "index.shtml"
    else:
        INDEX_URL = base_url + "noticias-{0}.shtml".format(page)

    print(INDEX_URL)
    index = requests.get(INDEX_URL).content
    soup = BeautifulSoup(index)
    news_index = soup.find(**{'class': 'news-index'}).find('ol')
    news_urls = [url.attrs['href'] for url in news_index.find_all('a')]
    return news_urls

def extract_title(article):
    """ Extract the news title.
    """

    try:
        title = article.title
    except Exception as ex:
        template = "An exception of type {0} occured during extraction of news title. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.exception(message)
        return None
    if title is None:
        logger.error("The news title is None")
    return title


