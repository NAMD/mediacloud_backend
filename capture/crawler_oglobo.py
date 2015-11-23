import datetime
import logging
from logging.handlers import RotatingFileHandler

import pymongo
from bs4 import BeautifulSoup
import requests

from goose import Goose
import settings
from downloader import compress_content, detect_language

from dateutil.parser import parse


###########################
#  Setting up Logging
###########################
logger = logging.getLogger("OGlobo")
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler('/tmp/mediacloud_oglobo.log',
                                    maxBytes=5e6,
                                    backupCount=3)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to stream_handler
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# add stream_handler to logger
logger.addHandler(stream_handler)  # uncomment for console output of messages
logger.addHandler(file_handler)


client = pymongo.MongoClient(settings.MONGOHOST, 27017)
MCDB = client.MCDB
ARTICLES = MCDB.articles  # Article Collection

def find_articles():
    INDEX_URL = "http://oglobo.globo.com/ultimas-noticias/"

    index = requests.get(INDEX_URL).content
    soup = BeautifulSoup(index)
    news_index = soup.find(id="ultimasNoticias").find('ul')
    news_urls = set([n.attrs['href'] for n in news_index.find_all('a')])
    return news_urls

def extract_published_time(soup):
    # Parsing date strings
    # Parse date from article
    time_tag = soup.find('time')
    if time_tag is None:
        return datetime.datetime.today()
    else:
        published_time_str = time_tag.attrs['datetime']
        try:
            published_time = parse(published_time_str)
            if published_time is None:
                published_time = datetime.datetime.today()
        except Exception as ex:
            logger.warning("Failed to parse published_time field with error: {0}".format(ex))
            return datetime.datetime.today()
        return published_time

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

def extract_content(article):
    """ Extract relevant information about news page
    """

    try:
        body_content = article.cleaned_text
    except Exception as ex:
        template = "An exception of type {0} occured during extraction of news content. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.exception(message)
        return None
    if body_content is None:
        logger.error("The news content is None")
    return body_content

def download_article(url):
    article = { 'link': url, 'source': 'crawler_oglobo'}
    logger.info("Downloading article: {0}".format(url))

    try:
        response = requests.get(url, timeout=30)
    except Exception as ex:
        logger.exception("Failed to fetch {0}. Exception: {1}".format(url, ex))
        return None

    extractor = Goose({'use_meta_language': False, 'target_language':'pt'})
    news = extractor.extract(url=url)
    soup = BeautifulSoup(response.text)

    article['link_content'] = compress_content(response.text)
    article['compressed'] = True
    article['language'] = detect_language(response.text)
    article['title'] = extract_title(news)
    article['published_time'] = extract_published_time(soup)
    article['body_content'] = extract_content(news)

    return article

if __name__ == '__main__':
    for url in find_articles():
        exists = list(ARTICLES.find({"link": url}))
        if not exists:
            article = download_article(url)
            if article['body_content'] is None:
                continue
            if article['published_time'] is None:
                continue
            ARTICLES.insert(article, w=1)
