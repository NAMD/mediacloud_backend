import re
import sys
import pymongo
import logging
import settings
import datetime
import requests

from goose import Goose
from bs4 import BeautifulSoup
from downloader import compress_content
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

client = pymongo.MongoClient(settings.MONGOHOST, 27017)
mcdb = client.MCDB
ARTICLES = mcdb.articles  # Article Collection
ARTICLES.ensure_index("source")


def find_articles(page=None):
    """
    """

    base_url = "http://www1.folha.uol.com.br/ultimas-noticias/"
    if page is None:
        index_url = base_url + "index.shtml"
    else:
        index_url = base_url + "noticias-{0}.shtml".format(page)

    index = requests.get(index_url).content
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

def extract_category(url):
    """
    """

    try:
        base_url = re.search('http://(.+?)/', url).group(1)
        category = re.search(base_url + '/(.+?)/', url).group(1)
    except AttributeError:
        logger.error("Some problem has occured in extraction of articles' category")
        return None
    return category

def extract_content(article):
    content_body = article.cleaned_text

    if "post completo no blog" not in content_body:
        return content_body
    else:
        logger.error("The article is into blog's link")
        return None

def download_article(url):
    """ Download the html content of a news page

    :param url: news page's url
    :type url: string
    :return: news page's content
    :rtype: requests.models.Response
    """

    article = { 'link': url, 'source': 'crawler_folha_sao_paulo' }
    logger.info("Downloading article: {0}".format(url))

    try:
        response = requests.get(url, timeout=30)
    except ConnectionError:
        logger.error("Failed to fetch:{0}".format(url))
        return None
    except Timeout:
        logger.error("Timed out while fetching {0}".format(url))
        return None

    extractor = Goose({'use_meta_language': False, 'target_language':'pt'})
    news = extractor.extract(url=url)

    article['title'] = extract_title(news)
    article['category'] = extract_category(url)
    article['body_content'] = extract_content(news)

    return article


if __name__ == '__main__':
    for url in find_articles():
        exists = list(ARTICLES.find({"link": url}))
        if not exists:
            article = download_article(url)
            if article['body_content'] is None:
                continue
            ARTICLES.insert(article, w=1)
