import datetime
import logging
from logging.handlers import RotatingFileHandler

import pymongo
from bs4 import BeautifulSoup
import requests

import settings
from downloader import compress_content, detect_language

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
    INDEX_URL = "http://oglobo.globo.com/ultimas-noticias/"

    index = requests.get(INDEX_URL).content
    soup = BeautifulSoup(index)
    news_index = soup.find(id="ultimasNoticias").find('ul')
    news_urls = set([n.attrs['href'] for n in news_index.find_all('a')])
    return news_urls

def get_published_time(soup):
    # Parsing date strings
    # Parse date from article
    time_tag = soup.find('time')
    if time_tag is None:
        return None
    else:
        published_time_str = time_tag.attrs['datetime']
        try:
            published_time = datetime.datetime.strptime(published_time_str,
             '%Y-%m-%dT%H:%M')
        except ValueError:
            published_time = datetime.datetime.strptime(published_time_str,
             '%Y-%m-%d')

        return published_time

def download_article(url):
    article = {
        'link': url,
        'source': 'crawler_oglobo',
    }
    logger.info("Downloading article: %s", url)
    try:
        response = requests.get(url, timeout=30)
    except ConnectionError:
        logger.error("Failed to fetch %s", url)
        return
    except Timeout:
        logger.error("Timed out while fetching %s", url)
        return

    encoding = response.encoding if response.encoding is not None else 'utf8'
    dec_content = response.content.decode(encoding)
    article['link_content'] = compress_content(dec_content)

    article['compressed'] = True
    article['language'] = detect_language(dec_content)


    soup = BeautifulSoup(dec_content)
    article['title'] = soup.find('title').text.strip()

    article['published'] = get_published_time(soup)

    return article

for url in find_articles():
    exists = list(ARTICLES.find({"link": url}))
    if not exists:
        article = download_article(url)
        ARTICLES.insert(article, w=1)
