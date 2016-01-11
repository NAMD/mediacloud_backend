# -*- coding: utf-8 -*-
from goose import Goose
import pymongo
from bs4 import BeautifulSoup
import requests
import datetime
import zlib
import cPickle as CP
import cld
from requests.exceptions import ConnectionError, Timeout
import bson
import settings
import logging_mc
import re

logger = logging_mc.get_logger('ZH')

client = pymongo.MongoClient(settings.MONGOHOST, 27017)
MCDB = client.MCDB
ARTICLES = MCDB.articles  # Article Collection
ARTICLES.ensure_index("source")

def find_articles():
    """
    Get the urls of last news
    :return: last news' urls of all categories
    :rtype: set()
    """
    urls = ['http://zh.clicrbs.com.br/rs/noticias/ultimas-noticias/',
            'http://zh.clicrbs.com.br/rs/entretenimento/ultimas-noticias/',
            'http://zh.clicrbs.com.br/rs/esportes/ultimas-noticias/',
            'http://zh.clicrbs.com.br/rs/porto-alegre/ultimas-noticias/',
            'http://zh.clicrbs.com.br/rs/vida-e-estilo/ultimas-noticias/',
            'http://zh.clicrbs.com.br/rs/ultimas-noticias/']
    news_urls = list()
    for INDEX_URL in urls:
        index = requests.get(INDEX_URL).content
        soup = BeautifulSoup(index, "lxml")
        news_index = soup.find_all(attrs={'class': re.compile(r".*\bmateria-manchete\b.*")})
        news_urls = news_urls + ['' + BeautifulSoup(art.encode('utf8'), "lxml").find('a').attrs['href'] for art in news_index]
    return set(news_urls)

def get_published_time(soup):
    """
    Get the news' published datetime
    :param soup: object with news html page
    :type soup: BeautifulSoup object
    :return: news published datetime
    :rtype: string
    """
    try:
        time_tag = soup.find('div', class_='meta__date').text
    except IndexError:
        logger.error('wrong time tag')
        return None
    if time_tag is None:
        return None
    else:
        try:
            match = re.search(r'\d{2}/\d{2}/\d{4} - \d{2}h\d{2}min', time_tag.encode('utf8'))
            published_time = datetime.datetime.strptime(match.group(), '%d/%m/%Y - %Hh%Mmin')
        except ValueError:
            logger.error('wrong date extraction')
            return None
        return published_time

def extract_title(article):
    """
    Extract the news title.
    """

    try:
        title = article.title
    except Exception as ex:
        template = "An exception of type {0} occured during extraction of news title. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.exception(message)
        return None
    return title

def extract_content(article):
    """
    Extract relevant information about news page
    """

    try:
        body_content = article.cleaned_text
    except Exception as ex:
        template = "An exception of type {0} occured during extraction of news content. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.exception(message)
        return None
    return body_content

def detect_language(text):
    """
    Detect the language of text using chromium_compact_language_detector
    :param text: text to be analyzed
    :return: {"name": portuguese, "pt"}
    """
    name, code, isReliable, textBytesFound, details = cld.detect(text.encode('utf8'))
    return {"name": name, "code": code}

def compress_content(html):
    """
    Compresses and encodes html content so that it can be BSON encoded an store in mongodb
    :param html: original html document
    :return: compressed an b64 encoded document
    """
    pickled = CP.dumps(html, CP.HIGHEST_PROTOCOL)
    squished = zlib.compress(pickled)
    encoded = bson.Binary(squished)  # b64.urlsafe_b64encode(squished)
    return encoded

def decompress_content(compressed_html):
    """
    Decompress data compressed by `compress_content`
    :param compressed_html: compressed html document
    :return: original html
    """
    # unencoded = b64.urlsafe_b64decode(str(compressed_html))
    decompressed = zlib.decompress(compressed_html)
    orig_html = CP.loads(decompressed)
    return orig_html


def download_article(url):
    """
    Download the html content of a news page
    :param url: news page's url
    :type url: string
    :return: news page's content
    :rtype: requests.models.Response
    """
    article = {
        'link': url,
        'source': 'crawler_ZH',
    }
    logger.info("Downloading article: {0}".format(url))
    try:
        response = requests.get(url, timeout=30)
    except ConnectionError:
        logger.error("Failed to fetch {0}".format(url))
        return
    except Timeout:
        logger.error("Timed out while fetching {0}".format(url))
        return

    encoding = response.encoding if response.encoding is not None else 'utf8'
    dec_content = response.content.decode(encoding)
    soup = BeautifulSoup(dec_content, "lxml")
    extractor  = Goose({'use_meta_language': False, 'target_language':'pt'})
    news = extractor.extract(url=url)

    article['link_content'] = compress_content(dec_content)
    article['compressed'] = True
    article['language'] = detect_language(dec_content)
    article['title'] =  extract_title(news)
    article['published'] = get_published_time(soup)
    article['main_text'] = extract_content(news)

    return article

if __name__ =='__main__':
    for url in find_articles():
        logger.info("url: {0}".format(url))
        exists = list(ARTICLES.find({"link": url}))
        if not exists:
            article = download_article(url)
            logger.info("Download done")
            ARTICLES.insert(article, w=1)
            logger.info("Saved")
        else:
            logger.info("It already exists")
