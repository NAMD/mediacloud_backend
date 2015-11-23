import re
import sys
import pymongo
import logging
import settings
import datetime
import requests

from goose import Goose
from bs4 import BeautifulSoup
from downloader import compress_content, detect_language
from logging.handlers import RotatingFileHandler

from dateutil.parser import parse


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
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to stream_handler
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# add stream_handler to logger
logger.addHandler(stream_handler)  # uncomment for console output of messages
logger.addHandler(file_handler)

client = pymongo.MongoClient(settings.MONGOHOST, 27017)
mcdb = client.MCDB
ARTICLES = mcdb.articles  # Article Collection


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
        #returns 'http: or 'https:'
        app_protocol = re.search('(.+?)/', url).group(1)
        base_url = re.search(app_protocol + '//(.+?)/', url).group(1)
        category = re.search(base_url + '/(.+?)/', url).group(1)
    except AttributeError:
        logger.error("Some problem has occured during extraction of article's category")
        return None
    return category

def extract_content(article, article_soup=None):
    """
    """

    try:
       body_content = article.cleaned_text
    except Exception as ex:
        template = "An exception of type {0} occured during extraction of news content. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        logger.exception(message)
        return None

    if "post completo no blog" not in body_content:
        return body_content

    try:
        content = article_soup.find("article", {"id":"news"})
        urls = content.find_all("a")
    except Exception as ex:
        logger.exception('An exception ot type {0} occurred during extraction of links in news content.'
            .format(ex))

    blog_url = [url['href'] for url in urls if url.text == u'blog']

    if blog_url is None:
        logger.error("There's no blog link in article's content. Check out error")
        return None

    if len(blog_url) is not 1:
        logger.error("There are multiples links whose name is 'blog'. Check out the right one to extract its content.")
        return None

    extractor = Goose({'use_meta_language': False, 'target_language':'pt'})
    blog_goose = extractor.extract(url=blog_url[0])
    blog_content = extract_content(blog_goose)

    return blog_url, blog_content

def extract_published_time(soup):
    """
    """

    time_tag = soup.find("time")
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
    except Exception as ex:
        logger.exception("Failed to fetch {0}".format(url))
        return None

    extractor = Goose({'use_meta_language': False, 'target_language':'pt'})
    news = extractor.extract(url=url)
    soup = BeautifulSoup(response.content)

    article['link_content'] = compress_content(response.text)
    article['compressed'] = True
    article['language'] = detect_language(response.text)
    article['title'] = extract_title(news)
    article['category'] = extract_category(url)
    article['published_time'] = extract_published_time(soup)

    content =  extract_content(news, soup)

    if len(content) is 2:
        article['link'], article['body_content'] = content
    else:
        article['body_content'] = content

    return article


if __name__ == '__main__':
    for url in find_articles():
        exists = list(ARTICLES.find({"link": url}))
        if not exists:
            article = download_article(url)
            if article['body_content'] is None:
                continue
            ARTICLES.insert(article, w=1)
            print(article['published_time'])


