#-*- coding:utf-8 -*-
u"""
Downloads Urls parses them an store them in a Mongodb Database
Created on 25/09/13
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import bs4
import feedparser
import pymongo
import logging
import requests
import settings
from multiprocessing.pool import ThreadPool

logger = logging.getLogger("downloader.rss")

 ## Media Cloud database setup

client = pymongo.MongoClient(settings.MONGOHOST, 27017)
MCDB = client.MCDB
FEEDS = MCDB.feeds  # Feed collection
ARTICLES = MCDB.articles  # Article Collection


class RSSDownload(object):
    def __init__(self, url):
        self.url = url

    def parse(self):
        response = feedparser.parse(self.url)
        self._save_articles(response.entries)
        return ((r.title, r.link) for r in response.entries)

    def _save_articles(self, entries):
        for a in entries:
            a.link_content = requests.get(a.link).content
            # Turn the tags field into a simple list of tags
            a.tags = [i['term'] for i in a.tags]
            a.pop('published_parsed')
            ARTICLES.insert(a)

def fetch_feed(feed):
    f = RSSDownload(feed)
    f.parse()

def parallel_fetch():
    """
    Starts parallel threads to fetch feeds.
    """
    feeds = FEEDS.find()
    P = ThreadPool(30)
    P.map(fetch_feed(), feeds)
