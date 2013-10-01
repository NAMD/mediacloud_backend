#!/usr/bin/env python
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
from bson.errors import InvalidDocument
import time


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
            ks = []
            for k, v in a.iteritems():
                if isinstance(v, time.struct_time):
                    ks.append(k)
            [a.pop(i) for i in ks]
            a['link_content'] = requests.get(a.link).content
            # Turn the tags field into a simple list of tags
            try:
                a['tags'] = [i['term'] for i in a.tags]
            except AttributeError:
                print "This feed has no tags: ", a.link
            try:
                a.pop('published_parsed')
            except KeyError:
                pass
            exists = list(ARTICLES.find({"link": a.link}))
            print exists
            if exists == []:
                ARTICLES.insert(a)
                print "inserted"

def fetch_feed(feed):
    try:
        f = RSSDownload(feed)
    except InvalidDocument:
        print "This feed failed: \n", f
    f.parse()

def parallel_fetch():
    """
    Starts parallel threads to fetch feeds.
    """
    feeds = FEEDS.find()
    feedurls = []
    for f in feeds:
        try:
            feedurls.append(f["title_detail"]["base"])
        except KeyError:
            print f

    P = ThreadPool(30)
    P.map(fetch_feed, feedurls)

if __name__ == "__main__":
    parallel_fetch()
