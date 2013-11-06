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
from pymongo.errors import DuplicateKeyError
import bson
import time
import datetime
import zlib
import cPickle as CP
import cld
from dateutil.parser import parse

from logging.handlers import RotatingFileHandler

###########################
#  Setting up Logging
###########################
logger = logging.getLogger("Downloader")
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = RotatingFileHandler('/tmp/mediacloud.log', maxBytes=5e6, backupCount=3)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add ch to logger
#logger.addHandler(ch)  # uncomment for console output of messages
logger.addHandler(fh)


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
        if response.bozo:
            logger.error("fetching %s returned an exception: %s", self.url, response.bozo_exception)
            return

        self._save_articles(response.entries)
        return ((r.title, r.link) for r in response.entries)

    def _save_articles(self, entries):
        logger.info("Downloading %s articles from %s", len(entries), self.url)
        for entry in entries:
            if "%set" in entry:  # hallmark of empty article
                logger.error("Empty article from %s", self.url)
                continue
            ks = []
            for k, v in entry.iteritems():
                if isinstance(v, time.struct_time):
                    # Convert to datetime instead of removing
                    entry[k] = datetime.datetime.fromtimestamp(time.mktime(v))
                    #ks.append(k)
            #[a.pop(i) for i in ks]

            r = requests.get(entry.link)
            # print r.encoding
            try:
                encoding = r.encoding if r.encoding is not None else 'utf8'
                # Articles are first decoded with the declared encoding and then compressed with zlib
                dec_content = r.content.decode(encoding)
                entry['link_content'] = compress_content(dec_content)
                entry['compressed'] = True
                entry['language'] = detect_language(dec_content)
                # Parsing date strings
                if 'published' in entry:
                    try:
                        entry['published'] = parse(entry['published'])
                    except ValueError:
                        logger.error("Could not parse date %s ", entry['published'])
                if 'updated' in entry:
                    try:
                        entry['updated'] = parse(entry['updated'])
                    except ValueError:
                        logger.error("Could not parse date %s ", entry['updated'])
                else:
                    entry['updated'] = datetime.datetime.now()
            except UnicodeDecodeError:
                print "could not decode page as ", encoding
                continue
            # Turn the tags field into a simple list of tags
            try:
                entry['tags'] = [i['term'] for i in entry.tags]
            except AttributeError:
                logger.info("This feed has no tags: %s", entry.link)
            try:
                entry.pop('published_parsed')
            except KeyError:
                pass
            exists = list(ARTICLES.find({"link": entry.link}))
            # print exists
            if not exists:
                if "published" in entry:
                    # consider parsing the string datetime into a datetime object
                    pass
                try:
                    ARTICLES.insert(entry, w=1)
                except DuplicateKeyError:
                    logger.error("Duplicate article found")
                # print "inserted"


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


def decompress_content(comphtml):
    """
    Decompress data compressed by `compress_content`
    :param comphtml: compressed html document
    :return: original html
    """
    # unencoded = b64.urlsafe_b64decode(str(comphtml))
    decompressed = zlib.decompress(comphtml)
    orig_html = CP.loads(decompressed)
    return orig_html


def detect_language(text):
    """
    Detect the language of text using chromium_compact_language_detector
    :param text: text to be analyzed
    :return: {"name": portuguese, "pt"}
    """
    name, code, isReliable, textBytesFound, details = cld.detect(text.encode('utf8'))
    return {"name": name, "code": code}


def fetch_feed(feed):
    try:
        f = RSSDownload(feed)
    except InvalidDocument:
        logger.error("This feed failed: %s", f)
    f.parse()


def parallel_fetch():
    """
    Starts parallel threads to fetch feeds.
    """
    feed_cursor = FEEDS.find()
    feed_urls = []
    t0 = time.time()
    feeds_scanned = 0
    while feeds_scanned < feed_cursor.count():
        feed_cursor = FEEDS.find()[feeds_scanned:feeds_scanned+100]
        for feed in feed_cursor:
            t = feed.get('title_detail', feed.get('subtitle_detail', None))
            if t is None:
                logger.error("Feed %s does not contain ", feed.get('link', None))
                continue
            try:
                feed_urls.append(t["base"].decode('utf8'))
            except KeyError:
                logger.error("Feed %s does not contain base URL", feed.get('link', None))
            except UnicodeEncodeError:
                logger.error("Feed %s failed Unicode decoding", feed.get('link', None))
            #fetch_feed(t["base"].decode('utf8'))

        P = ThreadPool(20)
        P.map(fetch_feed, feed_urls)
        P.close()
        feeds_scanned += 100
    logger.info("Time taken to download %s feeds: %s minutes.", len(feed_urls), (time.time()-t0)/60.)

if __name__ == "__main__":
    parallel_fetch()
