#!/usr/bin/env python
#-*- coding:utf-8 -*-
u"""
Downloads Urls parses them an store them in a Mongodb Database
Created on 25/09/13
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import logging
from multiprocessing.pool import ThreadPool
import time
import datetime
import zlib
import cPickle as CP
import cld
import sys
import os
from logging.handlers import RotatingFileHandler
import feedparser
import pymongo
import goose
import requests
from requests.exceptions import ConnectionError, MissingSchema, Timeout
from bson.errors import InvalidDocument
from pymongo.errors import DuplicateKeyError
import bson
from dateutil.parser import parse

import settings
import elasticsearch

#import nlp
#import load_into_pypln

es = elasticsearch.Elasticsearch(hosts=[settings.ELASTICHOST])

sys.path.append('/'.join(os.getcwd().split("/")[:-1]))

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

## Ensure indices are created
FEEDS.ensure_index([("subtitle_detail.base", pymongo.ASCENDING)])
FEEDS.ensure_index([("last_visited", pymongo.DESCENDING), ("updated", pymongo.DESCENDING)])
ARTICLES.ensure_index([("link", pymongo.ASCENDING), ("published", pymongo.ASCENDING)])
ARTICLES.ensure_index([("published", pymongo.DESCENDING)])
ARTICLES.ensure_index("cleaned_text")




config = {
    'threads': 45,  # Number of threads used in the fetching pool
}


def goosefy(content, article):
    cleaned_text = ''
    if len(content.strip()) > 0:
        cleaned_text = goose.Goose({'enable_image_fetching': False, 'use_meta_language': False,
                                'target_language': 'pt'}).extract(raw_html=content).cleaned_text
    if len(cleaned_text) == 0:
        if article.has_key('summary'):
            cleaned_text = article['summary']

    return cleaned_text


class RSSDownload(object):
    def __init__(self, feed_id, url):
        self.url = url
        self.feed_id = feed_id

    def parse(self):
        response = feedparser.parse(self.url)
        if response.bozo:
            logger.error("fetching %s returned an exception: %s", self.url, response.bozo_exception)
            return
        if not response.entries:
            logger.warning("{} had no entries".format(self.url))
            return

        self._save_articles(response.entries)
        if self.feed_id is not None:
            FEEDS.update({"_id": self.feed_id}, {"$set": {"last_visited": datetime.datetime.now()}})
        return ((r.get('title'), r.get('link')) for r in response.entries)

    def _save_articles(self, entries):
        logger.info("Downloading %s articles from %s", len(entries), self.url)
        #corpus = nlp.get_corpus()
        for entry in entries:
            if "%set" in entry:  # hallmark of empty article
                logger.error("Empty article from %s", self.url)
                continue

            for k, v in entry.iteritems():
                if isinstance(v, time.struct_time):
                    # Convert to datetime
                    entry[k] = datetime.datetime.fromtimestamp(time.mktime(v))

            try:
                r = requests.get(entry.get('link'), timeout=30)
            except ConnectionError:
                logger.error("Failed to fetch %s", entry.get('link'))
                continue
            except MissingSchema:
                logger.error("Failed to fetch %s because of missing link.", entry.get('link'))
                continue
            except Timeout:
                logger.error("Timed out while fetching %s", entry.get('link'))
                continue
            # print r.encoding
            try:
                encoding = r.encoding if r.encoding is not None else 'utf8'
                # Articles are first decoded with the declared encoding and then compressed with zlib
                dec_content = r.content.decode(encoding)
                entry['link_content'] = compress_content(dec_content)
                entry['compressed'] = True
                entry['language'] = detect_language(dec_content)
                entry['cleaned_text'] = goosefy(dec_content, entry)
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
                print "could not decode page as ", r.encoding
                continue
            # Turn the tags field into a simple list of tags
            try:
                tag_list = [tag['term'] for tag in entry.tags]
                entry['tags'] = tag_list
            except AttributeError:
                logger.info("This feed has no tags: %s", entry.link)
            try:
                entry.pop('published_parsed')
            except KeyError:
                pass
            #load_into_pypln.load_document(entry, corpus)
            exists = list(ARTICLES.find({"link": entry.link}))
            # print exists
            if not exists:
                if "published" in entry:
                    # consider parsing the string datetime into a datetime object
                    pass
                try:
                    _id = ARTICLES.insert(entry, w=1)
                except DuplicateKeyError:
                    logger.error("Duplicate article found")
                    return
                index_article_on_elastic(entry, _id)
                # print "inserted"

def index_article_on_elastic(doc, _id):
    """
    Index documents in elastic search
    :param doc: document to be indexed
    :param _id: mongodb id
    :return:
    """
    elastic_doc = {
            'index': 'mcdb',
            'doc_type': 'articles',
            'id': int('0x' + str(_id), 16)
        }
    indexed_fields = settings.ELASTIC_ARTICLE_FIELDS
    body = {k: v for k, v in doc.items() if k in indexed_fields}

    elastic_doc['body'] = doc
    es.index(index=elastic_doc['index'],
             doc_type=elastic_doc['doc_type'],
             id=elastic_doc['id'],
             body=body
    )


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
        f = RSSDownload(feed[0], feed[1])
    except InvalidDocument:
        logger.error("This feed failed: %s", f)
    try:
        f.parse()
    except Exception as e:
        logger.error("An error occurred while trying to fetch feed {}: {}".format(f.url, e))


def parallel_fetch():
    """
    Starts parallel threads to fetch feeds.
    """
    feed_count = FEEDS.count()  # Needed for first round of while
    feed_urls = []
    t0 = time.time()
    feeds_scanned = 0
    thread_pool = ThreadPool(config['threads'])
    while feeds_scanned < feed_count:
        feed_cursor = FEEDS.find({}, skip=feeds_scanned, limit=100, sort=[("last_visited", pymongo.DESCENDING),
                                                                          ("updated", pymongo.DESCENDING)])
        for feed in feed_cursor:
            if "updated" in feed:
                try:
                    date = feed["updated"]
                    if not isinstance(date, datetime.datetime):
                        date = parse(date)
                    FEEDS.update({"_id": feed["_id"]}, {"$set": {"updated": date}})
                except ValueError:
                    FEEDS.update({"_id": feed["_id"]}, {"$set": {"updated": datetime.datetime.now()}})
                except Exception as e:
                    logger.exception("Failed to parse updated field with error:"
                            " %s", e)

            feed_title = feed.get('title_detail', feed.get('subtitle_detail', None))
            if feed_title is None:
                logger.error("Feed %s does not contain a title or subtitle ", feed.get('link', None))
                continue
            try:
                feed_urls.append((feed['_id'], feed_title["base"].decode('utf8')))
            except KeyError:
                logger.error("Feed %s does not contain base URL", feed.get('link', None))
            except UnicodeEncodeError:
                logger.error("Feed %s failed Unicode decoding", feed.get('link', None))
            #fetch_feed(t["base"].decode('utf8'))
        if feed_urls:  # Only if there are urls to fetch
            thread_pool.map(fetch_feed, feed_urls)
            feeds_scanned += len(feed_urls)
            logger.info("%s feeds scanned after %s minutes", feeds_scanned, (time.time()-t0)/60.)
            feed_count = FEEDS.count()
    thread_pool.close()
    logger.info("Time taken to download %s feeds: %s minutes.", len(feed_urls), (time.time()-t0)/60.)

if __name__ == "__main__":
    parallel_fetch()
