#-*- coding:utf-8 -*-
u"""
Created on 04/02/14
by fccoelho
license: GPL V3 or Later

Script to index an entire mongo collection in batches into Solr
"""

import os
import sys
import zlib
import cPickle as CP
import time

import pymongo

from solr_doc_manager import DocManager


sys.path.append('/'.join(os.getcwd().split("/")[:-1]))
from capture import settings


class Indexer(object):
    """
    Indexes a Mongodb Collection in batches to speed it up.
    """
    def __init__(self, url, core, collection):
        """
        """
        # self.solr = Solr(os.path.join(url, core))
        self.collection  = collection
        self.doc_manager = DocManager(os.path.join(url, core))

    def start(self, batchsize=100):
        """
        Starts the indexing
        """
        num_docs = self.collection.count()
        t0 = time.time()
        for i in range(0, num_docs, batchsize):
            cur = self.collection.find({}, skip=i, limit=batchsize, sort=[("_id", pymongo.ASCENDING)])
            try:
                self.doc_manager.bulk_upsert(list(cur))
            except Exception as e:
                print e
            print "indexed {} of {}".format(max(i+num_docs, num_docs), num_docs)
        print "Indexed {} documents per second.".format(num_docs/(time.time() - t0))

    def decompress(self, doc):
        """
        Decompresses and encodes HTML content
        """
        # Decompress the content of the article before sending to Solr
        doc["link_content"] = decompress_content(doc["link_content"]).encode('utf8')
        return doc


def decompress_content(compressed_html):
    """
    Decompress data compressed by `compress_content`
    :param compressed_html: compressed html document
    :return: original html
    """
    decompressed = zlib.decompress(compressed_html)
    orig_html = CP.loads(decompressed)
    return orig_html

if __name__ == "__main__":
    conn = pymongo.MongoClient(settings.MONGOHOST)
    article_indexer = Indexer(settings.SOLR_URL, "mediacloud_articles", conn.MCDB.articles)
    feed_indexer = Indexer(settings.SOLR_URL, "mediacloud_feeds", conn.MCDB.feeds)
    article_indexer.start(200)
    feed_indexer.start(200)


