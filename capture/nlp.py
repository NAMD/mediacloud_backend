#!/usr/bin/env python
#coding:utf8
"""
This Module handles the pushing of mediacloud articles into a PyPLN corpus.
"""

import zlib
import cPickle as CP

import pymongo
from pypln.api import PyPLN

import settings


client = pymongo.MongoClient(settings.MONGOHOST, 27017)
MCDB = client.MCDB
FEEDS = MCDB.feeds  # Feed collection
ARTICLES = MCDB.articles  # Article Collection
pypln = PyPLN(settings.PYPLNHOST, settings.PYPLN_CREDENTIALS)

# Add an article corpus
article_corpus = pypln.add_corpus(name='MC_articles', description='MediaCloud Articles')


def send_to_pypln(document):
    """
    Takes a mediacloud document from the articles collection and insert into a pypln corpus.
    """
    article = article_corpus.add_document(decompress_content(document['link_content']))


def decompress_content(compressed_html):
    """
    Decompress data compressed by `compress_content`
    :param compressed_html: compressed html document
    :return: original html
    """
    decompressed = zlib.decompress(compressed_html)
    orig_html = CP.loads(decompressed)
    return orig_html
