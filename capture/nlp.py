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

ARTICLES.ensure_index([("pypln_url", pymongo.ASCENDING)], sparse=True)


def get_corpus():
    """
    Return the existing Mediacloud corpus or create it and return.
    """
    corpus_name = settings.CORPUS_NAME
    try:
        article_corpus = pypln.add_corpus(name=corpus_name, description='MediaCloud Articles')
    except RuntimeError:
        article_corpus = [c for c in pypln.corpora() if c.name == corpus_name][0]

    return article_corpus


def send_to_pypln(downloaded_article, corpus):
    """
    Takes a mediacloud document from the articles collection and insert into a pypln corpus.
    """
    if 'cleaned_text' in downloaded_article:
        if len(downloaded_article['cleaned_text']) > 0:
            pypln_document = corpus.add_document(
                    decompress_content(downloaded_article['cleaned_text']))
    elif 'summary' in downloaded_article:
        pypln_document = corpus.add_document(
                decompress_content(downloaded_article['summary']))
    else:
        pypln_document = corpus.add_document(
                decompress_content(downloaded_article['title']))
    return pypln_document


def decompress_content(compressed_html):
    """
    Decompress data compressed by `compress_content`
    :param compressed_html: compressed html document
    :return: original html
    """
    decompressed = zlib.decompress(compressed_html)
    orig_html = CP.loads(decompressed)
    return orig_html
