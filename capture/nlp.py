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


def get_corpus(corpus_name='MC_articles'):
    """
    Return the existing Mediacloud corpus or create it and return.
    """
    try:
        article_corpus = pypln.add_corpus(name=corpus_name, description='MediaCloud Articles')
    except RuntimeError:
        article_corpus = [c for c in pypln.corpora() if c.name == corpus_name][0]

    return article_corpus


def send_to_pypln(downloaded_article, corpus):
    """
    Takes a mediacloud document from the articles collection and insert into a pypln corpus.
    """
    if len(downloaded_article.get('cleaned_text','')) > 0:
        pypln_document = corpus.add_document(downloaded_article['cleaned_text'])
    elif len(downloaded_article.get('summary','')) > 0:
        pypln_document = corpus.add_document(downloaded_article['summary'])
    else:
        pypln_document = corpus.add_document(downloaded_article['title'])
    return pypln_document

