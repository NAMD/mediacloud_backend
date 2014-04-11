#-*- coding:utf-8 -*-
u"""
Created on 03/04/14
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import sys

import pymongo

import nlp
import settings


client = pymongo.MongoClient(host=settings.MONGOHOST)
articles = client.MCDB.articles



def load(corpus_name='MC_articles'):
    corpus = nlp.get_corpus(corpus_name) #XXX: This is not needed
    article_count = articles.count()
    art_loaded = 0
    while art_loaded < article_count:
        cursor = articles.find({}, skip=art_loaded, limit=100, sort=[("_id", pymongo.DESCENDING)])
        to_insert = cursor.count()
        for article in cursor:
            nlp.send_to_pypln(article, corpus_name)
        art_loaded += to_insert


if __name__=="__main__":
    if len(sys.argv) > 1:
        print "loading documents into corpus '{}'".format(sys.argv[1])
        load(sys.argv[1])
    else:
        load()
