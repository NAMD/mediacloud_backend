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
    corpus = nlp.get_corpus(corpus_name)
    article_count = articles.count()
    art_loaded = 0
    while art_loaded < article_count:
        cursor = articles.find({}, skip=art_loaded, limit=100, sort=[("_id", pymongo.DESCENDING)])
        for art in cursor:
            document = corpus.add_document(art['link_content'])
            articles.update({'_id': art['_id']},
                            {'$set': {"pypln_url": document.url}})
            art_loaded += 1


if __name__=="__main__":
    if len(sys.argv) > 1:
        print "loading documents into copus '{}'".format(sys.argv[1])
        load(sys.argv[1])
    else:
        load()
