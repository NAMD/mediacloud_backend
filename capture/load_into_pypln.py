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
    #article_count = articles.count()
    article_count = 10
    art_loaded = 0
    while art_loaded < article_count:
        cursor = articles.find({'pypln_url': {'$exists': False}},
                skip=art_loaded, limit=5, sort=[("_id", pymongo.DESCENDING)])
        for article in cursor:
            pypln_document = nlp.send_to_pypln(article, corpus)
            _id = article['_id']
            articles.update({'_id': _id},
                            {'$set': {"pypln_url": pypln_document.url}})
            # getting `cursor.count()` did not work. Even with
            # `with_limit_and_skip=True`, so we just count the loaded articles.
            art_loaded += 1
            sys.stdout.write(('inserted document with id {} into'
                    'PyPLN\n').format(_id))



if __name__=="__main__":
    if len(sys.argv) > 1:
        print "loading documents into corpus '{}'".format(sys.argv[1])
        load(sys.argv[1])
    else:
        load()
