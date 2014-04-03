#-*- coding:utf-8 -*-
u"""
Created on 03/04/14
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import pymongo

import nlp
import settings


client = pymongo.MongoClient(host=settings.MONGOHOST)

articles = client.MCDB.articles
corpus = nlp.get_corpus()

article_count = articles.count()
art_loaded = 0
while art_loaded < article_count:
    cursor = articles.find({}, limit=100)
    for doc in cursor:
        corpus.add_document(doc['link_content'])
