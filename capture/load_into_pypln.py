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


def load(skip, limit=0):
    corpus = nlp.get_corpus()
    articles_sent = 0
    filter_ = {'pypln_url': {'$exists': False}}

    find_kwargs = {'sort': [("_id", pymongo.DESCENDING)]}
    if skip:
        find_kwargs.update({'skip': skip})
    if limit:
        find_kwargs.update({'limit': limit})
    if limit == 0:
        count = articles.count()
    else:
        count = limit
    while articles_sent < count:
        cursor = articles.find(filter_, skip=articles_sent, limit=100, **find_kwargs)
        for article in cursor:
            pypln_document = nlp.send_to_pypln(article, corpus)
            _id = article['_id']
            articles.update({'_id': _id},
                            {'$set': {"pypln_url": pypln_document.url}})
            sys.stdout.write('inserted document with id {} into PyPLN\n'.format(_id))
            articles_sent += 1



if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(description=("Load MediaCloud documents"
        "into a PyPLN instance"))

    # parser.add_argument("-c", "--corpus_name", type=str, metavar="NAME",
    # default="MC_articles",
    #         help="Uploads documents to a corpus named NAME")
    parser.add_argument("-l", "--limit", metavar='N', type=int, default=0,
        help="Adds limit=N to the mongo query")
    parser.add_argument("-s", "--skip", metavar='N', type=int, default=0,
        help="Adds skip=N to the mongo query")
    args = parser.parse_args()

    load(args.skip, args.limit)
