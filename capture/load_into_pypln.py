#!/usr/bin/env python
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



def load(corpus_name, skip, limit):
    corpus = nlp.get_corpus(corpus_name)

    filter_ = {'pypln_url': {'$exists': False}}

    find_kwargs = {'sort': [("_id", pymongo.DESCENDING)]}
    if skip:
        find_kwargs.update({'skip': skip})
    if limit:
        find_kwargs.update({'limit': limit})

    cursor = articles.find(filter_, **find_kwargs)
    for article in cursor:
        pypln_document = nlp.send_to_pypln(article, corpus)
        _id = article['_id']
        articles.update({'_id': _id},
                        {'$set': {"pypln_url": pypln_document.url}})
        sys.stdout.write('inserted document with id {} into PyPLN\n'.format(_id))


if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(description=("Load MediaCloud documents"
        "into a PyPLN instance"))

    parser.add_argument("-c", "--corpus_name", type=str, metavar="NAME",
            default="MC_articles",
            help="Uploads documents to a corpus named NAME")
    parser.add_argument("-l", "--limit", metavar='N', type=int, default=0,
        help="Adds limit=N to the mongo query")
    parser.add_argument("-s", "--skip", metavar='N', type=int, default=0,
        help="Adds skip=N to the mongo query")
    args = parser.parse_args()

    load(args.corpus_name, args.skip, args.limit)
