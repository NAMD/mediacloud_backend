# -*- coding:utf-8 -*-
u"""
Created on 03/04/14
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import sys

import pymongo
import datetime

import nlp
import settings
import thread


client = pymongo.MongoClient(host=settings.MONGOHOST)
articles = client.MCDB.articles
pypln_temp = client.MCDB.pypln_temp
articles_analysis = client.MCDB.articles_analysis

def load(skip, limit=0):
    corpus = nlp.get_corpus()
    articles_sent = 0
    filter_ = {'status': {'$exists': False}}

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

            pypln_temp.insert({'pypln_url': pypln_document.url, 'articles_id': _id})

            articles.update({'_id': _id},
                            {'$set': {"status": 0}})

            sys.stdout.write('inserted document {} of {}, with id {} into PyPLN\n'.format(articles_sent, count, _id))
            articles_sent += 1


def search_pypln():
    cursor = pypln_temp.find()

    while cursor.count > 0:
        for article in cursor:
            my_doc = Document.from_url(article['pypln_url'], ('sendpypln','123'))
            _id = article['articles_id']

            if '_exception' in my_doc.properties:
                LOG INFO
                articles.update({'_id': _id}, {'$set': {'status': 2}})
            elif len(my_doc.properties) < 22:
                if 'time' in article:
                    if (datetime.datetime.now() - article['time']).seconds/60 > 5:
                        LOG INFO
                        articles.update({'_id': _id}, {'$set': {'status': 2}})
                    else:
                        continue
                else:
                    pypln_temp.update({'_id': article['_id']}, {'$set': {'time': datetime.datetime.now()}})

            else:
                for property in my_doc.properties:
                    p = my_doc.get_property(property)
                    articles_analysis.update({'articles_id': _id}, {'$set': {property: p}}, {'upsert': True})
                articles.update({'_id': _id}, {'$set': {'status': 1}})

        cursor = pypln_temp.find()




if __name__ == "__main__":
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

    thread.start_new_thread(search_pypln)
