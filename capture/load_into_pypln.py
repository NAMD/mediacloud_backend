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

import logging
from logging.handlers import RotatingFileHandler

import nlp
import settings
from threading import Thread
from pypln.api import Document


###########################
#  Setting up Logging
###########################
logger = logging.getLogger("load_into_pypln")
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# logger.addHandler(ch)  # uncomment for console output of messages
logger.addHandler(fh)


## Media Cloud database setup
client = pymongo.MongoClient(host=settings.MONGOHOST)
articles = client.MCDB.articles # articles collection
pypln_temp = client.MCDB.pypln_temp # pypln_temp temporary collection
articles_analysis = client.MCDB.articles_analysis # articles_analysis collection


Done = False

def load(skip, limit=0):
    global Done
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
    Done = True


def search_pypln():
    global Done
    cursor = pypln_temp.find(timeout=False)

    while (not Done) and pypln_temp.count > 0:
        for article in cursor:
            my_doc = Document.from_url(article['pypln_url'], ('sendpypln','123'))
            _id = article['articles_id']
            _id_temp = article['_id']

            if '_exception' in my_doc.properties:
                logger.warning("PyPLN found an error {}".format(article['pypln_url']))
                articles.update({'_id': _id}, {'$set': {'status': 2}})
                pypln_temp.remove({'_id': _id_temp})
                continue

            if len(my_doc.properties) < 22:
                if 'time' in article:
                    if (datetime.datetime.now() - article['time']).seconds/60 > 5:
                        logger.warning("PyPLN could not finish the analysis {}".format(article['pypln_url']))
                        articles.update({'_id': _id}, {'$set': {'status': 2}})
                        pypln_temp.remove({'_id': _id_temp})
                    else:
                        continue
                else:
                    pypln_temp.update({'_id': _id_temp}, {'$set': {'time': datetime.datetime.now()}})

            else:
                analysis = {'articles_id': _id}
                for property in my_doc.properties:
                    analysis[property] = my_doc.get_property(property)

                articles_analysis.insert(analysis)
                articles.update({'_id': _id}, {'$set': {'status': 1}})
                pypln_temp.remove({'_id': _id_temp})

    cursor.close()





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

    t = Thread(target=search_pypln)
    t.start()
    load(args.skip, args.limit)
    t.join()