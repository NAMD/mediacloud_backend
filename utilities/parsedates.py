#-*- coding:utf-8 -*-
"""
This module helps migrating dates from strings to date objetcs in the article collection in MDCB
Created on 23/10/13
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

from dateutil.parser import parse
import datetime
import pymongo
import sys


def parsedates(collection):
    for doc in collection.find():
        if not "published" in doc:
            continue
        if not isinstance(doc['published'], datetime.datetime):
            print "updating %s"%doc["_id"]
            collection.update({"_id": doc["_id"]}, {"%set": {"published": parse(doc['published']),
                                                            "updated": parse(doc['updated'])}})

if __name__=="__main__":
    ## Media Cloud database setup
    client = pymongo.MongoClient(sys.argv[1])
    MCDB = client.MCDB
    ARTICLES = MCDB.articles  # Article collection
    parsedates(ARTICLES)
