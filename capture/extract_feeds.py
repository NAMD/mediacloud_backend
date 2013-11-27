#!/usr/bin/env python
#-*- coding:utf-8 -*-
u"""
Created on 26/09/13
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import argparse
import logging

import pymongo
from pymongo.errors import OperationFailure

import feedfinder
import urlscanner
import settings


###########################
#  Setting up Logging
###########################
logger = logging.getLogger("Extract_feeds")
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#fh = RotatingFileHandler('/tmp/mediacloud.log', maxBytes=5e6, backupCount=3)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
#fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)  # uncomment for console output of messages
#logger.addHandler(fh)

## Media Cloud database setup
client = pymongo.MongoClient(settings.MONGOHOST, 27017)
MCDB = client.MCDB
URLS = MCDB.urls  # Feed collection


def main(urls, depth):
    if urls:
        with open(urls) as f:
            for u in f:
                print "scanning {} with depth {}".format(u, depth)
                scan_url(u, depth)
    else:  # Scan URLs from Mongodb url collection
        urls_count = URLS.count()
        urls_scanned = 0
        while urls_scanned < urls_count:
            cursor = URLS.find({}, skip=urls_scanned, limit=100, sort=[("_id", pymongo.DESCENDING)])
            try:
                for doc in cursor:
                    print "scanning {} with depth {}".format(doc['url'], depth)
                    scan_url(doc['url'], depth)
                    urls_scanned += 1
            except OperationFailure as e:
                logger.error("Mongodb Operation failure: %s", e)


def scan_url(url, depth):
    u2 = urlscanner.url_scanner(url.strip(), depth)
    for U in u2:
        logger.info("searching for feeds in: %s", U)
        feeds = feedfinder.feeds(U.strip())
        logger.info("found %s feeds", len(feeds))
        if feeds:
            logger.info(str(feeds))
            feedfinder.store_feeds(feeds)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search for feeds on a set of web pages (urls)')
    parser.add_argument('-f', '--file', type=str, default='', help='file with one or more urls to check (one per line)')
    parser.add_argument('-d', '--depth', type=int, default=2, help='Depth of the search, from the initial url')

    args = parser.parse_args()
    # print args.file
    main(args.file, args.depth)
