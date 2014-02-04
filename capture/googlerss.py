#!/usr/bin/env python
"""
Fetches all urls in a google query for RSS feeds in Brasil
"""
__author__ = 'fccoelho'

from urlparse import unquote
import argparse
import datetime
import time

import pymongo
from pymongo.errors import DuplicateKeyError

import GoogleScraper
import settings


##### Setup URL Collection ############
client = pymongo.MongoClient(settings.MONGOHOST, 27017)
MCDB = client.MCDB
URLS = MCDB.urls  # Collection of urls to extract feeds from
URLS.ensure_index('url', unique=True)
###########


def main(subject='', n=5):
    """
    Scrape google search up to the nth page and save the results to a MongoDB collection.
    :param n:
    """
    q = "{}+RSS+site:br".format(subject)
    for o in range(0, n*10, n):
        urls = GoogleScraper.scrape(q, number_pages=n, offset=o, language='lang_pt')
        for url in urls:
            # You can access all parts of the search results like that
            # url.scheme => URL scheme specifier (Ex: 'http')
            # url.netloc => Network location part (Ex: 'www.python.org')
            # url.path => URL scheme specifier (Ex: ''help/Python.html'')
            # url.params => Parameters for last path element
            # url.query => Query component
            #print url
            #print(unquote(url.geturl()))
            try:
                U = unquote(url.geturl()).split("&")[0]#sa=U&ei=")[0]  # Remove googlebot crap
                URLS.insert({'url': U, 'tags': [subject], 'fetched_on': datetime.datetime.now()})
            except DuplicateKeyError:
                pass
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search for urls listing RSS feeds on with google')
    parser.add_argument('-s', '--subject', type=str, default='', help='subject of the FEEDS')
    args = parser.parse_args()
    main(args.subject)
