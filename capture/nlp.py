import pymongo
import requests

import settings


client = pymongo.MongoClient(settings.MONGOHOST, 27017)
MCDB = client.MCDB
FEEDS = MCDB.feeds  # Feed collection
ARTICLES = MCDB.articles  # Article Collection


def send_to_pypln(feed_entry, url='http://demo.pypln.org/documents/',
                  data={'corpus':'http://demo.pypln.org/corpora/26/'},
                  auth=('demo', 'demo')):
    files = {'blob': feed_entry}
    response = requests.post(url, data=data, files=files, auth=auth)
    return response
