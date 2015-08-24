"""
To run the tasks we're using Celery, which is a an asynchronous task queue
based on distributed message passing. You can run the Celery worker by executing
"tasks_pypln" with the worker argument on your terminal:

    celery -A tasks_pypln worker --loglevel=info

"""

from celery import Celery
import pypln.api
import pymongo
import settings
from tasks_pypln import fetch_property
from celery import group


## Media Cloud database setup
client = pymongo.MongoClient(host=settings.MONGOHOST)
articles = client.MCDB.articles # articles collection
pypln_temp = client.MCDB.pypln_temp # pypln_temp temporary collection
articles_analysis = client.MCDB.articles_analysis # articles_analysis collection


def get_pypln_properties(doc_id):
    article = pypln_temp.find_one({'_id': doc_id})
    articles_analysis.insert({'articles_id': article['articles_id']})

    results = fetch_property.delay(doc_id)

    pypln_temp.remove({'_id': doc_id})



for article in pypln_temp.find():
    doc_id = article['_id']
    get_pypln_properties(doc_id)
