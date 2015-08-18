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
    article = pypln_temp.find({'_id': doc_id})
    articles_analysis.insert({'articles_id': article['articles_id']})

    results = fetch_property.delay(doc_id)

    if results.successful() is True:
        articles.update({'_id': article['articles_id']}, {'$set': {'status': 1}})
    else:
        articles.update({'_id': article['articles_id']}, {'$set': {'status': 2}})

    pypln_temp.remove({'_id': doc_id})



for article in pypln_temp.find():
    doc_id = article['_id']
    get_pypln_properties(doc_id)
