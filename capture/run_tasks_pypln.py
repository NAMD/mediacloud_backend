"""
To run the tasks we're using Celery, which is a an asynchronous task queue
based on distributed message passing. You can run the Celery worker by executing
"tasks_pypln" with the worker argument on your terminal:

    celery -A tasks_pypln worker --loglevel=info

"""

import pymongo
import settings
from tasks_pypln import fetch_property
from multiprocessing import Pool



## Media Cloud database setup
client = pymongo.MongoClient(host=settings.MONGOHOST)
articles = client.MCDB.articles # articles collection
pypln_temp = client.MCDB.pypln_temp # pypln_temp temporary collection
articles_analysis = client.MCDB.articles_analysis # articles_analysis collection


def send_to_queue(article):
    pypln_temp.update({'_id': article['_id']},
                      {'$set': {'status': 'on_queue'}})
    fetch_property.delay(article['_id'])


def get_pypln_properties():
    articles_fetch = 0
    filter_ = {'status': {'$nin': ['analysis_complete', 'on_queue', 'analysis complete']}}

    count = pypln_temp.count()
    cursor = pypln_temp.find(filter_, limit=10000)
    P = Pool()
    while articles_fetch < count:
        P.map(send_to_queue, (article for article in cursor))
        articles_fetch += 10000
        cursor = pypln_temp.find(filter_, limit=10000)
    P.close()
    P.join()

if __name__ == "__main__":
    get_pypln_properties()
