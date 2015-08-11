from celery import Celery
import pypln.api
import pymongo
import settings


## Media Cloud database setup
client = pymongo.MongoClient(host=settings.MONGOHOST)
articles = client.MCDB.articles # articles collection
pypln_temp = client.MCDB.pypln_temp # pypln_temp temporary collection
articles_analysis = client.MCDB.articles_analysis # articles_analysis collection


app = Celery('tasks', backend="mongodb")

@app.task(bind=True)
def fetch_property(self, _id, property_name):
    article = pypln_temp.find_one({"_id": _id})

    pypln_document = pypln.api.Document.from_url(article["pypln_url"],
                                                 settings.PYPLN_CREDENTIALS)

    try:
        value = pypln_document.get_property(property_name)
    except RuntimeError as exc:
        raise self.retry(exc=exc)

    articles_analysis.update({"articles_id": article["articles_id"]},
                             {"$set": {property_name: value}})

    return value
