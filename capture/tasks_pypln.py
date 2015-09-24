from celery import Celery
import pypln.api
import pymongo
import settings
from requests import ConnectionError

## Media Cloud database setup
client = pymongo.MongoClient(host=settings.MONGOHOST)
articles = client.MCDB.articles # articles collection
pypln_temp = client.MCDB.pypln_temp # pypln_temp temporary collection
articles_analysis = client.MCDB.articles_analysis # articles_analysis collection


app = Celery('tasks', backend=settings.CELERY_RESULT_BACKEND)

@app.task(bind=True)
def fetch_property(self, _id):
    article = pypln_temp.find_one({"_id": _id})

    pypln_document = pypln.api.Document.from_url(article["pypln_url"],
                                                 settings.PYPLN_CREDENTIALS)

    try:
        properties = pypln_document.get_property("all_data")
    except (RuntimeError, ConnectionError) as exc:
        raise self.retry(exc=exc)

    # Check the properties dict to know if PyPLn has finished the analysis.

    palavras_ran = properties['palavras_raw_ran']
    if palavras_ran == True and len(properties) == 28:
        doc_status = 'analysis_complete'
    elif palavras_ran == False and len(properties) == 22:
        doc_status = 'analysis_complete'
    else:
        doc_status = 'analysis_running'

    articles_analysis.update({"articles_id": article["articles_id"]},
                             {"$set": {'properties': properties}}, upsert=True)

    pypln_temp.update({"articles_id": article["articles_id"]},
                      {"$set": {'status': doc_status}})
