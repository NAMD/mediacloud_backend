__author__ = 'elisa'


import pymongo
import datetime

import logging
from logging.handlers import RotatingFileHandler

import settings
from pypln.api import Document

###########################
#  Setting up Logging
###########################
logger = logging.getLogger("load_into_pypln")
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
fh = RotatingFileHandler('/tmp/search_pypln.log', maxBytes=5e6, backupCount=3)
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


def main():
    cursor = pypln_temp.find()

    while pypln_temp.count() > 0:
        for article in cursor:
            try:
                url = article['pypln_url']
                my_doc = Document.from_url(url, settings.PYPLN_CREDENTIALS)
                _id = article['articles_id']
                _id_temp = article['_id']
            except RuntimeError as e:
                logger.error("The document {} could not be found on the PyPLN collection: {}".format(url, e))
                continue

            if '_exception' in my_doc.properties:
                logger.warning("PyPLN found an error {}".format(article['pypln_url']))
                articles.update({'_id': _id}, {'$set': {'status': 2}})
                pypln_temp.remove({'_id': _id_temp})
                continue

            if len(my_doc.properties) < 29:
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
        cursor = pypln_temp.find()
    cursor.close()

if __name__ == "__main__":
    main()
