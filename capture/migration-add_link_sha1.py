import hashlib
import logging
import pymongo
import sys

import settings


# This is not defined in config.py because this log is specific to this tool,
# and this should be run only once per database.

LOG_FILE = '/tmp/add_link_sha1_migration.log'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=logging.INFO)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger = logging.getLogger()
logger.addHandler(stdout_handler)

mongo_client = pymongo.MongoClient(settings.MONGOHOST)
collection = mongo_client.MCDB.articles

cursor = collection.find({'link_sha1': {'$exists': False}},
        timeout=False)

total = cursor.count()
if total == 0:
    logging.info('There are no articles to update \o/')
    sys.exit(0)

logging.info("We have {:d} articles to update.".format(total))
i = 0
for article in cursor:
    try:
        link = article['link']
        # Some links are lists? This happened in my dev db so I'll treat it
        # here.
        if isinstance(link, list):
            logger.exception('_id: %s has a list as link. Getting first one.',
                    article['_id'])
            link = link[0]
        sha1 = hashlib.sha1(link).hexdigest()
        collection.update({'_id': article['_id']}, {'$set':
            {'link_sha1': sha1}})

    except Exception as e:
        logger.exception('_id: %s [Exception] %s', article['_id'], e)
    i += 1
    if (i % 1000) == 0:
        logging.info('{:010d}/{:010d} articles updated.'.format(i, total))

cursor.close()
