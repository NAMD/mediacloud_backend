#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import dateutil.parser
import logging
import pymongo
import sys

import config


# This is not defined in config.py because this log is specific to this tool,
# and this should be run only once per database.

LOG_FILE = '/tmp/tweet_timestamp_migration.log'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=logging.INFO)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger = logging.getLogger()
logger.addHandler(stdout_handler)

mongo_client = pymongo.MongoClient(config.MONGO_HOST)
collection = mongo_client.MCDB.tweets

# timeout=False should make sure the cursor is not closed after 10min
# http://api.mongodb.org/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
cursor = collection.find({'created_at_timestamp': {'$exists': False}},
        timeout=False)

total = cursor.count()
if total == 0:
    logging.info('There are no tweets to update \o/')
    sys.exit(0)

i = 0
for tweet in cursor:
    try:
        timestamp = dateutil.parser.parse(tweet['created_at']).strftime('%s')
        collection.update({'_id': tweet['_id']}, {'$set':
            {'created_at_timestamp': timestamp}})
    except Exception as e:
        logger.exception('_id: %s [Exception] %s', tweet['_id'], e)
    i += 1
    if (i % 1000) == 0:
        logging.info('{:010d}/{:010d} tweets updated.'.format(i, total))

cursor.close()
