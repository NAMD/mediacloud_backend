#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import dateutil.parser
import pymongo
import sys

import config

mongo_client = pymongo.MongoClient(config.MONGO_HOST)
collection = mongo_client.MCDB.tweets

cursor = collection.find({'created_at_timestamp': {'$exists': False}})

total = cursor.count()
if total == 0:
    sys.stdout.write('There are no tweets to update \o/\n')
    sys.exit(0)

i = 0
for tweet in cursor:
    timestamp = dateutil.parser.parse(tweet['created_at']).strftime('%s')
    collection.update({'_id': tweet['_id']}, {'$set':
        {'created_at_timestamp': timestamp}})
    i += 1
    if (i % 1000) == 0:
        sys.stdout.write('{:010d}/{:010d} tweets updated\n'.format(i, total))
