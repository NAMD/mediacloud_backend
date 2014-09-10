#!/usr/bin/env python
#coding:utf8

import dateutil.parser
import logging
import json

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import tweepy
from tweepy.streaming import StreamListener

import config


FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='/tmp/twitterstream.log', format=FORMAT, level=logging.DEBUG)

mongo_host = config.MONGO_HOST if config.MONGO_HOST else 'localhost'

try:
    client = MongoClient(mongo_host)
except ConnectionFailure:
    client = MongoClient('localhost')
mcdb = client.MCDB
coll = mcdb.tweets
# credentials
access_token_key = config.access_token_key
access_token_secret = config.access_token_secret
consumer_key = config.consumer_key
consumer_secret = config.consumer_secret
#############
# Tweepy setup
#############
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token_key, access_token_secret)
# Construct the API instance
api = tweepy.API(auth)
##################


class Filteredcapture(StreamListener):
    """
    This listener will get tweets received from the stream, parse their creation
    time, add that as a timestamp to the data (so that it can be used for
    ordering and formatted in different ways) and save the tweet to a mongodb
    collection.
    """
    def on_data(self, data):
        parsed_data = json.loads(data)
        parsed_data['created_at_timestamp'] = dateutil.parser.parse(parsed_data['created_at']).strftime('%s')
        coll.insert(parsed_data, w=1)
        return True

    def on_error(self, status):
        logging.error("Invalid Tweet: %s" % status)


if __name__ == '__main__':
    listener = Filteredcapture()
    stream = tweepy.Stream(auth, listener)
    stream.filter(track=config.TRACK, languages=['pt'])

