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

# makes sure the tweets are indexed by date
coll.ensure_index('created_at_datetime')

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
        try:
            parsed_data['created_at_datetime'] = dateutil.parser.parse(parsed_data['created_at'])
            coll.insert(parsed_data, w=1)
        except KeyError as exc:
            logging.warn("Tweet without 'created_at': %s", parsed_data, exc_info=exc)
        return True

    def on_error(self, status):
        logging.error("Invalid response from twitter api: %s" % status)


if __name__ == '__main__':
    listener = Filteredcapture()
    logging.info('Connecting to twitter API...')
    stream = tweepy.Stream(auth, listener)
    logging.info('... connected.')
    try:
        stream.filter(track=config.TRACK, languages=['pt'])
    except Exception as e:
        logging.exception('Error during stream capture: %s', e)
