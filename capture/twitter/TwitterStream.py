#!/usr/bin/env python
#coding:utf8

import logging

from pymongo import MongoClient
import tweepy
from tweepy.streaming import StreamListener

import config


FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='/tmp/twitterstream.log', format=FORMAT, level=logging.DEBUG)

mongo_host = config.MONGO_HOST if config.MONGO_HOST else 'localhost'

client = MongoClient(mongo_host)
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


#
# def capture(twiterator):
#     for tweet in twiterator:
#         try:
#             if not tweet['lang'].startswith('pt'):
#                 continue
#             if not tweet.get('text'):
#                 continue
#             #print (tweet['text'])
#             coll.insert(tweet, w=1)
#         except twitter.TwitterError(420):
#             logging.warning('Error 420: Rate limit problem')
#         except KeyError as e:
#             logging.error("Invalid Tweet: %s" % e)


class Filteredcapture(StreamListener):
    """ A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        print data
        coll.insert(data, w=1)
        return True

    def on_error(self, status):
        print status
        logging.error("Invalid Tweet: %s" % status)


if __name__ == '__main__':
    listener = Filteredcapture()
    stream = tweepy.Stream(auth, listener)
    stream.filter(track=config.TRACK, languages=['pt'])

    # logging.info("Started running")
    # capture(sample_iterator)
    # logging.critical("Stopped running")
