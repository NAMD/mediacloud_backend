#coding:utf8
import logging

import oauth2 as oauth
from pymongo import MongoClient
import twitter

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


oauth_token = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)

TWstream = twitter.TwitterStream(auth=twitter.OAuth(access_token_key,
                            access_token_secret,consumer_key,consumer_secret))
iterator = TWstream.statuses.sample()



def capture():
    for tweet in iterator:
        try:
            if not tweet['lang'].startswith('pt'):
                continue
            if not tweet.get('text'):
                continue
            #print (tweet['text'])
            coll.insert(tweet, w=1)
        except twitter.TwitterError(420):
            logging.warning('Error 420: Rate limit problem')
        except KeyError as e:
            logging.error("Invalid Tweet: %s" % e)


if __name__ == '__main__':

    logging.info("Started running")
    logging.info('Size: {} tweets' .format(size))
    capture()
    logging.critical("Stopped running")
