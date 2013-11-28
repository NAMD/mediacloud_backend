# -*- coding: utf-8 -*-
import logging

from pymongo import MongoClient
from pymongo.errors import OperationFailure
from bson.objectid import ObjectId
from geopy import geocoders
from geopy.geocoders.googlev3 import GQueryError, GTooManyQueriesError
from dateutil import parser


FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='tweet_geoloc.log',format = FORMAT, level=logging.DEBUG)

# Initialize connection
client = MongoClient()
mcdb = client.MCDB
coll = mcdb.tweets
geoj = mcdb.geotweet
gg = geocoders.GoogleV3()
gn = geocoders.GeoNames()

data = { "type": "FeatureCollection",
  "features": []
   }

def geoloc_tweet(tweet):
    """
    Tweet is a dictionary representing the full tweet record
    """
    try:
        if tweet['place'] is not None:
            fullgeo = tweet[u'place'][u'full_name']
            lat, lon = fetch_loc(fullgeo)
            return lat, lon
    except KeyError:
        logging.info('keyError: %s', 'place')
    try:
        if tweet['geo'] is not None:
            lat, lon = tweet['geo']['coordinates']
            return lat, lon
    except KeyError:
        logging.info('keyError: %s', 'geo')
    try:
        if u'user' in tweet:
            if u'location' in tweet[u'user']:
                fullgeo = tweet[u'user'][u'location']
                lat, lon = fetch_loc(fullgeo)
                return lat, lon
    except KeyError:
        logging.info('keyError: %s', u'user')
    return



def fetch_loc(location):
    lat, lon = None, None
    try:
        lat, lon = gg.geocode(location, exactly_one=False)[0][-1]
    except GQueryError:
        try:
            lat, lon = gn.geocode(location, exactly_one=False)[0][-1]
            logging.info("fetched %s from Geonames instead" % ((lat, lon)))
        except TypeError:
            logging.info("%s not found by Google" % location)
    except GTooManyQueriesError:
        try:
            lat, lon = gn.geocode(location, exactly_one=False)[0][-1]
            logging.info("fetched %s from Geonames instead" % ((lat, lon)))
        except TypeError:
            logging.info("%s not found by Geonames" % location)
    return lat, lon


def save_tweet_as_geojson(tw, coords):
    """
    Saves the tweets as GeoJSON in MongoDb
    """
    if coords is None:
        return
    twgj = {"type": "Feature",
            "geometry": {"type": "Point", "coordinates": list(coords)},
            "properties": {}
      }
    datet = parser.parse(tw["created_at"])
    hts = [] if not tw["entities"]["hashtags"] else tw["entities"]["hashtags"][0]["text"]
    props = {"text": tw['text'],
             "retweeted": tw["retweeted"],
             "coordinates": tw["coordinates"],
             "hashtags": hts,
             "lang": tw["lang"],
             "date": datet,
        }
    twgj["properties"] = props
    geoj.insert({"originalID": ObjectId(tw["_id"]),
                 "geoJSONproperty": twgj,}, safe=True)
    data["features"].append(twgj)

def process_tweet():
    try:
        for tweet in coll.find({'geochecked': {'$exists' : 0}}):
            c = geoloc_tweet(tweet)
            save_tweet_as_geojson(tweet, c)
            coll.update({'_id': tweet['_id']}, {'$set': {'geochecked': 1}})
            print tweet['_id'], c
    except OperationFailure:
        logging.error("operationFailure")

if __name__ == "__main__":

    size = geoj.count()
    logging.info("Started running")
    process_tweet()
    current_size = geoj.count()
    variation = current_size - size
    logging.critical('End of the analysis, total of {} geolocated tweets, {} new geolocated tweets' .format(size, variation))


