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

def geoloc_tweet(_id):
    """
    Attempts to geolocate a tweet specified by `_id`
    """
    tweet = coll.find({"_id": _id})[0]
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


def save_tweet_as_geojson(_id, coords):
    """
    Saves the tweets as GeoJSON in MongoDb
    """
    tweet = coll.find({"_id": _id})[0]
    if coords is None:
        return
    twgj = {"type": "Feature",
            "geometry": {"type": "Point", "coordinates": list(coords)},
            "properties": {}
      }
    datet = parser.parse(tweet["created_at"])
    hts = [] if not tweet["entities"]["hashtags"] else tweet["entities"]["hashtags"][0]["text"]
    props = {"text": tweet['text'],
             "retweeted": tweet["retweeted"],
             "coordinates": tweet["coordinates"],
             "hashtags": hts,
             "lang": tweet["lang"],
             "date": datet,
            }
    twgj["properties"] = props
    geoj.insert({"originalID": ObjectId(tweet["_id"]),
                 "geoJSONproperty": twgj, }, w=1)
    data["features"].append(twgj)

def process_tweet():
    try:
        ids_to_process = list(coll.find({'geolocated': {'$exists': False}}, fields=["_id"]))
        for _id in ids_to_process:
            c = geoloc_tweet(_id)
            if c is None:
                coll.update({'_id': _id}, {'$set': {'geolocated': False}})
            save_tweet_as_geojson(_id, c)
            coll.update({'_id': _id}, {'$set': {'geolocated': True}})
            #print _id, c
    except OperationFailure:
        logging.error("operationFailure")

if __name__ == "__main__":

    size = geoj.find({{'geochecked': {'$exists': False}}}).count()
    logging.info("%s tweets left to geolocate", size)
    process_tweet()
    current_size = geoj.count()
    variation = current_size - size
    logging.critical('End of the analysis, total of {} geolocated tweets, {} new geolocated tweets' .format(size, variation))


