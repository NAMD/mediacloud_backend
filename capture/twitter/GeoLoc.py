# -*- coding: utf-8 -*-
import logging

from pymongo import MongoClient
from pymongo.errors import OperationFailure
from bson.objectid import ObjectId
from geopy import geocoders
from geopy.geocoders.googlev3 import GQueryError, GTooManyQueriesError
from dateutil import parser


FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='tweet_geoloc.log', format=FORMAT, level=logging.DEBUG)

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
    tweet = coll.find_one({"_id": _id})
    
    #I changed the tries' order because search for "geo" is considerably faster since fetch_loc is not called.
    #furthermore, i had not found any tweet with 'geo'but without 'place'.

    if tweet['geo'] is not None and 'coordinates' in tweet['geo']:
        lat, lon = tweet['geo']['coordinates']
        #print tweet["_id"], "geo"
        return lat, lon
    elif tweet['place'] is not None and 'full_name' in tweet['place']:
        fullgeo = tweet[u'place'][u'full_name']
        lat, lon = fetch_loc(fullgeo)
        #print tweet["_id"], "place"
        return lat, lon
    elif u'user' in tweet and u'location' in tweet[u'user']:
        fullgeo = tweet[u'user'][u'location']
        lat, lon = fetch_loc(fullgeo)
        #print tweet["_id"], "user location"
        return lat, lon
    else:
        logging.info("No geolocation possible.")



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


# add the same dafault parameters for the functions:  save_tweet_as_geojson e process_tweet
def save_tweet_as_geojson(_id, coords, db_source=coll, db_location=geoj):
    """
    Saves the tweets as GeoJSON in MongoDb
    """
    tweet = db_source.find_one({"_id": _id})
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
    db_location.insert({"originalID": ObjectId(tweet["_id"]),
                 "geoJSONproperty": twgj, }, w=1)
    data["features"].append(twgj)


def process_tweet(db_source = coll):
    try:
        ids_to_process = list(db_source.find({'geolocated': {'$exists': False}}, fields=["_id"]))
        for _id in ids_to_process:
            c = geoloc_tweet(_id["_id"])
            if c == (None, None):
                db_source.update({'_id': _id["_id"]}, {'$set': {'geolocated': False}})
            else:
                db_source.update({'_id': _id["_id"]}, {'$set': {'geolocated': True}})
                save_tweet_as_geojson(_id["_id"], c)
            #print _id, c
    except OperationFailure:
        logging.error("operationFailure on tweet id: {}".format(_id))

if __name__ == "__main__":
    process_tweet()


