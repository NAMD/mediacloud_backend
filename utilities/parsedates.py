#-*- coding:utf-8 -*-
"""
This module helps migrating dates from strings to date objetcs in the article collection in MDCB
Created on 23/10/13
by fccoelho with contribution of parse_pt_date by @turicas
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import datetime
import sys

from dateutil.parser import parse
import pymongo
from pymongo.errors import DuplicateKeyError


def parse_dates(collection):
    for doc in collection.find():
        # TODO: only iterate over documents with non-ISODate 'updated' field
        if "published" in doc and not isinstance(doc['published'], datetime.datetime):
            print "updating {0:s}".format(doc["_id"])
            try:
                collection.update({"_id": doc["_id"]}, {"$set": {"published": parse(doc['published'])}})
            except DuplicateKeyError:
                print "could not update this document:\n{}".format(doc)
            except ValueError:
                try:
                    collection.update({"_id": doc["_id"]}, {"$set": {"published": parse_pt_date(doc['published'])}})
                except:
                    print "Could not parse string: {0:s}".format(doc['published'])
            except TypeError:
                try:
                    collection.update({"_id": doc["_id"]}, {"$set": {"published": parse_pt_date(doc['published'])}})
                except:
                    print "Could not parse string: {0:s}".format(doc['published'])

        if "updated" in doc and not isinstance(doc['updated'], datetime.datetime):
            try:
                collection.update({"_id": doc["_id"]}, {"$set": {"updated": parse(doc['updated'])}})
            except DuplicateKeyError:
                print "could not update this document:\n{}".format(doc)
            except ValueError:
                try:
                    collection.update({"_id": doc["_id"]}, {"$set": {"updated": parse_pt_date(doc['updated'])}})
                except:
                    print "Could not parse string: {0:s}".format(doc['updated'])
            except TypeError:
                try:
                    collection.update({"_id": doc["_id"]}, {"$set": {"updated": parse_pt_date(doc['updated'])}})
                except:
                    print "Could not parse string: {0:s}".format(doc['updated'])

MONTHS = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4,  'mai': 5,  'jun': 6,
          'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12}
FULL_MONTHS = {'janeiro': 1,  'fevereiro': 2, u'março': 3,    'abril': 4,
               'maio': 5,     'junho': 6,     'julho': 7,     'agosto': 8,
               'setembro': 9, 'outubro': 10,  'novembro': 11, 'dezembro': 12}

def parse_pt_date(date_string):
    '''Parses a date-time string and return datetime object
       The format is like this:
       'Seg, 21 Out 2013 22:14:36 -0200'
    '''
    date_info = date_string.lower().split()
    if date_info.count('de') == 2 or len(date_info) == 3:
        if ',' in date_info[0]:
            date_string = date_string.split(',')[1]
        date_info = date_string.lower().replace('de ', '').split()
        day, month_pt, year = date_info
        if len(month_pt) == 3:
            month = MONTHS[month_pt]
        else:
            month = FULL_MONTHS[month_pt]
        date_iso = '{}-{:02d}-{:02d}'.format(year, int(month), int(day))
        date_object = datetime.datetime.strptime(date_iso, '%Y-%m-%d')
        return date_object
    else:
        _, day, month_pt, year, hour_minute_second, offset = date_info

        if offset.lower() == 'gmt':
            offset = '+0000'
        offset_signal = int(offset[0] + '1')
        offset_hours = int(offset[1:3])
        offset_minutes = int(offset[3:5])
        total_offset_seconds = offset_signal * (offset_hours * 3600 +
                                                offset_minutes * 60)
        offset_in_days = total_offset_seconds / (3600.0 * 24)

        month = MONTHS[month_pt]
        datetime_iso = '{}-{:02d}-{:02d}T{}'.format(year, month, int(day),
                hour_minute_second)
        datetime_object = datetime.datetime.strptime(datetime_iso,
                '%Y-%m-%dT%H:%M:%S')
        return datetime_object - datetime.timedelta(offset_in_days)

if __name__ == "__main__":
    ## Media Cloud database setup
    client = pymongo.MongoClient(sys.argv[1])
    MCDB = client.MCDB
    if len(sys.argv) > 2:
        Collection = MCDB[sys.argv[2]]  # user-defined collection
    else:
        Collection = MCDB['articles']  # Article collection (default)
    parse_dates(Collection)
