#!/usr/bin/env python
# coding: utf-8

"""Script to migrate dates/datetimes from strings to date objetcs"""

from __future__ import print_function

__docformat__ = 'restructuredtext en'

import argparse
import datetime
import sys

from re import compile as regexp_compile

import bson
import pymongo

from dateutil.parser import parse as dateutil_parse_date
from pymongo.errors import DuplicateKeyError


BSON_DATE = ord(bson.BSONDAT) # WTF, pymongo?
MONTHS = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4,  'mai': 5,  'jun': 6,
          'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12,

          'feb': 2, 'apr': 4, 'may': 5, 'aug': 8,  'sep': 9,  'oct': 10,
          'dec': 12}
FULL_MONTHS = {'janeiro': 1,   'fevereiro': 2, u'março': 3,    'abril': 4,
               'maio': 5,      'junho': 6,     'julho': 7,     'agosto': 8,
               'setembro': 9,  'outubro': 10,  'novembro': 11, 'dezembro': 12,

               'january': 1,   'februrary': 2, 'march': 3,     'april': 4,
               'may': 5,       'june': 6,      'july': 7,      'august': 8,
               'september': 9, 'october': 10,  'november': 11, 'december': 12,}
regexp_almost_iso_date = \
        regexp_compile(r'([0-9]{4}-[0-9]{2}-[0-9]{2})t([0-9]{2}:[0-9]{2}:[0-9]{2})([+-]+[0-9:]*)')


def get_offset_datetime(offset):
    if offset.lower() == 'gmt':
        offset = '+0000'
    offset_signal = int(offset[0] + '1')
    offset_hours = int(offset[1:3])
    offset_minutes = int(offset[3:5])
    total_offset_seconds = offset_signal * (offset_hours * 3600 +
                                            offset_minutes * 60)
    offset_in_days = total_offset_seconds / (3600.0 * 24)
    return datetime.timedelta(offset_in_days)


def parse_pt_date(date_string):
    '''Parses a date-time string and return datetime object
       The format is like this:
       'Seg, 21 Out 2013 22:14:36 -0200'
    '''
    date_info = date_string.lower().strip()
    if '\n' in date_info:
        date_info = date_info.split('\n')[-1].strip()
    date_info = date_info.split()
    regexp_results = regexp_almost_iso_date.findall(' '.join(date_info))

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
    elif regexp_results:
        date_almost_iso = list(regexp_results[0])
        if date_almost_iso[2][0] in '+-' and date_almost_iso[2][1] in '+-':
            date_almost_iso[2] = date_almost_iso[2][1:]
        date_almost_iso[2] = date_almost_iso[2].replace(':', '')
        if len(date_almost_iso[2]) == 4:
            date_almost_iso[2] = '{}0{}'.format(date_almost_iso[2][0],
                    date_almost_iso[2][1:])

        date_iso = '{0}T{1}'.format(*date_almost_iso)
        date_object = datetime.datetime.strptime(date_iso, '%Y-%m-%dT%H:%M:%S')
        offset = get_offset_datetime(date_almost_iso[2])
        return date_object - offset
    else:
        _, day, month_pt, year, hour_minute_second, offset = date_info

        offset = get_offset_datetime(offset)

        month = MONTHS[month_pt]
        datetime_iso = '{}-{:02d}-{:02d}T{}'.format(year, month, int(day),
                hour_minute_second)
        datetime_object = datetime.datetime.strptime(datetime_iso,
                '%Y-%m-%dT%H:%M:%S')
        return datetime_object - offset


def parse_date(value):
    try:
        new_value = dateutil_parse_date(value)
    except (ValueError, TypeError):
        try:
            new_value = parse_pt_date(value)
        except (ValueError, TypeError, IndexError):
            raise ValueError()
    return new_value


def parse_dates_in(collection):
    published = {'$and': [{'published': {'$exists': True}},
                          {'published': {'$not': {'$type': BSON_DATE}}}]}
    updated = {'$and': [{'updated': {'$exists': True}},
                        {'updated': {'$not': {'$type': BSON_DATE}}}]}
    date_filter = {'$or': [published, updated]}
    fields = {'updated': True, 'published': True, '_id': True}
    cursor = collection.find(date_filter, fields)

    date_fields = ['published', 'updated']
    undesired_types = (type(None), datetime.datetime, datetime.date)
    updated_documents = 0
    total = cursor.count()

    for document in cursor:
        print('Updating document "{}"...'.format(document['_id']), end='')

        updated = False
        for field_name in date_fields:
            value = document.get(field_name, None)
            if type(value) not in undesired_types:
                try:
                    new_value = parse_date(value)
                except ValueError:
                    print(' ERROR ({} = {})'.format(field_name, repr(value)),
                            end='')
                else:
                    print(' UPDATED ({}: {} -> {})'.format(field_name,
                        repr(value), repr(new_value)), end='')
                    collection.update({'_id': document['_id']},
                            {'$set': {field_name: new_value}})
                    updated = True
        print('')
        if updated:
            updated_documents += 1

    print('Total documents updates: {}. Found: {}'
            .format(updated_documents, total))


if __name__ == '__main__':
    args = argparse.ArgumentParser(description=globals()['__doc__'])
    args.add_argument('host', type=str, help='MongoDB host to connect to')
    args.add_argument('--port', type=int, default=27017,
            help='MongoDB port to connect to')
    args.add_argument('database', type=str)
    args.add_argument('collection', type=str)
    argv = args.parse_args()

    client = pymongo.MongoClient(host=argv.host, port=argv.port)
    database = client[argv.database]
    collection = database[argv.collection]

    parse_dates_in(collection)
