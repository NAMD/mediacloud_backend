#!/usr/bin/env/python3
"""
This script downloads the latest names table and updates the geoindex elastic search index
"""

import requests
import os
import datetime
import pandas as pd
import zipfile
import elasticsearch
from elasticsearch.helpers import bulk

DATA_URL = "http://download.geonames.org/export/dump/allCountries.zip"

def download_table(url):
    local_filename = "/tmp/" + url.split('/')[-1]
    if os.path.exists(local_filename):
        mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(local_filename))
        mod_time = datetime.date.fromordinal(mod_time.toordinal())
        if mod_time - datetime.date.today() > datetime.timedelta(-1):  # Less than a day old
            return local_filename

    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename

def load_data_file(fname):
    """
    Load the data file returning a TextFileReader Iterator.
    It parses the date on the last column and skip ba lines silently
    :param fname: Name of the data file
    :return: TextFileReader Iterator
    """
    columns = ["geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude", "feature class",
               "feature code", "country code", "cc2", "admin1 code", "admin2 code", "admin3 code", "admin4 code",
               "population", "elevation", "dem", "timezone", "modification date"]
    zf = zipfile.ZipFile(fname)
    reader = pd.read_csv(zf.open('allCountries.txt'), sep='\t', parse_dates=[-1], infer_datetime_format=True,
                         header=None, names=columns, error_bad_lines=False, iterator=True, chunksize=500)
    return reader

def index_geonames_on_elastic(reader):
    es = elasticsearch.Elasticsearch(hosts=['200.20.164.152'])
    es.indices.delete('Geonames', ignore=404)
    es.indices.create(index='Geonames', ignore=400)
    def rec_gen():
        for chunk in reader:
            for rec in chunk.to_dict(orient='records'):
                # print rec
                _id = rec['geonameid']
                op_dict = {
                '_index': 'Geonames',
                '_type': 'allCountries',
                '_id': int(_id),
                '_source': rec
                }
                yield op_dict

    bulk(es, rec_gen(), stats_only=True, chunk_size=500)





if __name__=="__main__":
    fn = download_table(DATA_URL)
    reader = load_data_file(fn)
    index_geonames_on_elastic(reader)