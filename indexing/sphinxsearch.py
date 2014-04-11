#-*- coding:utf-8 -*-
u"""
Created on 11/04/14
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import argparse

import sphinxapi


client = sphinxapi.SphinxClient()
client.SetServer('127.0.0.1', 9312)

def do_query(q, ind):
    return client.Query(query=" ".join(q), index=ind)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Perform a query on sphinxsearch and return it as a json object')
    parser.add_argument('--host', '-H', type=str, default='127.0.0.1', help="Searchd Host")
    parser.add_argument('--port', '-p', type=int, default=9312, help="port")
    parser.add_argument('--index', '-i', required=False, default="MCDB_ARTICLES", help="Index")
    parser.add_argument('--query', '-q', required=True, type=str, nargs="+", help="query words")
    args = parser.parse_args()
    print args.query
    r = do_query(args.query, args.index)
    print r

