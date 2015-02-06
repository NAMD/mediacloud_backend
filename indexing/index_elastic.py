#!/usr/bin/env python
"""
Index any collection into Elasticsearch.
this script sould be used to import
"""

import elasticsearch
from pymongo import MongoClient
import argparse
from elasticsearch.helpers import bulk, streaming_bulk

es = elasticsearch.Elasticsearch(hosts=['localhost'])



def index_collection(db, collection, fields, host='localhost', port=27017):
    if len(fields) == 1:
        fields = fields[0].split(',')
    es.indices.create(index=db.lower(), ignore=400)
    conn = MongoClient(host, port)
    coll = conn[db][collection]
    cursor = coll.find({}, fields=fields, timeout=False)
    print "Starting Bulk index of {} documents".format(cursor.count())

    # def action_gen():
    # """
    # Generator to use for bulk inserts
    #     """
    #     for n, doc in enumerate(cursor):
    #         #print fields
    #         did = doc.pop('_id')
    #         if doc == {}:
    #             print "Empty document, skipping"
    #             continue
    #         op_dict = {
    #             '_index': db.lower(),
    #             '_type': collection,
    #             '_id': int('0x' + str(did), 16),
    #             '_source': doc
    #         }
    #         #op_dict['doc'] = doc
    #         yield op_dict
    for doc in cursor:
        did = int('0x' + str(doc.pop('_id')), 16)
        res = es.index(index=db.lower(), doc_type=collection, body=doc, id=did)
        #print res

    # res = bulk(es, action_gen(), stats_only=True)
    print res

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Perform a query on mongo db and index on Elasticsearch')
    parser.add_argument('--db', '-d', required=True, help="Database")
    parser.add_argument('--col', '-c', required=True, help="Collection")
    parser.add_argument('--host', '-H', type=str, default='127.0.0.1', help="MongoDB Host")
    parser.add_argument('--port', '-p', type=int, default=27017, help="port")
    parser.add_argument('--fields', '-f', required=True, type=str, nargs="+", help="Fields to be indexed")
    args = parser.parse_args()  # print args, args.prune

    index_collection(db=args.db, collection=args.col, fields=args.fields, host=args.host, port=args.port)
