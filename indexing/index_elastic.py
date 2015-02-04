#!/usr/bin/env python
"""
Index any collection into Elasticsearch.
this script sould be used to import
"""

import elasticsearch
from pymongo import MongoClient
import argparse

es = elasticsearch.Elasticsearch(hosts=['localhost'])



def index_collection(db, collection, fields, host='localhost', port=27017):
    locationdic = {'db': db, 'collection': collection}
    conn = MongoClient(host, port)
    coll = conn[db][collection]
    cursor = coll.find({}, fields=fields, timeout=False)
    total = coll.count()

    for n, doc in enumerate(cursor):
        elastic_doc = {
            'index': db.lower(),
            'doc_type': collection,
            'id': int('0x' + str(doc['_id']), 16),
        }
        doc.pop('_id')
        elastic_doc['body'] = doc
        es.index(index=elastic_doc['index'],
                 doc_type=elastic_doc['doc_type'],
                 id=elastic_doc['id'],
                 body=doc
        )
        #print "indexed {} articles of a total of {}".format(n, total)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Perform a query on mongo db and index on Elasticsearch')
    parser.add_argument('--db', '-d', required=True, help="Database")
    parser.add_argument('--col', '-c', required=True, help="Collection")
    parser.add_argument('--host', '-H', type=str, default='127.0.0.1', help="MongoDB Host")
    parser.add_argument('--port', '-p', type=int, default=27017, help="port")
    parser.add_argument('--fields', '-f', required=True, type=str, nargs="+", help="Fields to be indexed")
    args = parser.parse_args()  # print args, args.prune

    index_collection(db=args.db, collection=args.col, fields=args.fields, host=args.host, port=args.port)