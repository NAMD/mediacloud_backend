#!/usr/bin/env python
#-*- coding:utf-8 -*-
#
# Copyright 2012 NAMD-EMAP-FGV

"""
This is a script to serialize a query in MongoDB to XML so that it can be indexed by SphinxSearch.
XML is output as a stream in order to keep memory usage low.

The output of this file is compatible with xmlpipe2:
http://sphinxsearch.com/docs/2.0.1/xmlpipe2.html

See mongo2sphinx --help for usage information.
Copyright 2014 NAMD-EMAP-FGV
Created on 03/10/11
by flavio
"""
__author__ = 'Flavio Codeço Coelho'

import argparse
from xml.etree.ElementTree import Element, tostring, SubElement
import time
import sys
import zlib
import cPickle as CP
import json

from pymongo import MongoClient


SW = sys.stdout  #Stream Writer
header = '<?xml version="1.0" encoding="utf-8"?><sphinx:docset>'
schema_head = """<sphinx:schema>
<sphinx:attr name="db" type="string"/>
<sphinx:attr name="collection" type="string"/>
<sphinx:attr name="_id" type="string"/>
"""

attr_type_dict = {"published": "timestamp",
                  "links": "string",
                  "language": "string",
                 }

def get_schema_tag(head, fields, attrs):
    """
    Returns the schema tag
    """
    for a in attrs:
        head += '<sphinx:attr name="{}" type="{}"/>\n'.format(a, attr_type_dict.get(a, "string"))
    for n in fields:
        head += '<sphinx:field name="%s"/>\n' % n
    return head + "</sphinx:schema>"


def serialize(doc, id, fields):
    """
    Receives raw MongoDB document data and returns XML.
    SphinxSearch demands that each document is identified by
    an unique unsigned integer `id`. We use a counter for this.
    """
    document = Element("sphinx:document", attrib={'id': str(id)})
    try:
        for k, v in doc.iteritems():
            if k not in fields:
                continue
            if k == '_id':
                SubElement(document, k).text = str(v)
                continue
            elif k == "link_content":
                SubElement(document, k).text = decompress_content(v)
            elif k == "published":
                SubElement(document, k).text = str(time.mktime(v.timetuple()))
            elif k == "links":
                SubElement(document, k).text = json.dumps({"links": v})
            elif k == "language":
                SubElement(document, k).text = json.dumps(v)
            else:
                SubElement(document, k).text = v
    except IndexError as e:
        print k, v
    return tostring(document)


def decompress_content(compressed_html):
    """
    Decompress data compressed by `compress_content`
    :param compressed_html: compressed html document
    :return: original html
    """
    # unencoded = b64.urlsafe_b64decode(str(compressed_html))
    decompressed = zlib.decompress(compressed_html)
    orig_html = CP.loads(decompressed)
    return orig_html



def query(db, collection, fields, attrs, host='127.0.0.1', port=27017):
    """
    Given a mongo db, a collection and a list of fields, writes a stream of XML to stdout
    """
    conn = MongoClient(host, port)
    coll = conn[db][collection]
    cursor = coll.find({}, fields=fields)
    locationdic = {'db': db, 'collection': collection}
    schema = get_schema_tag(schema_head, fields, attrs)
    SW.write(header)
    SW.write(schema)
    i=1
    for doc in cursor:
        id = int('0x' + str(doc['_id']), 16)
        doc.update(locationdic)
        try:
            ser_doc = serialize(doc, i, fields)
            SW.write(ser_doc)

        except IOError as e:
            with open("IOError.log", 'w') as f:
                f.write(header+schema + '\n\n')
                f.write(ser_doc+"</sphinx:docset>")
            raise IOError('Failed! see IOError logfile')
        i += 1
    SW.write("</sphinx:docset>")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Perform a query on mongo db and return it as XML')
    parser.add_argument('--db', '-d', required=True, help="Database")
    parser.add_argument('--col', '-c', required=True, help="Collection")
    parser.add_argument('--host', '-H', type=str, default='127.0.0.1', help="MongoDB Host")
    parser.add_argument('--port', '-p', type=int, default=27017, help="port")
    parser.add_argument('--fields', '-f', required=True, type=str, nargs="+", help="Fields to be indexed")
    parser.add_argument('--attrs', '-a', required=True, type=str, nargs="+", help="Extra Attributes")
    args = parser.parse_args()  #    print args, args.prune

    query(db=args.db, collection=args.col, fields=args.fields, attrs=args.attrs, host=args.host, port=args.port)
    #TODO: allow the user to specify an unique integer id field to be used in cases where the index needs to be updated if it is not provided, a counter should be used.
