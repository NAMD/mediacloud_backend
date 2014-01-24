# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Receives documents from the oplog worker threads and indexes them
into the backend.

This file is a document manager for the Solr search engine, but the intent
is that this file can be used as an example to add on different backends.
To extend this to other systems, simply implement the exact same class and
replace the method definitions with API calls for the desired backend.
"""
import re
import json
import logging
import zlib
import cPickle as CP
from threading import Timer
from bson import json_util
from pysolr import Solr, SolrError
from mongo_connector import errors
from mongo_connector.util import retry_until_ok


# create console handler and set level to debug
logger = logging.getLogger("Solr")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
logger.addHandler(ch)

ADMIN_URL = 'collection1/schema/?wt=json'

decoder = json.JSONDecoder()

def decompress_content(compressed_html):
    """
    Decompress data compressed by `compress_content`
    :param compressed_html: compressed html document
    :return: original html
    """
    decompressed = zlib.decompress(compressed_html)
    orig_html = CP.loads(decompressed)
    return orig_html

class DocManager():
    """The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that multiple
    updates to the same doc reflect the most up to date version as opposed to
    multiple, slightly different versions of a doc.
    """

    def __init__(self, url, auto_commit=False, unique_key='_id', **kwargs):
        """Verify Solr URL and establish a connection.
        """
        self.solr = Solr(url)
        self.unique_key = unique_key
        self.auto_commit = auto_commit
        self.field_list = []
        self.dynamic_field_list = []
        self.build_fields()

        if auto_commit:
            self.run_auto_commit()

    def _parse_fields(self, result, field_name):
        """ If Schema access, parse fields and build respective lists
        """
        # print field_name, result
        field_list = []
        for field in result.get('schema', {}).get(field_name, {}):
            if field["name"] not in field_list:
                field_list.append(field)
        return field_list

    def build_fields(self):
        """ Builds a list of valid fields
        """
        declared_fields = self.solr._send_request('get', ADMIN_URL)
        result = decoder.decode(declared_fields)
        self.field_list = self._parse_fields(result, 'fields'),
        self.dynamic_field_list = self._parse_fields(result, 'dynamicFields')

    def clean_doc(self, doc):
        """ Cleans a document passed in to be compliant with the Solr as
        used by Solr. This WILL remove fields that aren't in the schema, so
        the document may actually get altered.
        """
        if "link_content" in doc:
            doc = self.decompress(doc)

        if not self.field_list:
            return doc

        fixed_doc = {}
        for key, value in doc.items():
            if key in self.field_list[0]:
                fixed_doc[key] = value

            # Dynamic strings. * can occur only at beginning and at end
            else:
                for field in self.dynamic_field_list:
                    if field["name"] == '*':
                        regex = re.compile(r'\w%s\b' % (field))
                    else:
                        regex = re.compile(r'\b%s\w' % (field))
                    if regex.match(key):
                        fixed_doc[key] = value

        return fixed_doc

    def stop(self):
        """ Stops the instance
        """
        self.auto_commit = False

    def decompress(self, doc):
        # Decompress the content of the article before sending to Solr
        doc["link_content"] = decompress_content(doc["link_content"])
        return doc

    def upsert(self, doc):
        """Update or insert a document into Solr

        This method should call whatever add/insert/update method exists for
        the backend engine and add the document in there. The input will
        always be one mongo document, represented as a Python dictionary.
        """

        try:
            self.solr.add([self.clean_doc(doc)], commit=True)
        except SolrError:
            logging.error( "Could not insert %r into Solr" % doc)
            raise errors.OperationFailed(
                 "Could not insert %r into Solr" % json.dumps(doc, default=json_util))

    def bulk_upsert(self, docs):
        """Update or insert multiple documents into Solr

        docs may be any iterable
        """
        try:
            cleaned = (self.clean_doc(d) for d in docs)
            self.solr.add(cleaned, commit=True)
        except SolrError:
            raise errors.OperationFailed(
                "Could not bulk-insert documents into Solr")

    def remove(self, doc):
        """Removes documents from Solr

        The input is a python dictionary that represents a mongo document.
        """
        self.solr.delete(id=str(doc[self.unique_key]), commit=True)

    def _remove(self):
        """Removes everything
        """
        self.solr.delete(q='*:*')

    def search(self, start_ts, end_ts):
        """Called to query Solr for documents in a time range.
        """
        query = '_ts: [%s TO %s]' % (start_ts, end_ts)
        return self.solr.search(query, rows=100000000)

    def _search(self, query):
        """For test purposes only. Performs search on Solr with given query
            Does not have to be implemented.
        """
        return self.solr.search(query, rows=200)

    def commit(self):
        """This function is used to force a commit.
        """
        retry_until_ok(self.solr.commit)

    def run_auto_commit(self):
        """Periodically commits to the Solr server.
        """
        self.solr.commit()
        if self.auto_commit:
            Timer(1, self.run_auto_commit).start()

    def get_last_doc(self):
        """Returns the last document stored in the Solr engine.
        """
        #search everything, sort by descending timestamp, return 1 row
        try:
            result = self.solr.search('*:*', sort='_ts desc', rows=1)
        except ValueError:
            return None

        if len(result) == 0:
            return None

        return result.docs[0]
