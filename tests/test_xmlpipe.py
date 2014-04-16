#-*- coding:utf-8 -*-
u"""
Created on 16/04/14
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import unittest

from pymongo import MongoClient

from indexing import mongo2sphinx


conn = MongoClient('127.0.0.1', 27017)
ARTICLES = conn["MCDB"]['articles']
FEEDS = conn["MCDB"]['feeds']

class TestXMLPipe(unittest.TestCase):
    def test_serialization_of_documents(self):
        docs  = ARTICLES.find({}, limit=100)
        for doc in docs:
            res = mongo2sphinx.serialize(doc, 1, ['_id', 'summary', 'link', 'link_content', 'title', 'language', 'published'])
            self.assertIn("<published>", res)
            self.assertIn("<_id>", res)
            self.assertIn("<summary>", res)
            self.assertIn("<link>", res)
            #self.assertIn("<id>", res)
            self.assertIn("<link_content>", res)
            self.assertIn("<title>", res)
            self.assertIn("<language>", res)

    def test_serialization_of_feeds(self):
        docs  = FEEDS.find({}, limit=100)
        for doc in docs:
            res = mongo2sphinx.serialize(doc, 1, ['_id', 'link', 'links', 'title', 'language', 'published'])
            self.assertIn("<published>", res)
            self.assertIn("<_id>", res)
            self.assertIn("<summary>", res)
            self.assertIn("<link>", res)
            #self.assertIn("<id>", res)
            self.assertIn("<link_content>", res)
            self.assertIn("<title>", res)
            self.assertIn("<language>", res)




