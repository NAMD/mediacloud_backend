#-*- coding:utf-8 -*-
u"""
Created on 24/04/14
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import unittest
import datetime
from collections import Counter

import pymongo

from analysis import htod


class TestFreqdist(unittest.TestCase):
    """
    Writing tests for this is hard because it takes a long time to calculate the freqdist file.
    """
    pass

class TestHtod(unittest.TestCase):
    def setUp(self):
        htod.client = pymongo.MongoClient('localhost', 27017)
        htod.ARTICLES = htod.client.MCDB.articles
        htod.PYPLNUSER = "mediacloud2"
        htod.PYPLNPASSWORD = "senha do mediacloud"

    def test_fetch_today_articles(self):
        d = "2014-02-14"
        arts = list(htod.fetch_articles(d))
        self.assertGreater(len(arts), 0)
        for a in arts:
            self.assertGreaterEqual(a["published"], datetime.datetime(2014, 02, 14))
            self.assertLess(a["published"], datetime.datetime(2014, 02, 15))

    def test_get_doc_freqdist(self):
        d = "2014-02-14"
        arts = [a for a in htod.fetch_articles(d) if "pypln_url" in a]
        fd = htod.get_doc_freqdist(arts[0]['pypln_url'])
        self.assertIsInstance(fd, list)

    def test_get_htod(self):
        d = "2014-02-14"
        count = htod.get_htod(d)
        self.assertIsInstance(count, Counter)
        self.assertGreater(len(count), 0)

