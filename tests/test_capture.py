#-*- coding:utf-8 -*-
__author__ = 'fccoelho'

import unittest
from capture import feedfinder


class FeedFinderTests(unittest.TestCase):
    def setUp(self):
        with open('data/URLS.txt') as f:
            self.urls = f.readlines()
    def test_find_on_single_page(self):
        fs = feedfinder.feeds(self.urls[0],all=True)
        self.assertIsNot(fs,[],msg="feeds returned an empty list.")


if __name__ == '__main__':
    unittest.main()
