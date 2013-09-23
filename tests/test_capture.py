#-*- coding:utf-8 -*-
__author__ = 'fccoelho'

import unittest
from capture import feedfinder


class FeedFinderTests(unittest.TestCase):
    def setUp(self):
        with open('data/URLS.txt') as f:
            self.urls = [s.strip() for s in f.readlines()]

    def test_get_page(self):
        page = feedfinder.get_page('https://www.google.com')
        self.assertIn('google', page)

    def test_couldbefeed(self):
        page = feedfinder.get_page('http://wikipedia.org')
        feed = feedfinder.get_page('http://www.engadget.com/rss.xml')
        self.assertEquals(feedfinder.couldBeFeedData(page), 0)
        self.assertNotEquals(feedfinder.couldBeFeedData(feed), 0)

    def test_find_on_single_page(self):
        fs = feedfinder.feeds(self.urls[0], all=True)
        self.assertNotEquals(fs, [], msg="feeds returned an empty list.")


if __name__ == '__main__':
    unittest.main()
