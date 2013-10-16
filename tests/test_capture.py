#-*- coding:utf-8 -*-
__author__ = 'fccoelho'

import unittest
from capture import feedfinder, urlscanner, downloader
import subprocess
from capture import googlerss
import bson


class FeedFinderTests(unittest.TestCase):
    def setUp(self):
        with open('data/URLS.txt') as f:
            self.urls = [s.strip() for s in f.readlines()]

    def tearDown(self):
        downloader.FEEDS.drop()

    def test_get_page(self):
        page = feedfinder.get_page('https://www.google.com')
        self.assertIn('google', page)

    def test_isfeed(self):
        page = feedfinder.get_page('http://wikipedia.org')
        feed = feedfinder.get_page('http://www.engadget.com/rss.xml')
        self.assertEquals(feedfinder.isFeed(page), 0)
        self.assertEquals(feedfinder.isFeed(feed), 1)

    def test_find_on_single_page(self):
        fs = feedfinder.feeds(self.urls[0], all=True)
        self.assertNotEquals(fs, [], msg="feeds returned an empty list.")

    def test_store_feed(self):
        feedfinder.store_feeds(['http://www.engadget.com/rss.xml'])
        res = downloader.FEEDS.find({"title_detail.base": 'http://www.engadget.com/rss.xml'}, fields=["title_detail"])
        res = list(res)
        self.assertEquals(res[0]['title_detail']['base'], 'http://www.engadget.com/rss.xml')

class TestUrlScanner(unittest.TestCase):
    def tearDown(self):
        subprocess.call(['rm', '-rf', 'hts-*'])
        subprocess.call(['rm', '-rf', 'cookies.txt'])
        subprocess.call(['rm', '-rf', 'www.google.com'])

    def test_scan(self):
        l = urlscanner.url_scanner('www.google.com', 1)
        self.assertEquals(l, ['http://www.google.com/robots.txt', 'http://www.google.com/',])

class TestDownloader(unittest.TestCase):
    def setUp(self):
        self.d = downloader.RSSDownload('http://estadao.feedsportal.com/c/33043/f/534104/index.rss')

    def tearDown(self):
        downloader.ARTICLES.drop()

    def test_store_articles(self):
        self.d.parse()
        res = downloader.ARTICLES.find().count()
        self.assertGreater(res, 0, "{} is not greater than 0".format(res))

    def test_compress_decompress(self):
        page = feedfinder.get_page('http://www.fgv.br')
        cp = downloader.compress_content(page)
        self.assertEqual(page, downloader.decompress_content(cp))

    def test_store_compressed_data(self):
        page = feedfinder.get_page('http://www.fgv.br')
        cp = downloader.compress_content(page)
        downloader.ARTICLES.insert({"compressed": cp})
        rp = downloader.ARTICLES.find_one()
        self.assertEqual(page, downloader.decompress_content(rp["compressed"]))


class TestGoogleRSS(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        googlerss.MCDB.urls.drop()

    def test_save_urls(self):
        googlerss.main()
        assert googlerss.MCDB.urls.count() > 0


if __name__ == '__main__':
    unittest.main()
