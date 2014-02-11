#-*- coding:utf-8 -*-
import os
import json
import unittest
import tempfile

from app import app
from app import fetch_docs, mongo_client


class MonitorTestCase(unittest.TestCase):

    def setUp(self):
        # print app.config['DATABASE']
        self.db_fd, app.config['DATABASE'] = tempfile.mkstemp()
        app.config['TESTING'] = True
        app.config["MEDIACLOUD_DATABASE_HOST"] = 'localhost'
        self.app = app.test_client()
        #app.init_db()

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(app.config['DATABASE'])

    def test_fetch_docs_feeds(self):
        rv = self.app.get('/feeds/json')
        self.assertIn("aaData", json.loads(rv.data))
        self.assertGreater(len(json.loads(rv.data)["aaData"]), 0)

    def test_fetch_docs_articles(self):
        rv = self.app.get('/articles/json')
        self.assertIn("aaData", json.loads(rv.data))
        self.assertGreater(len(json.loads(rv.data)["aaData"]), 0)

    def test_fetch_docs_urls(self):
        rv = self.app.get('/urls/json')
        self.assertIn("meta", json.loads(rv.data))

    def test_solr_query_articles(self):
        rv = self.app.get('/solrquery/mediacloud_articles/rolezinho')
        self.assertGreater(len(json.loads(rv.data)), 0)

    def test_fetch_docs_from_list_of_ids(self):
        ids = [d["_id"] for d in mongo_client.MCDB.articles.find({}, fields=[], limit=10)]
        res = json.loads(fetch_docs("articles", ids=ids))
        # print len(ids), res
        self.assertEqual(len(res['data']), 10)

    @unittest.skip("wont work because Solr server is not indexing local db")
    def test_fetch_docs_from_query(self):
        rv = self.app.get('/articles/json/noticia')

if __name__ == '__main__':
    unittest.main()

