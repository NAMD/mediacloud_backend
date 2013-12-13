#-*- coding:utf-8 -*-
import os
import json
import unittest
import tempfile

from app import app


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


if __name__ == '__main__':
    unittest.main()

