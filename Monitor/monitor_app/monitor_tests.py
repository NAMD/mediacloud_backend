#-*- coding:utf-8 -*-
import os
from app import app
import unittest
import tempfile

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

    def test_mongo_query_with_empty_collections(self):
        rv = self.app.get('/query/feeds')
        self.assertIn('{"error": "ValueError(', rv.data)
        rv = self.app.get('/query/articles')
        self.assertIn('{"error": "ValueError(', rv.data)

if __name__ == '__main__':
    unittest.main()

