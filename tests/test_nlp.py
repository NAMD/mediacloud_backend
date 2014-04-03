#-*- coding:utf-8 -*-
u"""
Created on 03/04/14
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'
import unittest
import subprocess
import json
from pypln.api import PyPLN, Corpus, Document
import requests
import time


from capture import nlp

class TestNlp(unittest.TestCase):
    def setUp(self):
        self.corpus = nlp.get_corpus("test_corpus")
        self.test_documents = []
        self.doc = requests.get("http://pt.wikipedia.org/wiki/M%C3%A9dia_%28comunica%C3%A7%C3%A3o%29").content
    def tearDown(self):
        for doc in self.test_documents:
            pass

    def test_add_document(self):
        doc = self.corpus.add_document(self.doc)

        self.test_documents.append(doc)
        time.sleep(2)
        self.assertGreater(len(self.corpus.documents), 0)
