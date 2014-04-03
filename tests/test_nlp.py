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


from capture import nlp

class TestNlp(unittest.TestCase):
    def setUp(self):
        self.corpus = nlp.get_corpus()
        self.test_documents = []
    def tearDown(self):
        for doc in self.test_documents:
            nlp.pypln.
