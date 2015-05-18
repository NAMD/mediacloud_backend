#-*- coding:utf-8 -*-
u"""
Created on 26/09/13
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

##########
# Storage and Indexing configuration
##########
MONGOHOST = "localhost"

##########
# NLP Configuration
##########
PYPLNHOST = "http://fgv.pypln.org/"
# Change this for the real credentials
PYPLN_CREDENTIALS = ("<PYPLN_USERNAME>", "<PYPLN_PASSWORD>")

##########
# Elasticsearch configuration
##########
ELASTICHOST = "localhost"
ELASTIC_ARTICLE_FIELDS = ['summary', 'title', 'cleaned_text', 'link', 'links', 'language', 'published']
