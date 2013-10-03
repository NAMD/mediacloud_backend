#!/usr/bin/env python
"""
Fetches all urls in a google query for RSS feeds in Brasil
"""
__author__ = 'fccoelho'



import requests
#from beautifulsoup import
import GoogleScraper
from urlparse import unquote

q = "RSS+site:br"

urls = GoogleScraper.scrape(q, number_pages=5)
for url in urls:
    # You can access all parts of the search results like that
    # url.scheme => URL scheme specifier (Ex: 'http')
    # url.netloc => Network location part (Ex: 'www.python.org')
    # url.path => URL scheme specifier (Ex: ''help/Python.html'')
    # url.params => Parameters for last path element
    # url.query => Query component
    #print url
    print(unquote(url.geturl()))