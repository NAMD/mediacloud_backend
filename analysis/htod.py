#!/usr/bin/env python
"""
This script calculates the "hot topic of the day". By looking at all articles published on a given day.
"""
__author__ = 'fccoelho'

import argparse
import datetime
from collections import Counter

import pymongo
from pypln.api import PyPLN, Document


Today = datetime.datetime.today()


def get_htod(d):
    """
    Calculates "Hot token of the day"
    :param d: day in 'YYYY-MM-DD' format
    :return: Counter object with the counts of topics
    """
    arts = fetch_articles(d)
    total = Counter()  # Use Counters to add up freqdists
    for article in arts:
        if "pypln_url" in article:
            fd = get_doc_freqdist(article['pypln_url'])
            total += Counter(dict(fd))

    return total


def get_doc_freqdist(url):
    """
    Get Freqdist for a given pypln document given its URL
    :param url: URL of the Document
    :return: Freqdist (list of lists)
    """
    try:
        doc = Document.from_url(url, (PYPLNUSER, PYPLNPASSWORD))
        fd = doc.get_property("freqdist")
    except RuntimeError as e:
        fd = []
    return fd


def fetch_articles(d=None):
    """
    Fetch Articles published on a single Day
    :param d: Day in 'YYYY-MM-DD' format
    :return: articles (list of dictionaries)
    """
    if d is None:
        d = Today
    else:
        year, month, day = [int(i) for i in d.split('-')]
        d = datetime.datetime(year, month, day)
    end = d + datetime.timedelta(1)
    arts = ARTICLES.find({"published": {"$gte": d, "$lt": end}}, fields=["published", "pypln_url"])
    return arts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Calculate 'Hot Topics of the Day'.\nA ranking of the most mentioned subjects"))

    parser.add_argument("-d", "--date", type=str, default="{}-{}-{}".format(Today.year, Today.month, Today.day),
                        help="Date to Analyse in YEAR-MO-DD format")
    parser.add_argument("-h", '--host', type=str, help='MongoDB host to connect to')
    parser.add_argument("-p", '--port', type=int, default=27017, help='MongoDB port to connect to')
    parser.add_argument("--pyplhost", type=str, help="PyPLN host to use.")
    parser.add_argument("--pyplnuser", type=str, help="PyPLN user.")
    parser.add_argument("--pyplnpassword", type=str, help="PyPLN password")

    args = parser.parse_args()

    client = pymongo.MongoClient(args.host, args.port)
    MCDB = client.MCDB
    FEEDS = MCDB.feeds  # Feed collection
    ARTICLES = MCDB.articles  # Article Collection
    PYPLNUSER = args.pyplnuser
    PYPLNPASSWORD = args.pyplnpassword
    pypln = PyPLN(args.pyplnhost, (args.pyplnuser, args.pyplnpassword))

    get_htod(args.date)
