#!/usr/bin/env python
#-*- coding:utf-8 -*-
u"""
Created on 26/09/13
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'


import feedfinder
import urlscanner
import argparse

def main(args):
    with open(args.urls) as f:
        for u in f:
            print "searching for feeds in: ", u
            feeds = feedfinder.feeds(u.strip())
            print "found %s feeds" % len(feeds)
            if feeds:
                feedfinder.store_feeds()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search for feeds on a set of web pages (urls)')
    parser.add_argument('file', metavar='urls', nargs=1,
                        help='file with one or more urls to check (one per line)')

    args = parser.parse_args()
    main(args)


