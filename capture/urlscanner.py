#-*- coding:utf-8 -*-
u"""
This module provides a url scanner function but depends on hhttrack being available on the system
Created on 24/09/13
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import csv
import subprocess
import os

def url_scanner(url, depth=1):
    """
    Scan the domain of the of the url give finding all urls up to the specified depth
    :param depth: depth of the search
    :param url: Initial url
    :return: list of urls
    """
    agent = "Mozilla/5.0 (X11; U; Linux; i686; en-US; rv:1.6) Gecko Debian/1.6-7"
    subprocess.check_call(['httrack', '-p0', '-d', '-r%s' % depth, '-c8', '-F "%s"' % agent, url])

    with open("hts-cache/new.txt") as f:
        t = csv.DictReader(f, delimiter='\t')
        urls = []
        for l in t:
            urls.append(l['URL'])
    subprocess.call(['rm', '-rf', 'hts-*'])
    return urls

