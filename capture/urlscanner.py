#-*- coding:utf-8 -*-
u"""
This module provides a url scanner function but depends on httrack being available on the system
Created on 24/09/13
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import csv
import subprocess
import os
import tempfile
import shutil

def url_scanner(url, depth=1):
    """
    Scan the domain of the of the url give finding all urls up to the specified depth
    :param depth: depth of the search
    :param url: Initial url
    :return: list of urls
    """
    current_dir = os.getcwd()
    tempdir = tempfile.gettempdir()
    os.chdir(tempdir)
    agent = "Mozilla/5.0 (X11; U; Linux; i686; en-US; rv:1.6) Gecko Debian/1.6-7"
    try:
        subprocess.check_output(['httrack', '-p0', '-%P', '-b1', '-i', '-d', '-T1', '-R1', '-r%s' % depth, '-c16', '-F "%s"' % agent, url])
    except subprocess.CalledProcessError as e:
        print "failed on {}:\n{}".format(url, e.output)

    with open("hts-cache/new.txt") as f:
        t = csv.DictReader(f, delimiter='\t')
        urls = []
        for l in t:
            urls.append(l['URL'])
    try:
        shutil.rmtree('hts-cache', ignore_errors=True)
        shutil.rmtree(url.split("://")[-1])
    except OSError:
        print "Directory not found"
    try:
        os.remove(os.path.join(tempdir, 'cookies.txt'))
        os.remove(os.path.join(tempdir, 'hts-log.txt'))
        if os.path.exists(os.path.join(tempdir, 'cookies.txt')):
            os.remove(os.path.join(tempdir, 'cookies.txt'))
    except OSError as e:
        print e
    os.chdir(current_dir)
    return urls

