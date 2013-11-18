"""
feedfinder: Find the Web feeds from a Web page
Based on script by:
http://www.aaronsw.com/2002/feedfinder/

Modified by the Mediacloud Brasil team at NAMD (github.com/NAMD)

Usage:
  feed(uri) - returns feed found for a URI
  feeds(uri) - returns all feeds found for a URI

    >>> import feedfinder
    >>> feedfinder.feed('scripting.com')
    'http://scripting.com/rss.xml'
    >>>
    >>> feedfinder.feeds('scripting.com')
    ['http://delong.typepad.com/sdj/atom.xml', 
     'http://delong.typepad.com/sdj/index.rdf', 
     'http://delong.typepad.com/sdj/rss.xml']
    >>>

Can also use from the command line.  Feeds are returned one per line:

    $ python feedfinder.py diveintomark.org
    http://diveintomark.org/xml/atom.xml

How it works:
  0. At every step, feeds are minimally verified to make sure they are really feeds.
  1. If the URI points to a feed, it is simply returned; otherwise
     the page is downloaded and the real fun begins.
  2. Feeds pointed to by LINK tags in the header of the page (autodiscovery)
  3. <A> links to feeds on the same server ending in ".rss", ".rdf", ".xml", or 
     ".atom"
  4. <A> links to feeds on the same server containing "rss", "rdf", "xml", or "atom"
  5. <A> links to feeds on external servers ending in ".rss", ".rdf", ".xml", or 
     ".atom"
  6. <A> links to feeds on external servers containing "rss", "rdf", "xml", or "atom"
  7. Try some guesses about common places for feeds (index.xml, atom.xml, etc.).
  8. As a last ditch effort, we search Syndic8 for feeds matching the URI
"""


_debug = 0

import sgmllib, urllib, urlparse, re, sys, robotparser
import requests
import argparse
import feedparser
import logging
import settings
import pymongo
import time
from pymongo.errors import DuplicateKeyError
from logging.handlers import RotatingFileHandler

client = pymongo.MongoClient(settings.MONGOHOST, 27017)
MCDB = client.MCDB
FEEDS = MCDB.feeds  # Feed collection

###########################
#  Setting up Logging
###########################
logger = logging.getLogger("Feedfinder")
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = RotatingFileHandler('/tmp/mediacloud.log', maxBytes=5e6, backupCount=3)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add ch to logger
#logger.addHandler(ch)  # uncomment for console output of messages
logger.addHandler(fh)

def get_page(url):
    """
    Fetches html from an URL
    """
    try:
        r = requests.get(url)
        if 'content-encoding' in r.headers and r.headers['content-encoding'] == 'gzip':
            html = r.content
        else:
            html = r.text
    except requests.ConnectionError:
        html = ''

    return html

class BaseParser(sgmllib.SGMLParser):
    def __init__(self, baseuri):
        sgmllib.SGMLParser.__init__(self)
        self.links = []
        self.baseuri = baseuri
        
    def normalize_attrs(self, attrs):
        def cleanattr(v):
            v = sgmllib.charref.sub(lambda m: unichr(int(m.groups()[0])), v)
            v = v.strip()
            v = v.replace('&lt;', '<').replace('&gt;', '>').replace('&apos;', "'").replace('&quot;', '"').replace('&amp;', '&')
            return v
        attrs = [(k.lower(), cleanattr(v)) for k, v in attrs]
        attrs = [(k, k in ('rel','type') and v.lower() or v) for k, v in attrs]
        return attrs
        
    def do_base(self, attrs):
        attrsD = dict(self.normalize_attrs(attrs))
        if not attrsD.has_key('href'): return
        self.baseuri = attrsD['href']
    
    def error(self, *a, **kw):
        pass  # we're not picky

class LinkParser(BaseParser):
    FEED_TYPES = ('application/rss+xml',
                  'text/xml',
                  'application/atom+xml',
                  'application/x.atom+xml',
                  'application/x-atom+xml')

    def do_link(self, attrs):
        attrsD = dict(self.normalize_attrs(attrs))
        if not attrsD.has_key('rel'):
            return
        rels = attrsD['rel'].split()
        if 'alternate' not in rels:
            return
        if attrsD.get('type') not in self.FEED_TYPES:
            return
        if not attrsD.has_key('href'):
            return
        self.links.append(urlparse.urljoin(self.baseuri, attrsD['href']))


class ALinkParser(BaseParser):
    def start_a(self, attrs):
        attrsD = dict(self.normalize_attrs(attrs))
        if not attrsD.has_key('href'):
            return
        self.links.append(urlparse.urljoin(self.baseuri, attrsD['href']))

def makeFullURI(uri):
    uri = uri.strip()
    if uri.startswith('feed://'):
        uri = 'http://' + uri.split('feed://', 1).pop()
    for x in ['http', 'https']:
        if uri.startswith('%s://' % x):
            return uri
    return 'http://%s' % uri

def getLinks(data, baseuri):
    p = LinkParser(baseuri)
    p.feed(data)
    return p.links

def getALinks(data, baseuri):
    p = ALinkParser(baseuri)
    p.feed(data)
    return p.links

def getLocalLinks(links, baseuri):
    baseuri = baseuri.lower()
    urilen = len(baseuri)
    local_links = []
    for l in links:
        try:
            if l.lower().startswith(baseuri):
                local_links.append(l)
        except UnicodeDecodeError:
            try:
                l = l.decode('utf8')
                if l.lower().startswith(baseuri):
                    local_links.append(l)
            except UnicodeDecodeError as e:
                logger.error("Could not decode link: %s \n %s", l, e)
            except UnicodeEncodeError as e:
                logger.error("Could not encode link: %s\n%s", l, e)
    return local_links

def isFeedLink(link):
    return link[-4:].lower() in ('.rss', '.rdf', '.xml', '.atom')

def isXMLRelatedLink(link):
    link = link.lower()
    return link.count('rss') + link.count('rdf') + link.count('xml') + link.count('atom')

r_brokenRedirect = re.compile('<newLocation[^>]*>(.*?)</newLocation>', re.S)
def tryBrokenRedirect(data):
    if '<newLocation' in data:
        newuris = r_brokenRedirect.findall(data)
        if newuris:
            return newuris[0].strip()

def isFeed(url):
    """
    Check if content corresponds to an html document (returns 0)
    or a feed (returns >0).
    """
    p = feedparser.parse(url)
    version = p.get("version")
    return int(version != "")


def sortFeeds(feed1Info, feed2Info):
    return cmp(feed2Info['headlines_rank'], feed1Info['headlines_rank'])

    
def feeds(uri, all=False, _recurs=None):
    """
    Returns List of feeds found on the page
    """
    if _recurs is None: _recurs = [uri]
    fulluri = makeFullURI(uri)
    try:
        data = get_page(fulluri)
    except:
        return []
    # is this already a feed?
    if isFeed(data):
        return [fulluri]
    newuri = tryBrokenRedirect(data)
    if newuri and newuri not in _recurs:
        _recurs.append(newuri)
        return feeds(newuri, all=all, _recurs=_recurs)
    # nope, it's a page, try LINK tags first

    try:
        outfeeds = getLinks(data, fulluri)
    except:
        outfeeds = []
    # print('found %s feeds through LINK tags' % len(outfeeds))
    outfeeds = filter(isFeed, outfeeds)
    if all or not outfeeds:
        # no LINK tags, look for regular <A> links that point to feeds
        # print('no LINK tags, looking at A tags')
        try:
            links = getALinks(data, fulluri)
        except:
            links = []
        locallinks = getLocalLinks(links, fulluri)
        # look for obvious feed links on the same server
        outfeeds.extend(filter(isFeed, filter(isFeedLink, locallinks)))
        if all or not outfeeds:
            # look harder for feed links on the same server
            outfeeds.extend(filter(isFeed, filter(isXMLRelatedLink, locallinks)))
        if all or not outfeeds:
            # look for obvious feed links on another server
            outfeeds.extend(filter(isFeed, filter(isFeedLink, links)))
        if all or not outfeeds:
            # look harder for feed links on another server
            outfeeds.extend(filter(isFeed, filter(isXMLRelatedLink, links)))
    if all or not outfeeds:
        # print('no A tags, guessing')
        suffixes = [ # filenames used by popular software:
          'atom.xml', # blogger, TypePad
          'index.atom', # MT, apparently
          'index.rdf', # MT
          'rss.xml', # Dave Winer/Manila
          'index.xml', # MT
          'index.rss' # Slash
        ]
        outfeeds.extend(filter(isFeed, [urlparse.urljoin(fulluri, x) for x in suffixes]))

    if hasattr(__builtins__, 'set') or __builtins__.has_key('set'):
        outfeeds = list(set(outfeeds))
    return outfeeds

def store_feeds(feed_list):
    """
    Store the Feeds in the Feed collection in the database
    :param feed_list: LIst of feed URLs returned by feeds()
    """
    for f in feed_list:
        response = feedparser.parse(f)
        # insert only if is not already in the database
        res = FEEDS.find({"title_detail.base": f}, fields=["title_detail"])
        if not list(res):
            # Delete fields which cannot be serialized into BSON

            for k, v in response.feed.iteritems():
                # Convert to datetime instead of removing
                entry[k] = datetime.datetime.fromtimestamp(time.mktime(v))
            try:
                FEEDS.insert(response.feed, w=1)
            except DuplicateKeyError:
                print "Feed {} already in database".format(f)

def feed(uri):
    #todo: give preference to certain feed formats
    feedlist = feeds(uri)
    if feedlist:
        return feedlist[0]
    else:
        return None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Find feed links on a given url')
    parser.add_argument('urls', metavar='urls', type=str, nargs='+',
                       help='one or more urls to check')

    args = parser.parse_args()

    print "====Found %s Feeds"%len(args.urls)
    for u in args.urls:
        print "\n".join(feeds(u))
