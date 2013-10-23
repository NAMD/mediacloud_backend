Mediacloud Backend
==================

MediaCloud backend repository to serve as base to MediaCloud repository.


##Quick Start

This library has the following features implemented so far:

### Search for urls which contain links to feeds

```
$ capture/googlerss.py [-s <subject>]
```
This command searches google for pages matching "RSS" and "<subject>". returning many pages of resoults. Don't run this
too often if you don't want to get your IP banned. If subject is ommited a general search for "RSS" is done. This script
only searches for sites in Brazil (domain ending with .br).

The urls found are inserted in a collection called URLS, which can be used by the *extract_feeds.py* script.

### Search for Feeds on URLs

Given a text file with one url per line

```
$ capture/extract_feeds.py -d3 URLS.txt
```
If the file containing the URLs is omitted, the URL collection is scanned instead. Each URL is scanned by httrack,
returning all the feeds found, which are then inserted in the feeds collection. Feeds are checked for duplicates before
insertion into the collection.

Before you do this, edit capture/settings.py and define the MongoDb server in which the feeds should be stored.

### Download Articles from Feeds stored in MCDB.feeds

Articles downloaded will be stored in collection MCDB.Articles

```
$ capture/dowloader.py 
```
multi-threaded downloads will ensue for every feed in the feed collection. Insertion in the database checks for
duplicate urls and doesn't re-insert them. We recomend that this is run as a cron job every so often.
