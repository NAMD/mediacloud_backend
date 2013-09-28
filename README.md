mediacloud_backend
==================

MediaCloud backend repository to serve as base to MediaCloud repository.


##Quick Start

This library has the following features implemented so far:

### Search for Feeds on URLs

Given a text file with one url per line

```
$ capture/extract_feeds.py -d3 URLS.txt
```

Before you do this, edit capture/settings.py and define the MongoDb server in which the feeds should be stored.

### Download Articles from Feeds stored in MCDB.feeds

Articles downloaded will be stored in collection MCDB.Articles

```
$ capture/dowloader.py 
```
multi-threaded downloads will ensue for every feed in the feed collection. Insertion in the database checks for duplicate urls and doesn't re-insert them.
