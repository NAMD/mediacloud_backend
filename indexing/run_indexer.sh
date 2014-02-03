#!/bin/sh

#testing config
mongo-connector -m localhost:27017 -t http://200.20.164.152:8983/solr/mediacloud -o oplog_progress.txt -n MCDB.articles,MCDB.feeds -d ./solr_doc_manager.py
#production
#mongo-connector -m 172.16.4.51:27017 -t http://172.16.4.52:8983/solr -o oplog_progress.txt -n MCDB.articles, MCDB.feeds -u _id  -a admin -d ./solr_doc_manager.py
