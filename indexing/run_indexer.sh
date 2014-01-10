#!/usr/bin/sh

mongo-connector -m 172.16.4.51:27217 -t http://172.16.4.52:4001/solr -o oplog_progress.txt -n alpha.foo,test.test -u _id -k auth.txt -a admin -d ./solr_doc_manager.py
