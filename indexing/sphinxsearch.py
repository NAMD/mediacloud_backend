#-*- coding:utf-8 -*-
u"""
Created on 11/04/14
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'

import argparse
import sphinxapi


client = sphinxapi.SphinxClient()


def do_query(q, ind):
    """
    very simple query function to test the indexing
    :param q: query expression
    :param ind: index to search
    :return: dictionary with the results
    """
    return client.Query(query=" ".join(q), index=ind)

def get_full_docs(id_list):
    """
    Fetch the docs from mongodb corresponding to the ids in id_list
    :param id:
    :return:
    """

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Perform a query on sphinxsearch and return it as a json object')
    parser.add_argument('--host', '-H', type=str, default='127.0.0.1', help="Searchd Host")
    parser.add_argument('--port', '-p', type=int, default=9312, help="port")
    parser.add_argument('--index', '-i', required=False, default="MCDB_ARTICLES", help="Index")
    parser.add_argument('--query', '-q', required=True, type=str, nargs="+", help="query words")
    args = parser.parse_args()

    client.SetServer(args.host, args.port)

    r = do_query(args.query, args.index)
    print r

