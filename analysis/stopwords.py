#!/usr/bin/env python
"""
This Script uses the sphinxsearch index to obtain a list of stopwords, based on frequency analyses
"""
__author__ = 'fccoelho'


import argparse
import os

def generate_sw(dic, conf, ind, freq):
    os.system("indexer --buildstops {} {} --buildfreqs {} -c {}".format(dic, freq, ind, conf))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=("Creates a file with the most common words in the index."))

    parser.add_argument("-d", "--dict", type=str, default="dict.txt", help="Dictionary file to be generated.")
    parser.add_argument("-c", "--conf", type=str, help="Path to the sphinx configuration file.")
    parser.add_argument("-i", "--index", type=str, help="Index to process.")
    parser.add_argument("-f", "--frequency",type=int, default=100000, help="Frequency cutoff to use.")

    args = parser.parse_args()
    generate_sw(args.dict, args.conf, args.index, args.frequency)