#!/usr/bin/env python
"""
This Script uses the sphinxsearch index to obtain a list of words and their frequencies
"""
__author__ = 'fccoelho'

import argparse
import os



def generate_freqdist(dic, conf, ind, freq):
    """
    Generate freqdist using Sphinx's indexer
    :param dic: name of the file in which to save the freqdist
    :param conf: path to sphinx's configuration file
    :param ind: name of the index to analyse
    :param freq: frequency cutoff. Freqdist will contain only the `freq` most frequent words.
    """
    os.system("indexer --buildstops {} {} --buildfreqs {} -c {}".format(dic, freq, ind, conf))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=("Creates a file with the most common words in the index."))

    parser.add_argument("-d", "--dict", type=str, default="dict.txt", help="Dictionary file to be generated.")
    parser.add_argument("-c", "--conf", type=str, help="Path to the sphinx configuration file.")
    parser.add_argument("-i", "--index", type=str, help="Index to process.")
    parser.add_argument("-f", "--frequency", type=int, default=100000, help="Frequency cutoff to use.")

    args = parser.parse_args()
    generate_freqdist(args.dict, args.conf, args.index, args.frequency)
