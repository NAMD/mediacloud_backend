#!/usr/bin/env python
"""
This script calculates the "hot topic of the day". By looking at all articles published on a given day.
"""
__author__ = 'fccoelho'

import argparse
import datetime

Today = datetime.datetime.today()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=("Calculate 'Hot Topics of the Day'.\nA ranking of the most mentioned subjects"))

    parser.add_argument("-d", "--date", type=str, default="{}-{}-{}".format(Today.year, Today.month, Today.day),
            help="Date to Analyse in YEAR-MO-DD format")