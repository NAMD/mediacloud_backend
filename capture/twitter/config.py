# coding=utf-8
import os

# To use this module create the file below in your home directory and put the required keys there, in the order specified below.
secrets_file = os.path.expanduser("~/.twitter_keys")
with open(secrets_file)as f:
    key_1, key_2, key_3, key_4 = f.readlines()

access_token_key = key_1.strip()
access_token_secret = key_2.strip()
consumer_key = key_3.strip()
consumer_secret = key_4.strip()

MONGO_HOST = '172.16.4.51'

TRACK = [
    'dengue',
    'aedes aegypti',
    'manifesta',
]
