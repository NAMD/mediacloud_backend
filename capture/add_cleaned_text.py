import logging
import pymongo
import sys
import goose
import pymongo
import zlib
import pickle as CP

import config


# This is not defined in config.py because this log is specific to this tool,
# and this should be run only once per database.

LOG_FILE = '/tmp/migrate_tweet_date_to_datetime.log'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=logging.INFO)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger = logging.getLogger()
logger.addHandler(stdout_handler)

mongo_client = pymongo.MongoClient(config.MONGO_HOST)
collection = mongo_client.MCDB.articles

cursor = collection.find({'cleaned_text': {'$exists': False}},
        timeout=False)

total = cursor.count()
if total == 0:
    logging.info('There are no articles to update \o/')
    sys.exit(0)

logging.info("We have {:d} articles to update.".format(total))
i = 0
for article in cursor:
    try:
        decompressed = zlib.decompress(article['link_content'])
        orig_html = CP.loads(decompressed)
        if len(orig_html.strip()) == 0:
            logging.info('orig_html empty %s', article['_id'])
            cleaned_text = ''
        else:
            cleaned_text = goose.Goose({'enable_image_fetching': False, 'use_meta_language': False,
                                    'target_language': 'pt'}).extract(raw_html= orig_html).cleaned_text
        if len(cleaned_text) == 0:
            if article.has_key('summary'):
                cleaned_text = article['summary']
        collection.update({'_id': article['_id']}, {'$set':
            {'cleaned_text': cleaned_text}})
    except Exception as e:
        logger.exception('_id: %s [Exception] %s', article['_id'], e)
        collection.update({'_id': article['_id']}, {'$set':
            {'cleaned_text': article['summary']}})
    i += 1
    if (i % 1000) == 0:
        logging.info('{:010d}/{:010d} articles updated.'.format(i, total))

cursor.close()