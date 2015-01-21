import requests
from requests.exceptions import ConnectionError, Timeout

from bs4 import BeautifulSoup
from downloader import compress_content, detect_language

def download_article(url, source, published_time_extractor, logger):
    """
    This function takes a url, a source name, a function that knows how to
    extract the published time from a BeautifulSoup object and a logger. It
    returns a dictionary with the necessary attributes of an article suitable
    for saving in the `MCDB.articles` collection.
    """
    article = {
        'link': url,
        'source': source,
    }
    logger.info("Downloading article: %s", url)
    try:
        response = requests.get(url, timeout=30)
    except ConnectionError:
        logger.error("Failed to fetch %s", url)
        return
    except Timeout:
        logger.error("Timed out while fetching %s", url)
        return

    encoding = response.encoding if response.encoding is not None else 'utf8'
    dec_content = response.content.decode(encoding)
    article['link_content'] = compress_content(dec_content)

    article['compressed'] = True
    article['language'] = detect_language(dec_content)


    soup = BeautifulSoup(dec_content)
    article['title'] = soup.find('title').text.strip()

    article['published'] = published_time_extractor(soup)

    return article
