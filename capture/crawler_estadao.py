import sys
import pymongo
import logging
import datetime
import requests
import settings

from goose import Goose
from soupy import Soupy, Q
from bs4 import BeautifulSoup
from logging.handlers import RotatingFileHandler
from downloader import compress_content, detect_language

###########################
#  Setting up Logging
###########################

logger = logging.getLogger("Estadao")
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = RotatingFileHandler('/tmp/mediacloud_estadao.log', maxBytes=5e6, backupCount=3)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)  # uncomment for console output of messages
logger.addHandler(fh)

client = pymongo.MongoClient(settings.MONGOHOST, 27017)
MCDB = client.MCDB
ARTICLES = MCDB.articles  # Article Collection
ARTICLES.ensure_index("source")


def find_articles(category, page=1):
	"""Get the urls of last news and its categories

	:param category: the category of the news (politica, economia, internacional, esportes, sao-paulo, cultura)
	:param page: the page number of last news
	:type category: string
	:type page: integer
	:return: last news with its categories
	:rtype: list()

	"""

	CATEGORIES = (u'politica', u'economia', u'internacional', u'esportes', u'sao-paulo', u'cultura', u'opiniao',
				  u'alias', u'brasil', u'ciencia', u'educacao', u'saude', u'sustentabilidade', u'viagem')

	if category not in CATEGORIES:
		raise ValueError("Category value not accepted.")

	INDEX_URL = "http://{0}.estadao.com.br/ultimas/{1}".format(category, page)

	index = requests.get(INDEX_URL).content
	soup = BeautifulSoup(index)
	news_index = soup.findAll("div", {"class":"listadesc"})
	news_urls = [url.contents[1]['href'] for url in news_index]
	
	return news_urls

def clean_content(response_content):
	""" For getting only the news' content is necessary to remove dirty elements like script,
		iframe, figcaption tags and related news reference in content's body.
	"""
	
	soup = BeautifulSoup(response_content)
				
	for tag in soup.find_all("header"):
		tag.decompose()
	for tag in soup.find_all("div", {"class":"wp-caption"}):
		tag.decompose()
	for tag in soup.find_all("div", {"class":"tags"}):
		tag.decompose()
	for tag in soup.find_all('code'):
		tag.decompose()
	for tag in soup.find_all('iframe'):
		tag.decompose()
	for tag in soup.find_all('div', {'class':'relacionadastexto'}):
		tag.decompose()
	for tag in soup.find_all('figcaption'):
		tag.decompose()
	for tag in soup.find_all('script'):
		tag.decompose()
	return soup.html

def extract_published_time(url, soup):
	""" Get the news published datetime 	

	:param soup: object with news html page
	:type soup: BeautifulSoup object
	:return: news published datetime
	:rtype: string

	"""

	MONTHS = {	u"janeiro": u"Jan", u"fevereiro": u"Fev", u"mar\xe7o": u"Mar", u"abril": u"Apr",
				u"maio": u"May", u"junho": u"Jun", u"julho": u"Jul", u"agosto": u"Aug",
				u"setembro": u"Sep", u"outubro": u"Oct", u"novembro": u"Nov", u"dezembro": u"Dec"
			 }
	try:
		date = soup.findAll("p", {"class":"data"})[0]
	except IndexError:
		try:
			date = soup.findAll("span", {"class":"data"})[0]
		except IndexError:
			logger.error('wrong date tags')
			return None


	try:
		date = date.text.strip().split()
		date[1] = date[1].lower()
		date[1] = MONTHS[date[1]]
		
		if "estadao.com.br/noticias/" in url:
			date = date[0:3] + date[4:6]
			date[3] = date[3][0:2]
			
		elif "estadao.com.br/blogs/" in url:
			date[3] = date[4][0:2]
			date[4] = date[4][3:5]
	except ValueError:
		logger.error('wrong data extraction')
		return None
	
	date = '-'.join(date)
	
	try:
		published_time = datetime.datetime.strptime(date, '%d-%b-%Y-%H-%M')
		return published_time 
	except ValueError:
		logger.error('wrong published time format')
		return

def extract_title(article):
	""" Extract the news title.

	"""
	
	try:
		title = article.title
	except:
		logger.error('error in extract article title')
		return

	return title

def extract_content(article):
	""" Extract relevant information about news page

	"""
	
	try:
		body_content = article.cleaned_text
	except:
		logger.error("error in extract article content")
		return

	return body_content
	
def download_article(url):
	""" Download the html content of a news page

	:param url: news page's url
	:type url: string
	:return: news page's content 
	:rtype: requests.models.Response

	"""

	article = {
		'link': url,
		'source': 'crawler_estadao'
	}

	logger.info("Downloading article: {0}".format(url))

	try:
		response = requests.get(url, timeout=30)
	except ConnectionError:
		logger.error("Failed to fetch:{0}".format(url))
		return
	except Timeout:
		logger.error("Timed out while fetching {0}".format(url))
		return
	
	encoding = response.encoding if response.encoding is not None else 'utf8'
	response_content = response.content.decode(encoding)
	soup = BeautifulSoup(response_content)

	g = Goose({'use_meta_language': False, 'target_language':'pt'})
	news = g.extract(url=url)
		
	article = {}
	article['link_content'] = compress_content(response_content)
	article['compressed'] = True
	article['title'] = extract_title(news)
	article['body_content'] = extract_content(news)
	article['published_time'] = extract_published_time(url, soup)

	return article

if __name__ == '__main__':
	for url in find_articles(sys.argv[1]):
	    exists = list(ARTICLES.find({"link": url}))
	    if not exists:
	        article = download_article(url)
	        ARTICLES.insert(article, w=1)

# example
# for i in range(0,10):
# 	for url in find_articles(u'esportes', page = i):
# 		article = download_article(url)
# 		print(article['title'])