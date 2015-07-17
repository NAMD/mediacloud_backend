import pymongo
import logging
import datetime
import requests
import settings

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

	params = (category, page)

	INDEX_URL = "http://{0}.estadao.com.br/ultimas/{1}".format(*params)

	index = requests.get(INDEX_URL).content
	soup = BeautifulSoup(index)
	news_index = soup.findAll("div", {"class":"listadesc"})
	news_urls = [url.contents[1]['href'] for url in news_index]
	
	return news_urls

def get_published_time(soup):
	""" Get the news published datetime 	

	:param soup: object with news html page
	:type soup: BeautifulSoup object
	:return: news published datetime
	:rtype: string

	"""

	MONTHS = {	u"Janeiro": u"Jan", u"Fevereiro": u"Fev", u"Mar\xe7o": u"Mar", u"Abril": u"Apr",
				u"Maio": u"May", u"Junho": u"Jun", u"Julho": u"Jul", u"Agosto": u"Aug",
				u"Setembro": u"Sep", u"Outubro": u"Oct", u"Novebro": u"Nov", u"Dezembro": u"Dec"
			 }

	time = soup.findAll("p", {"class":"data"})
	time = time[0].text.strip().split()
	del time[3]
	time[1] = MONTHS[time[1]]
	time[3] = time[3][0:2]
	time = '-'.join(time)
	if time is None:
		return None
	else:
		published_time = datetime.datetime.strptime(time, '%d-%b-%Y-%H-%M')

		return published_time


def clean_content(response_content):
	"""

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

def extract_title(soup):
	"""

	"""
	
	try:
		title = soup.findAll('h1', {'class':'titulo'})[0].text
	except:
		try:
			title = soup.findAll('h2', {'class':'subtitulo'})[0].text
		except:
			logger.error('wrong title tag or attribute')

	return title

def extract_content(url, response_content):
	""" Extract relevant information about news page

	:param url: news page's url
	:type url: string
	:return: compressed content, title, published time and body content of news page 
	:rtype: dict()

	"""
	
	cleaned_content = clean_content(response_content)
	
	if url.find("estadao.com.br/noticias/") is not -1:
		soupy = Soupy(cleaned_content).find("div", {"itemprop":"articleBody"})
		try:
			content = soupy.children.each(Q.text.strip()).filter(len).val()
		except:
			try:
				soupy = Soupy(cleaned_content).find("article")
				content = soupy.children.each(Q.text.strip()).filter(len).val()
			except:
				logger.error("wrong tags or attributes")
				return

	elif url.find("estadao.com.br/blogs/") is not -1:
		try:
			soupy = Soupy(cleaned_content).find("article")
			content = soupy.children.each(Q.text.strip()).filter(len).val()
		except:
			logger.error("wrong tags or attributes")
			return

	article = "".join(content) 
	return article
	
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

	logger.info("Downloading article: %s", url)
	try:
		response = requests.get(url, timeout=30)
	except ConnectionError:
		logger.error("Failed to fetch: %s", url)
		return
	except Timeout:
		logger.error("Timed out while fetching %s", url)
		return
	
	encoding = response.encoding if response.encoding is not None else 'utf8'
	response_content = response.content.decode(encoding)
	soup = BeautifulSoup(response_content)
		
	article = {}
	article['link_content'] = compress_content(response_content)
	article['compressed'] = True
	article['title'] = extract_title(soup)
	article['body_content'] = extract_content(url, response_content)

	return article


for i in range(1, 10):
	for url in find_articles(u'politica',i):
		article = download_article(url)
		print(article['body_content'])
