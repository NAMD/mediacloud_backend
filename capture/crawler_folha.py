from bs4 import BeautifulSoup
import requests

INDEX_URL = "http://www1.folha.uol.com.br/ultimas-noticias/index.shtml"

index = requests.get(INDEX_URL).content
soup = BeautifulSoup(index)
news_index = soup.find(**{'class': 'news-index'}).find('ol')
news_urls = set([n.attrs['href'] for n in news_index.find_all('a')])
