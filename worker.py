import logging
import time
import os
import constants as cns
import redis
from pymongo import MongoClient
from urllib.parse import urljoin
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)


def worker(url_queue, data_queue, url_cache, use_lxml):

    def _put_url(url):
        if url not in url_cache:
            url_cache.add(url)
            logger.debug('Put new url {}'.format(url))
            url_queue.put(url)

    pid = os.getpid()
    mc = MongoClient('localhost', 27017)
    db = mc['hearthstone']
    collection = db['posts']
    logger.info('Started. Mongo connection {}'.format(mc.address))

    while True:
        start_time = time.time()
        data = data_queue.get()
        if data == cns.FINISH_COMMAND:
            data_queue.task_done()
            break
        base_url, data = data
        documents = []
        logger.info('Data is gotten')
        soup = BeautifulSoup(data.decode().replace('\n', ''), #todo убрать пробельные символы
                             'lxml' if use_lxml else 'html.parser')

        # парсинг новых ссылок из пагинации
        pagination = soup.select('ul.ui-pagination')
        if pagination:
            pagination = pagination[0]
            tag = pagination.find('a')
            query_param_name = tag.attrs['href'].split('=')[0][1:]
            last_child = -1
            max_page = pagination.contents[last_child].string
            while not max_page.isdigit():
                last_child -= 1
                max_page = pagination.contents[last_child].string
            for i in range(2, int(max_page) + 1):
                url = base_url.split('?')[0] + '?' + query_param_name + '=' + str(i)
                _put_url(url)

        # парсинг данных, новыхх ссылок и запись в БД
        for subcategory in soup.select('li.child-forum'):
            d = {}
            descendants = list(d for d in subcategory.descendants if not isinstance(d, str))
            if not descendants: continue
            d['name'] = descendants[-1].get_text()
            d['description'] = descendants[-3].get_text()
            href = descendants[0].attrs['href']
            url = urljoin(base_url, href)
            d['url'] = url
            _put_url(url)
            documents.append(d)

        for topic in soup.select('tr.regular-topic'):
            d = {}
            d['name'] = topic.contents[1].get_text()
            d['author'] = topic.contents[2].get_text()
            d['replies'] = topic.contents[3].get_text()
            d['views'] = topic.contents[4].get_text()
            d['modified'] = topic.select('td.title-cell > meta[itemprop=dateModified]')[0].attrs['content']
            href = topic.select('td.title-cell > a')[0].attrs['href']
            url = urljoin(base_url, href)
            d['url'] = url
            _put_url(url)
            documents.append(d)

        for post in soup.select('div.post-interior'):
            d = {}
            d['number'] = post.select('a.post-index')[0].get_text()
            d['user'] = post.select('div.context-user > strong')[0].get_text()
            d['created'] = post.select('meta[itemprop=dateCreated]')[0].attrs['content']
            tag = post.select('div.post-rating')
            if tag:
                d['rating']  = tag[0].get_text()
            d['text'] = post.select('div.post-content')[0].get_text()
            documents.append(d)

        if not documents:
            logger.warning('No documents for url {}'.format(base_url))
        else:
            collection.insert_many(documents)
        data_queue.task_done()

        end_time = time.time()
        logger.debug('Data is written. Duration: {}'.format(end_time-start_time))

    logger.info('Stoped')
