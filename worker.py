import logging
import time
import os
import tornado.ioloop
from pymongo import MongoClient
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from tornado.gen import coroutine


logger = logging.getLogger(__name__)


def worker(url_queue, data_queue, url_cache, use_lxml):
    pid = os.getpid()
    w_id = 'Worker[pid={}]'.format(pid)
    logger.info('{} is started.'.format(w_id))

    mc = MongoClient('localhost', 27017)
    db = mc['hearthstone']
    collection = db['mage']
    logger.info('{} is connected to mongo {}'.format(w_id, mc.address))

    while True:
        start_time = time.time()
        base_url, data = data_queue.get()
        documents = []
        logger.info('{} data is gotten'.format(w_id))
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
                if url not in url_cache:
                    url_cache.add(url)
                    logger.info('{} put new url {}'.format(w_id, url))
                    url_queue.put(url)

        # парсинг данных, новыхх ссылок и запись в БД
        for subcategory in soup.select('li.child-forum'):
            d = {}
            descendants = list(d for d in subcategory.descendants if not isinstance(d, str))
            if not descendants: continue
            d['name'] = descendants[-1].get_text()
            d['href'] = descendants[0].attrs['href']
            d['description'] = descendants[-3].get_text()
            url = urljoin(base_url, d['href'])
            if url not in url_cache:
                url_cache.add(url)
                logger.info('{} put new url {}'.format(w_id, url))
                url_queue.put(url)
            documents.append(d)

        for topic in soup.select('tr.regular-topic'):
            d = {}
            d['name'] = topic.contents[1].get_text()
            d['author'] = topic.contents[2].get_text()
            d['replies'] = topic.contents[3].get_text()
            d['views'] = topic.contents[4].get_text()
            d['replies'] = topic.select('td.title-cell > meta[itemprop=dateModified]')[0].attrs['content']
            documents.append(d)

        collection.insert_many(documents)
        data_queue.task_done()

        end_time = time.time()
        logger.info('{} data is written. Duration - {}'.format(w_id, end_time-start_time))

    logger.info('{} is stoped'.format(w_id))
