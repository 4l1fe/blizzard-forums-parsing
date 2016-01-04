import logging
import time
import constants as cns
from redis import Redis
from pymongo import MongoClient
from urllib.parse import urljoin
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)


def worker(options, use_lxml):

    mc = MongoClient(options.mongo_host, options.mongo_port)
    db = mc[cns.MONGO_DB]
    collection = db[cns.MONGO_COLLECTION]
    r_client = Redis(options.redis_host, options.redis_port)
    logger.info('Started. Mongo connection {}. Redis connection {}'.format((options.mongo_host, options.mongo_port),
                                                      (options.redis_host, options.redis_port)))

    def _put_url(url):
        if not r_client.sismember(cns.PARSED_URLS_KEY, url):
            pipeline = r_client.pipeline()
            pipeline.sadd(cns.PARSED_URLS_KEY, url)
            pipeline.lpush(cns.URL_QUEUE_KEY, url)
            pipeline.execute()
            logger.debug('Put new url {}'.format(url))

    while True:
        start_time = time.time()
        data_key = r_client.rpop(cns.DATA_QUEUE_KEY)
        if not data_key:
            continue
        data_key = data_key.decode()
        if data_key == cns.FINISH_COMMAND:
            break

        try:
            data = r_client.get(data_key)
            r_client.delete(data_key)
            base_url = data_key.replace(cns.DATA_KEY_PREFIX, '')
            documents = []
            logger.info('Data is gotten')
            soup = BeautifulSoup(data.decode().replace(r'\n', ''), #todo убрать пробельные символы
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

            # парсинг данных, новых ссылок и запись в БД
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
        except:
            logger.exception('Parsing error for key {}'.format(data_key))

        end_time = time.time()
        logger.debug('Data is written. Duration: {}'.format(end_time-start_time))

    logger.info('Stoped')
