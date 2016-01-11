import logging
import time
import constants as cns
from urllib.parse import urljoin
from redis import Redis
from pymongo import MongoClient
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)


def worker(options, use_lxml):
    """Воркер выполняет работу в отдельном процессе по парсингу страниц:
     -ищет ссылки, по которым можно получить новые данные и ставит их в очередь,
      которую читает фетчер
     -ищет данные по топикам, постам и темам форума и сохраняет их пачкой в БД.
     Так же ожидает команду от родительского процесса для завершения работы(выхода из цикла)"""

    mc = MongoClient(options.mongo_host, options.mongo_port)
    db = mc[cns.MONGO_DB]
    collection = db[cns.MONGO_COLLECTION]
    r_client = Redis(options.redis_host, options.redis_port)
    logger.info('Started. Mongo connection {}:{}. Redis connection {}:{}'.format(options.mongo_host, options.mongo_port,
                                                      options.redis_host, options.redis_port))

    def _put_urls(urls):
        """Вставка ссылок в очередь пачкой. Ссылки сохраняются
         в множество запрошенных, а в очередь только в него не входящие"""

        pipeline = r_client.pipeline()
        for url in urls:
            pipeline.sismember(cns.PARSED_URLS_KEY, url)
        members = pipeline.execute()

        urls = [url for url, ismember in zip(urls, members) if not ismember]
        if urls:
            pipeline.sadd(cns.PARSED_URLS_KEY, *urls)
            pipeline.lpush(cns.URL_QUEUE_KEY, *urls)
            pipeline.execute()
            logger.debug('Put new urls: {}'.format(urls))

    while True:
        start_time = time.time()
        data_key = r_client.brpop(cns.DATA_QUEUE_KEY, cns.BLOCKING_TIMEOUT)
        data_key = data_key[1].decode()
        if data_key == cns.FINISH_COMMAND:
            break
        try:
            data = r_client.get(data_key)
            r_client.delete(data_key)
            base_url = data_key.replace(cns.DATA_KEY_PREFIX, '')
            new_urls = []
            documents = []
            logger.info('Data is gotten')
            soup = BeautifulSoup(data.decode().replace('\n', ''), #todo убрать пробельные символы
                                 'lxml' if use_lxml else 'html.parser')
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
                for i in range(2, int(max_page) + 1): # todo: сделать множественную вставку ссылок
                    url = base_url.split('?')[0] + '?' + query_param_name + '=' + str(i)
                    new_urls.append(url)

            for subcategory in soup.select('li.child-forum'):
                d = {}
                descendants = list(d for d in subcategory.descendants if not isinstance(d, str))
                if not descendants: continue
                d['name'] = descendants[-1].get_text()
                d['description'] = descendants[-3].get_text()
                href = descendants[0].attrs['href']
                d['url'] = urljoin(base_url, href)
                documents.append(d)
                new_urls.append(d['url'])

            for topic in soup.select('tr.regular-topic'):
                d = {}
                a_tag = topic.find(class_='topic-title')
                d['url'] = urljoin(base_url, a_tag['href'])
                d['name'] = a_tag.get_text()
                d['author'] = topic.find(class_='author-cell').get_text()
                d['replies'] = topic.find(class_='reply-cell').get_text()
                d['views'] = topic.find(class_='view-cell').get_text()
                d['created'] = topic.find('meta', itemprop='dateCreated')['content']
                d['modified'] = topic.find('meta', itemprop='dateModified')['content']
                documents.append(d)
                new_urls.append(d['url'])

            for post in soup.select('div.post-interior'):
                d = {}
                d['number'] = post.select('a.post-index')[0].get_text()
                d['user'] = post.select('div.context-user > strong')[0].get_text()
                d['created'] = post.select('meta[itemprop=dateCreated]')[0].attrs['content']
                tag = post.select('div.post-rating')
                if tag:
                    d['rating'] = tag[0].get_text()
                d['text'] = post.select('div.post-content')[0].get_text()
                documents.append(d)

            _put_urls(new_urls)
            if not documents:
                logger.warning('No documents for url {}'.format(base_url))
            else:
                collection.insert_many(documents)

            end_time = time.time()
            logger.debug('Data is written. Duration: {}'.format(end_time-start_time))
        except:
            logger.exception('Parsing error for key {}'.format(data_key))

    logger.info('Stoped')
