import logging
import time
import os
import signal
import constants as cns
from collections import defaultdict
from urllib.parse import urljoin
from redis import Redis
from pymongo import MongoClient
from bs4 import BeautifulSoup
from tree import Tree, Node


logger = logging.getLogger(__name__)


def worker(options, stop_flag, use_lxml=False):
    """Воркер выполняет работу в по парсингу страниц и созданию новых ссылок.

    -Ищет ссылки, по которым можно получить новые данные и ставит их в очередь,
     которую читает фетчер.
    -Ищет данные по топикам, постам, темам форума и сохраняет их пачкой в БД.
    """

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    mc = MongoClient(options.mongo_host, options.mongo_port)
    db = mc[cns.MONGO_DB]
    collection = db[cns.MONGO_COLLECTION]
    r_client = Redis(options.redis_host, options.redis_port)
    tree = Tree(r_client)
    pid = os.getpid()
    logger.info('Started. Mongo connection {}:{}. Redis connection {}:{}'.format(options.mongo_host, options.mongo_port,
                                                                                options.redis_host, options.redis_port))

    def put_urls(urls):
        """Вставка ссылок в очередь пачкой. Ссылки сохраняются
        в множество запрошенных, а в очередь только в него не входящие.
        """

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

    documents = []
    while not stop_flag.value:
        start_time = time.time()
        child_nodes = []
        new_urls = []
        try:
            data_key = r_client.brpop(cns.DATA_QUEUE_KEY, cns.BLOCKING_TIMEOUT)
            if not data_key:
                logger.info('Continue after an expired blocking')
                continue
            data_key = data_key[1].decode()
            with r_client.pipeline() as pipeline:
                pipeline.get(data_key)
                pipeline.delete(data_key)
                data, del_count = pipeline.execute()
            base_url = data_key.replace(cns.DATA_KEY_PREFIX, '')
            logger.info('Data is gotten')

            soup = BeautifulSoup(data.decode(), 'lxml' if use_lxml else 'html.parser')

            for post in soup.find_all(class_='TopicPost'):
                document = defaultdict(dict)
                document['post']['id'] = post['id']
                document['post']['user'] = post.find(class_='Author-name').get_text()
                document['post']['created'] = post.find(class_='TopicPost-timestamp')['data-tooltip-content']
                document['post']['rank'] = post.find(class_='TopicPost-rank').get_text()
                document['post']['text'] = post.find(class_='TopicPost-bodyContent').get_text()
                for node in tree.get_parents(base_url):
                    if node.level == cns.NODE_TOPIC_LEVEL:
                        document['topic'] = node.data
                    elif node.level == cns.NODE_SUBCATEGORY_LEVEL:
                        document['subcategory'] = node.data
                documents.append(document)

            for topic in soup.find_all(class_='ForumTopic'):
                d = {}
                d['url'] = urljoin(base_url, topic['href'])
                d['name'] = topic.find(class_='ForumTopc-heading').get_text()
                d['author'] = topic.find(class_='ForumTopic-author').get_text()
                d['replies'] = topic.find(class_='ForumTopic-replies').get_text()
                n = Node(position=d['url'], data=d, level=cns.NODE_TOPIC_LEVEL, parent=base_url)
                child_nodes.append(n)
                new_urls.append(d['url'])

            pagination = soup.find(class_='Topic-pagination--header').find_all(class_='Pagination-button--ordinal')
            if pagination:
                last_page = int(pagination[-1]['data-page-number'])
                urls = [base_url+'?page='+str(n) for n in range(1, last_page+1)]
                new_urls.append(urls)

            for subcategory in soup.find_all(class_='ForumCard'):
                href = subcategory['href']
                d = {}
                d['url'] = urljoin(base_url, href)
                d['name'] = subcategory.find(class_='ForumCard-heading').get_text()
                d['description'] = subcategory.find(class_='ForumCard-description').get_text()
                n = Node(position=d['url'], data=d, level=cns.NODE_SUBCATEGORY_LEVEL, parent=base_url)
                child_nodes.append(n)
                new_urls.append(d['url'])

            tree.add_nodes(child_nodes)
            put_urls(new_urls)

            if not documents:
                logger.warning('No documents for url {}'.format(base_url))
            elif len(documents) >= cns.INSERT_BATCH_SIZE:
                collection.insert_many(documents)
                documents.clear()
                logger.info('Data is written')

            end_time = time.time()
            logger.debug('Job duration: {}'.format(end_time-start_time))
        except:
            logger.exception('Work error')

    if documents:
        try:
            collection.insert_many(documents)
        except:
            logger.exception('Final insertion error')

    logger.info('Stopped')
