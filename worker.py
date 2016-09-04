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
                pipeline.get(data_key)  #TODO pipeline
                pipeline.delete(data_key)
                data, del_count = pipeline.execute()
            base_url = data_key.replace(cns.DATA_KEY_PREFIX, '')
            logger.info('Data is gotten')

            soup = BeautifulSoup(data.decode(), 'lxml' if use_lxml else 'html.parser')
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
                    new_urls.append(url)

            for subcategory in soup.select('li.child-forum'):
                d = {}
                descendants = list(d for d in subcategory.descendants if not isinstance(d, str))
                if not descendants: continue
                href = descendants[0].attrs['href']
                d['url'] = urljoin(base_url, href)
                d['name'] = descendants[-1].get_text()
                d['description'] = descendants[-3].get_text()
                n = Node(position=d['url'], data=d, level=cns.NODE_SUBCATEGORY_LEVEL, parent=base_url)
                child_nodes.append(n)
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
                n = Node(position=d['url'], data=d, level=cns.NODE_TOPIC_LEVEL, parent=base_url)
                child_nodes.append(n)
                new_urls.append(d['url'])

            for post in soup.select('div.post-interior'):
                document = defaultdict(dict)
                document['post']['number'] = post.select('a.post-index')[0].get_text()
                document['post']['user'] = post.select('div.context-user > strong')[0].get_text()
                document['post']['created'] = post.select('meta[itemprop=dateCreated]')[0].attrs['content']
                tag = post.select('div.post-rating')
                if tag:
                    document['post']['rating'] = tag[0].get_text()
                document['post']['text'] = post.select('div.post-content')[0].get_text()
                for node in tree.get_parents(base_url):
                    if node.level == cns.NODE_TOPIC_LEVEL:
                        document['topic'] = node.data
                    elif node.level == cns.NODE_SUBCATEGORY_LEVEL:
                        document['subcategory'] = node.data
                documents.append(document)

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
