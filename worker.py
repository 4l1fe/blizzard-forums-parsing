import logging
import time
import signal
import constants as cns
from collections import defaultdict
from urllib.parse import urljoin
from redis import Redis
from pymongo import MongoClient
from bs4 import BeautifulSoup
from tree import Tree, Node
from multiprocessing import Process


logger = logging.getLogger(__name__)


class Worker(Process):

    YET_NO_DATA = (None, None)

    def __init__(self, options, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.stop_flag = options.stop_flag
        self.use_lxml = options.use_lxml

        self.mc = MongoClient(options.mongo_host, options.mongo_port)
        db = self.mc[cns.MONGO_DB]
        self.collection = db[cns.MONGO_COLLECTION]

        self.rc = Redis(options.redis_host, options.redis_port)
        self.tree = Tree(self.rc)

        logger.info('Mongo connection {}:{}. Redis connection {}:{}'\
                    .format(options.mongo_host, options.mongo_port, options.redis_host, options.redis_port))

    def run(self):
        logger.info('Started')
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        self.documents = []  # to collect during a whole lifecircle for a batching
        while not self.stop_flag.value:
            try:
                start_time = time.time()

                data, base_url = self.get_data_and_base_url()
                if (data, base_url) == self.YET_NO_DATA:
                    logger.info('Continue after waiting data getting. Blocking is expired')
                    continue
                soup = BeautifulSoup(data.decode(), 'lxml' if self.use_lxml else 'html.parser')
                self.parse_posts_fill_documents(soup, base_url)
                self.parse_topics(soup, base_url)
                self.parse_cards(soup, base_url)
                self.save_documents(base_url)

                end_time = time.time()
                logger.debug('Job duration: {}'.format(end_time-start_time))
            except:
                logger.exception('Work error')

        self.save_documents(finish=True)

        logger.info('Stopped')

    def get_data_and_base_url(self):
        queuekey_datakey = self.rc.brpop(cns.DATA_QUEUE_KEY, cns.BLOCKING_TIMEOUT)
        if not queuekey_datakey:
            return self.YET_NO_DATA

        data_key = queuekey_datakey[1].decode()
        with self.rc.pipeline() as pipeline:
            pipeline.get(data_key)
            pipeline.delete(data_key)
            data, del_count = pipeline.execute()
        base_url = self._extract_base_url(data_key)
        logger.info('Data is gotten')
        return data, base_url

    def parse_cards(self, soup, base_url):
        child_nodes = []
        new_urls = []
        for card_source in soup.find_all(class_='ForumCard'):
            href = card_source['href']
            card = {}
            card['url'] = urljoin(base_url, href)
            name = card_source.find(class_='ForumCard-heading')
            if name:
                card['name'] = name.get_text()
            description = card_source.find(class_='ForumCard-description')
            if description:
                card['description'] = description.get_text()

            n = Node(position=card['url'], data=card, level=cns.NODE_SUBCATEGORY_LEVEL, parent=base_url)
            child_nodes.append(n)
            new_urls.append(card['url'])

        self.add_child_nodes(child_nodes)
        self.add_new_urls(new_urls)

    def parse_topics(self, soup, base_url):
        self._add_paginated_links(soup, base_url)

        child_nodes = []
        new_urls = []
        for topic_source in soup.find_all(class_='ForumTopic'):
            topic = {}
            topic['url'] = urljoin(base_url, topic_source['href'])
            topic['name'] = topic_source.find(class_='ForumTopic-heading').get_text()
            topic['author'] = topic_source.find(class_='ForumTopic-author').get_text()
            topic['replies'] = topic_source.find(class_='ForumTopic-replies').get_text()

            n = Node(position=topic['url'], data=topic, level=cns.NODE_TOPIC_LEVEL, parent=base_url)
            child_nodes.append(n)
            new_urls.append(topic['url'])

        self.add_child_nodes(child_nodes)
        self.add_new_urls(new_urls)

    def parse_posts_fill_documents(self, soup, base_url):
        for post_source in soup.find_all(class_='TopicPost'):
            document = defaultdict(dict)
            document['post']['id'] = post_source['id']
            document['post']['user'] = post_source.find(class_='Author-name').get_text()
            document['post']['created'] = post_source.find(class_='TopicPost-timestamp')['data-tooltip-content']
            document['post']['rank'] = post_source.find(class_='TopicPost-rank').get_text()
            document['post']['text'] = post_source.find(class_='TopicPost-bodyContent').get_text()
            for node in self.tree.get_parents(base_url):
                if node.level == cns.NODE_TOPIC_LEVEL:
                    document['topic'] = node.data
                elif node.level == cns.NODE_SUBCATEGORY_LEVEL:
                    document['subcategory'] = node.data
            self.documents.append(document)

    def add_child_nodes(self, nodes):
        self.tree.add_nodes(nodes)

    def add_new_urls(self, urls):
        """Вставка ссылок в очередь пачкой. Ссылки сохраняются
        в множество запрошенных, а в очередь только в него не входящие.
        """

        pipeline = self.rc.pipeline()
        for url in urls:
            pipeline.sismember(cns.PARSED_URLS_KEY, url)
        members = pipeline.execute()

        urls = [url for url, ismember in zip(urls, members) if not ismember]
        if urls:
            pipeline.sadd(cns.PARSED_URLS_KEY, *urls)
            pipeline.lpush(cns.URL_QUEUE_KEY, *urls)
            pipeline.execute()
            logger.debug('Put new urls: {}'.format(urls))
        pipeline.reset()

    def save_documents(self, base_url=None, finish=False, batch_size=cns.INSERT_BATCH_SIZE):
        if len(self.documents) >= batch_size:
            self.collection.insert_many(self.documents)
            self.documents.clear()
            logger.info('Data is written')
        elif finish and self.documents:
            self.collection.insert_many(self.documents)
            logger.info('Finished step. Data is written')
        elif not self.documents:
            logger.warning('No documents for url {}'.format(base_url))

    def _extract_base_url(self, key):
        return key.replace(cns.DATA_KEY_PREFIX, '')

    def _add_paginated_links(self, soup, base_url):
        new_urls = []
        pagination = soup.select('.Topic-pagination--header .Pagination-button--ordinal')
        if pagination:
            last_page = int(pagination[-1]['data-page-number'])
            urls = [urljoin(base_url, '?page='+str(n)) for n in range(1, last_page+1)]
            new_urls.extend(urls)
        self.add_new_urls(new_urls)
