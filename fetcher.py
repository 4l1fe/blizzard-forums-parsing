import os
import logging
import fake_useragent
import tornado.ioloop
import constants as cns
from concurrent.futures import ThreadPoolExecutor as TPE
from tornado.httpclient import AsyncHTTPClient
from tornado.concurrent import Future
from tornado.gen import coroutine


logger = logging.getLogger(__name__)

def fetcher(url_queue, data_queue, fetcher_concurrent, use_curl):
    pid = os.getpid()
    pool = TPE(fetcher_concurrent)
    logger.info('Fetcher is started.')

    @coroutine
    def main():

        @coroutine
        def fetch(i):
            while True:
                url = yield pool.submit(url_queue.get)
                if url == cns.FINISH_COMMAND:
                    logger.info('Concurrent {} is stoped'.format(i))
                    url_queue.task_done()
                    break
                logger.debug('Got url {}'.format(url))
                client = AsyncHTTPClient()
                user_agent = fake_useragent.UserAgent(cache=True).random #todo снести кэш-файл
                response = yield client.fetch(url, user_agent=user_agent)
                if response.body:
                    yield pool.submit(data_queue.put, (url, response.body))
                    logger.debug("Request time - {}. {}".format(response.request_time, url))
                url_queue.task_done()

        logger.info('Start {} concurrent requests'.format(fetcher_concurrent))
        completed = yield [fetch(i) for i in range(1, fetcher_concurrent+1)]

    if use_curl:
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient") # использует keep-alive
    tornado.ioloop.IOLoop.current().run_sync(main)
    logger.info('Stoped. IOloop shutdown.')
