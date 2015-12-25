import os
import logging
import fake_useragent
import tornado.ioloop
from concurrent.futures import ThreadPoolExecutor as TPE
from tornado.httpclient import AsyncHTTPClient
from tornado.concurrent import Future
from tornado.gen import coroutine


logger = logging.getLogger(__name__)


def fetcher(url_queue, data_queue, fetcher_concurrent, use_curl):
    pid = os.getpid()
    f_id = 'Fetcher[pid={}]'.format(pid)
    logger.info('{} is started.'.format(f_id))
    pool = TPE(fetcher_concurrent)

    @coroutine
    def main():

        @coroutine
        def fetch():
            while True:
                url = yield pool.submit(url_queue.get)
                logger.debug('{} got url {}'.format(f_id, url))
                client = AsyncHTTPClient()
                user_agent = fake_useragent.UserAgent(cache=True).random #todo снести кэш-файл
                response = yield client.fetch(url, user_agent=user_agent)
                if response.body:
                    yield pool.submit(data_queue.put, (url, response.body))
                    logger.debug("{} request time - {}. {}".format(f_id, response.request_time, url))
                else:
                    logger.info('{} is stoped. No data.'.format(f_id))
                    break
                url_queue.task_done()

        logger.info('{} starts {} concurrent requests'.format(f_id, fetcher_concurrent))
        for _ in range(fetcher_concurrent):
            fetch()

        f = Future()
        yield f # бесконечное ожидание

    if use_curl:
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient") # использует keep-alive
    tornado.ioloop.IOLoop.current().run_sync(main)
    logger.info('{} is stoped. IOloop shutdown.'.format(f_id))
