import os
import logging
import signal
import fake_useragent
import tornado.ioloop
import tornadoredis
import constants as cns
from tornado.httpclient import AsyncHTTPClient
from tornado.gen import coroutine, Task


logger = logging.getLogger(__name__)


def fetcher(options, fetcher_concurrent, stop_flag, use_curl=False):
    """Фетчер берёт ссылки из редиса, созданные воркерами, и выполняет запросы
    к внешним ресурсам асинхронно, далее складывает их в редис на обработку воркерами.

    Логирование через обработчик MongoHandler блокирует основной поток IOLoop.
    Сделано так из-за маленькой задержки на логирование, создание неблокирующего
    логера несет лишние накладные расходы.
    """

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    pool = tornadoredis.ConnectionPool(fetcher_concurrent, wait_for_available=True,
                                       host=options.redis_host, port=options.redis_port)
    pid = os.getpid()
    logger.info('Fetcher is started. Redis connection {}:{}'.format(options.redis_host, options.redis_port))

    @coroutine
    def main():

        @coroutine
        def fetch(i):
            tr_client = tornadoredis.Client(connection_pool=pool)
            while not stop_flag.value:
                try:
                    url = yield Task(tr_client.brpop, cns.URL_QUEUE_KEY, cns.BLOCKING_TIMEOUT)
                    url = url[cns.URL_QUEUE_KEY]
                    logger.debug('Coroutine {} got url {}'.format(i, url))
                    client = AsyncHTTPClient()
                    user_agent = fake_useragent.UserAgent(cache=True).random # кэш во временной папке системы
                    response = yield client.fetch(url, user_agent=user_agent)
                    if response.body:
                        data_key = cns.DATA_KEY_PREFIX + url
                        pipeline = tr_client.pipeline(transactional=True)
                        pipeline.lpush(cns.DATA_QUEUE_KEY, data_key)
                        pipeline.set(data_key, response.body.decode())
                        yield Task(pipeline.execute)
                        logger.debug("Coroutine {} request time: {}. {}".format(i, response.request_time, url))
                except:
                    logger.exception('Coroutine {} error with url {}'.format(i, url))
            logger.info('Coroutine {} is stopped'.format(i))

        logger.info('Start {} concurrent requests'.format(fetcher_concurrent))
        completed = yield [fetch(i) for i in range(1, fetcher_concurrent+1)]

    if use_curl:
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient") # использует keep-alive
    tornado.ioloop.IOLoop.current().run_sync(main)
    logger.info('Stopped. IOloop shutdown.')
