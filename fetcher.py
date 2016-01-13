import logging
import fake_useragent
import tornado.ioloop
import tornadoredis
import constants as cns
from tornado.httpclient import AsyncHTTPClient
from tornado.gen import coroutine, Task


logger = logging.getLogger(__name__)


def fetcher(options, fetcher_concurrent, use_curl):
    """Фетчер берёт ссылки из редиса, созданные воркерами, и выполняет запросы
    к внешним ресурсам асинхронно, далее складывает их в редис на обработку воркерами.

    Логирование через обработчик MongoHandler блокирует основной поток IOLoop.
    Сделано так из-за маленькой задержки на логирование, создание неблогирующего
    логера несет лишние накладные расходы.
    """

    pool = tornadoredis.ConnectionPool(fetcher_concurrent, wait_for_available=True,
                                       host=options.redis_host, port=options.redis_port)
    logger.info('Fetcher is started. Redis connection {}:{}'.format(options.redis_host, options.redis_port))

    @coroutine
    def main():

        @coroutine
        def fetch(i):
            tr_client = tornadoredis.Client(connection_pool=pool)
            while True:
                url = yield Task(tr_client.brpop, cns.URL_QUEUE_KEY, cns.BLOCKING_TIMEOUT)
                url = url[cns.URL_QUEUE_KEY]
                if url == cns.FINISH_COMMAND:
                    logger.info('Concurrent {} is stoped'.format(i))
                    break
                else:
                    logger.debug('Got url {}'.format(url))
                    client = AsyncHTTPClient()
                    user_agent = fake_useragent.UserAgent(cache=True).random # кэш во временной папке системы
                    try:
                        response = yield client.fetch(url, user_agent=user_agent)
                        if response.body:
                            key = cns.DATA_KEY_PREFIX + url
                            pipeline = tr_client.pipeline()
                            pipeline.lpush(cns.DATA_QUEUE_KEY, key)
                            pipeline.set(key, response.body.decode())
                            yield Task(pipeline.execute)
                            logger.debug("Request time: {}. {}".format(response.request_time, url))
                    except:
                        logger.exception('Error with url {}'.format(url))

        logger.info('Start {} concurrent requests'.format(fetcher_concurrent))
        completed = yield [fetch(i) for i in range(1, fetcher_concurrent+1)]

    if use_curl:
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient") # использует keep-alive
    tornado.ioloop.IOLoop.current().run_sync(main)
    logger.info('Stoped. IOloop shutdown.')
