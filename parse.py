import logging
import sys
import time
import fake_useragent
import tornado.ioloop
from bs4 import BeautifulSoup
from tornado.httpclient import AsyncHTTPClient
from tornado.gen import coroutine
from tornado.queues import Queue


logger = logging.getLogger('')
formatter = logging.Formatter(style='{', datefmt='%H:%M:%S', fmt='[{asctime}.{msecs:.3g} {levelname}] {message}')
stream_hndl = logging.StreamHandler(stream=sys.stdout)
stream_hndl.setFormatter(formatter)
logger.addHandler(stream_hndl)
logger.setLevel(logging.INFO)


@coroutine
def writer(queue):
    while True:
        data = yield queue.get()
        if data == 'start': # эту задачу завершает последний живой fetcher
            logger.info('Writer is started')
        else:
            logger.info('Data is gotten')
            soup = BeautifulSoup(data.decode(), 'html.parser')
            with open('news.html', 'a') as file: # todo накладывает расходы?
                file.write(soup.prettify())
            logger.info('Data is written')

            queue.task_done()
            logger.info('Task done')

page = 1
alive = 8
@coroutine
def main():
    queue = Queue()
    url = 'http://eu.battle.net/hearthstone/ru/blog/infinite?page={}&articleType=blog'
    start_time = time.time()

    @coroutine
    def fetcher(i):
        logger.info('Fetcher {} is started'.format(i))
        global page, alive # todo убрать из глобальной области?
        while True:
            client = AsyncHTTPClient()
            user_agent = fake_useragent.UserAgent(cache=True).random #todo снести кэш-файл
            response = yield client.fetch(url.format(page), user_agent=user_agent)
            if response.body:
                yield queue.put(response.body)
                logger.info("{} Page's request time - {}".format(page, response.request_time))
                page += 1
            else:
                logger.info('No data. Fetcher {} is stoped'.format(i))
                alive -= 1
                if not alive:
                    queue.task_done() # завершение задачи 'start'
                break

    for i in range(alive):
        fetcher(i)

    writer(queue)
    queue.put('start')

    yield queue.join()
    end_time = time.time()
    logger.info('Duration - {}'.format(end_time-start_time))


if __name__ == '__main__':
    AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient") # использует keep-alive
    tornado.ioloop.IOLoop.current().run_sync(main)
