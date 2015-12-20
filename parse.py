import logging
import sys
import time
import fake_useragent
import tornado.ioloop
from bs4 import BeautifulSoup
from tornado.httpclient import AsyncHTTPClient, HTTPClient
from tornado.gen import coroutine


logger = logging.getLogger('')
formatter = logging.Formatter(style='{', datefmt='%H:%M:%S', fmt='[{asctime}.{msecs:.3g} {levelname}] {message}')
stream_hndl = logging.StreamHandler(stream=sys.stdout)
stream_hndl.setFormatter(formatter)
stream_hndl.setLevel(logging.DEBUG)
logger.addHandler(stream_hndl)
logger.setLevel(logging.DEBUG)


@coroutine
def main():
    url = 'http://eu.battle.net/hearthstone/ru/blog/infinite?page={}&articleType=blog'
    page = 1
    # client = AsyncHTTPClient()
    client = HTTPClient()
    file = open('news.html', 'a')

    start_time = time.time()
    while True:
        user_agent = fake_useragent.UserAgent(cache=True).random #todo снести кэш-файл
        try:
            # response = yield client.fetch(url.format(page), user_agent=user_agent)
            response = client.fetch(url.format(page), user_agent=user_agent)
            if not response.body:
                break
            logger.info('Request time - {}'.format(response.request_time))
            soup = BeautifulSoup(response.body.decode(), 'html.parser')
            file.write(soup.prettify())
            page += 1
        except:
            logger.exception()
            break

    file.close()
    end_time = time.time()
    duration = int(end_time-start_time)
    logger.info('Duration - {}, Pages - {}'.format(duration, page))


if __name__ == '__main__':
    tornado.ioloop.IOLoop.current().run_sync(main)
