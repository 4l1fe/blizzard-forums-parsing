import logging
import time
import constants as cns
from itertools import chain
from logging.config import dictConfig
from argparse import ArgumentParser
from multiprocessing import Process
from redis import Redis
from fetcher import fetcher
from worker import worker
from utils import Node


logger = logging.getLogger('main')


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'custom': {
            'style': '{',
            'datefmt': '%H:%M:%S',
            'format': '[{asctime}.{msecs:.3g} {name}({process}) {levelname}] {message}'
        },
    },
    'handlers': {
        'stream': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
            'formatter': 'custom',
        },
        'mongo': {
            'level': 'DEBUG',
            'class': 'utils.MongoHandler',
            'host': 'localhost',
            'port': 27017,
            'fields': ('ip', 'asctime', 'msecs', 'name', 'process', 'levelname', 'message'),
        },
    },
    'loggers': {
        '': {
            'handlers': ['stream', 'mongo'],
            'level': 'DEBUG',
            'propagate': True
        },
        'tornado.curl_httpclient': {
            'handlers': ['stream'],
            'level': 'INFO',
            'propagate': False
        },
    }
}


def main(options):
    """Родительский процесс инициализирует настройки логера, порождает заданное
    количество фетчеров и воркеров, ложит начальную ссылку для запроса
    фетчером(начала работы всей системы), далее по заданной паузе просматривает
    содержимое очередей для ворекров\фетчеров и подаёт команду на мягкое завершение,
    если очереди пустые(интерпритация завершения работы системы).
    """

    r = Redis(options.redis_host, options.redis_port)
    logger.info('Main process is started. Args: {}'.format(options))

    logger.info('Start {} fetchers'.format(options.fetcher_count))
    fetchers = []
    for i in range(options.fetcher_count):
        p = Process(target=fetcher, name='Fetcher', args=(options, options.fetcher_concurrent, options.use_curl))
        p.start()
        fetchers.append(p)

    logger.info('Start {} workers'.format(options.worker_count))
    workers = []
    for i in range(options.worker_count):
        p = Process(target=worker, name='Worker', args=(options, options.use_lxml))
        p.start()
        workers.append(p)

    Node(position=cns.NODE_KEY_PREFIX+options.url, client=r).save()  # создание корневого узла иерархии данных

    logger.info('=================================Start parsing=================================')
    start_time = time.time()
    r.lpush(cns.URL_QUEUE_KEY, options.url)
    while True:
        time.sleep(options.check_period)
        uq_size = r.llen(cns.URL_QUEUE_KEY); dq_size = r.llen(cns.DATA_QUEUE_KEY)
        logger.debug('Url queue size: {}; Data queue size: {}'.format(uq_size, dq_size))
        if not (uq_size or dq_size): #todo изменить на queue.join() ?
            r.lpush(cns.URL_QUEUE_KEY, *(cns.FINISH_COMMAND for _ in range(options.fetcher_concurrent * options.fetcher_count)))
            r.lpush(cns.DATA_QUEUE_KEY, *(cns.FINISH_COMMAND for _ in range(options.worker_count)))
            break
    logger.info('Waiting for processes terminating')
    for p in chain(fetchers, workers):
        p.join()
    r.delete(cns.PARSED_URLS_KEY)
    end_time = time.time()
    logger.info('End parsing. Duration: {}'.format(end_time-start_time))


if __name__ == '__main__':
    # http://eu.battle.net/hearthstone/ru/forum/
    parser = ArgumentParser()
    parser.add_argument('url', metavar='<url>')
    parser.add_argument('--fetcher-count', type=int, default=1, metavar='<count>',
                        help='default: %(default)s')
    parser.add_argument('--fetcher-concurrent', type=int, default=6, metavar='<count>',
                        help='default: %(default)s')
    parser.add_argument('--worker-count', type=int, default=1, metavar='<count>',
                        help='default: %(default)s')
    parser.add_argument('--check-period', type=float, default=3.0, metavar='<seconds>',
                        help="default: %(default)s")
    parser.add_argument('--redis-host', default='localhost', metavar='<ip address>',
                        help="default: %(default)s")
    parser.add_argument('--redis-port', type=int, default=6379, metavar='<port>',
                        help="default: %(default)s")
    parser.add_argument('--mongo-host', default='localhost', metavar='<ip address>',
                        help="default: %(default)s")
    parser.add_argument('--mongo-port', type=int, default=27017, metavar='<port>',
                        help="default: %(default)s")
    parser.add_argument('-c', '--use-curl', action='store_true', help="isn't used by default")
    parser.add_argument('-l', '--use-lxml', action='store_true', help="isn't used by default")

    options = parser.parse_args()
    LOGGING['handlers']['mongo']['host'] = options.mongo_host
    LOGGING['handlers']['mongo']['port'] = options.mongo_port
    dictConfig(LOGGING)
    main(options)
