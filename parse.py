import logging
import time
import signal
import sys
import uuid
import constants as cns
from itertools import chain
from logging.config import dictConfig
from argparse import ArgumentParser
from multiprocessing import Process
from redis import Redis
from fetcher import fetcher
from worker import worker
from tree import Tree

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
    если очереди пустые(интерпритация завершения работы системы) или пришел сигнал на завершение.
    """

    r = Redis(options.redis_host, options.redis_port)
    uidh = uuid.uuid4().hex
    parent_key = cns.CLUSTER_NODE_PREFIX + uidh
    STOP = False
    logger.info('Cluster node {} process is started. Args: {}'.format(uidh, options))

    def graceful_stop(signum, frame):
        nonlocal STOP
        if STOP:
            sys.exit()
        logger.info('Graceful stopping...')
        STOP = True
    signal.signal(signal.SIGINT, graceful_stop)
    signal.signal(signal.SIGTERM, graceful_stop)

    logger.info('Start {} fetchers'.format(options.fetcher_count))
    fetchers = []
    for i in range(options.fetcher_count):
        p = Process(target=fetcher, name='Fetcher', args=(parent_key, options, options.fetcher_concurrent, options.use_curl))
        p.start()
        fetchers.append(p)

    logger.info('Start {} workers'.format(options.worker_count))
    workers = []
    for i in range(options.worker_count):
        p = Process(target=worker, name='Worker', args=(parent_key, options, options.use_lxml))
        p.start()
        workers.append(p)

    finish_flags = {p.pid: 1 for p in chain(fetchers, workers)}    #TODO замена на комуникацию через pipe
    r.hmset(parent_key, finish_flags)

    Tree(r).add_root(options.url)

    logger.info('=================================Start parsing=================================')
    start_time = time.time()
    r.lpush(cns.URL_QUEUE_KEY, options.url)
    while True:
        time.sleep(options.check_period)
        uq_size = r.llen(cns.URL_QUEUE_KEY); dq_size = r.llen(cns.DATA_QUEUE_KEY)
        logger.debug('Url queue size: {}; Data queue size: {}'.format(uq_size, dq_size))
        if STOP or not (uq_size or dq_size):  #TODO при большом периоде проверки check_period придется ожидать
            finish_flags = {p.pid: 1 for p in chain(fetchers, workers)}
            r.hdel(parent_key, *tuple(finish_flags))
            break
    logger.info('Waiting for processes terminating')
    for p in chain(fetchers, workers):
        p.join()
    r.delete(*r.keys(cns.NAMESPACE + '*')) # чистка ключей только в пространстве имен парсера
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