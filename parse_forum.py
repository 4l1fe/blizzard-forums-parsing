import logging
import os
import sys
import time
from logging.config import dictConfig
from argparse import ArgumentParser, RawTextHelpFormatter
from multiprocessing import Process, JoinableQueue
from fetcher import fetcher
from worker import worker

logger = logging.getLogger(__name__)

LOGGING =  {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'custom': {
            'style': '{',
            'datefmt': '%H:%M:%S',
            'format': '[{levelname} {name} {asctime}.{msecs:.3g}] {message}'
        },
    },
    'handlers': {
        'stream': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
            'formatter': 'custom',
        },
    },
    'loggers': {
        '': {
            'handlers': ['stream'],
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


def main(args):
    url_queue, data_queue = JoinableQueue(), JoinableQueue() #todo maxsize
    url_cache = set() # todo: обезопасить доступ мьютексом?
    logger.info('Main[pid={}] process is started. Args: {}'.format(os.getpid(), args))

    logger.info('Start {} fetchers'.format(args.fetcher_count))

    fetchers = []
    for i in range(args.fetcher_count):
        p = Process(target=fetcher, name='Fetcher', args=(url_queue, data_queue, args.fetcher_concurrent, args.use_curl))
        p.start()
        fetchers.append(p)

    logger.info('Start {} workers'.format(args.worker_count))
    workers = []
    for i in range(args.worker_count):
        p = Process(target=worker, name='Worker', args=(url_queue, data_queue, url_cache, args.use_lxml))
        p.start()
        workers.append(p)

    logger.info('=================================Start parsing=================================')
    start_time = time.time()
    url_queue.put('http://eu.battle.net/hearthstone/ru/forum/')
    end_time = time.time()
    logger.info('End parsing. Duration - {}'.format(end_time-start_time))


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--fetcher-count', type=int, default=1, metavar='<count>',
                        help='default: %(default)s')
    parser.add_argument('--fetcher-concurrent', type=int, default=5, metavar='<count>',
                        help='default: %(default)s')
    parser.add_argument('--worker-count', type=int, default=1, metavar='<count>',
                        help='default: %(default)s')
    parser.add_argument('--use-curl', action='store_true', help="isn't used by default")
    parser.add_argument('--use-lxml', action='store_true', help="isn't used by default")
    args = parser.parse_args()
    dictConfig(LOGGING)
    main(args)
