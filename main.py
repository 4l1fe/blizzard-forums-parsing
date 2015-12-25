import logging
import time
import constants as cns
from itertools import chain
from logging.config import dictConfig
from argparse import ArgumentParser
from multiprocessing import Process, JoinableQueue
from fetcher import fetcher
from worker import worker


logger = logging.getLogger('main')

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'custom': {
            'style': '{',
            'datefmt': '%H:%M:%S',
            'format': '[{asctime}.{msecs:.3g} {name}[{process}] {levelname} ] {message}'
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
    url_queue, data_queue = JoinableQueue(), JoinableQueue()
    url_cache = set() # todo: обезопасить доступ ?
    logger.info('Main process is started. Args: {}'.format(args))

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
    url_queue.put(args.url)
    while True:
        time.sleep(args.check_period)
        uq_size = url_queue.qsize(); dq_size = data_queue.qsize()
        logger.debug('Url queue size: {}; Data queue size: {}'.format(uq_size, dq_size))
        if not (uq_size or dq_size): #todo изменить на queue.join() ?
            # завершить нужно все конкурентные запросы по всем фетчерам и воркеров
            for _ in range(args.fetcher_concurrent*args.fetcher_count):
                url_queue.put(cns.FINISH_COMMAND)
            for _ in range(args.worker_count):
                data_queue.put(cns.FINISH_COMMAND)
            break
    logger.info('Waiting for processes terminating')
    for p in chain(fetchers, workers):
        p.join()
    end_time = time.time()
    logger.info('End parsing. Duration - {}'.format(end_time-start_time))


if __name__ == '__main__':
    # 'http://eu.battle.net/hearthstone/ru/forum/'
    parser = ArgumentParser()
    parser.add_argument('url', metavar='<url>')
    parser.add_argument('--fetcher-count', type=int, default=1, metavar='<count>',
                        help='default: %(default)s')
    parser.add_argument('--fetcher-concurrent', type=int, default=5, metavar='<count>',
                        help='default: %(default)s')
    parser.add_argument('--worker-count', type=int, default=1, metavar='<count>',
                        help='default: %(default)s')
    parser.add_argument('--check-period', type=float, default=3.0, metavar='<seconds>',
                        help="default: %(default)s")
    parser.add_argument('-c', '--use-curl', action='store_true', help="isn't used by default")
    parser.add_argument('-l', '--use-lxml', action='store_true', help="isn't used by default")

    args = parser.parse_args()
    dictConfig(LOGGING)
    main(args)
