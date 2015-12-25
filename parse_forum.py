import logging
import os
import sys
import time
from argparse import ArgumentParser
from multiprocessing import Process, JoinableQueue
from fetcher import fetcher
from worker import worker


logger = logging.getLogger('')
formatter = logging.Formatter(style='{', datefmt='%H:%M:%S', fmt='[{levelname} {asctime}.{msecs:.3g}] {message}')
stream_hndl = logging.StreamHandler(stream=sys.stdout)
stream_hndl.setFormatter(formatter)
logger.addHandler(stream_hndl)
logger.setLevel(logging.INFO)


def main(args):
    url_queue, data_queue = JoinableQueue(), JoinableQueue() #todo maxsize
    url_cache = set() # todo: обезопасить доступ мьютексом?
    logger.info('Main[pid={}] process is started'.format(os.getpid()))
    logger.info('Args: {}'.format(args))

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
    # url_queue.join()
    # logger.info('Url queue is empty')
    # data_queue.join()
    # logger.info('Data queue is empty')
    end_time = time.time()
    logger.info('End parsing. Duration - {}'.format(end_time-start_time))


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--fetcher-count', type=int, default=1)
    parser.add_argument('--fetcher-concurrent', type=int, default=5)
    parser.add_argument('--worker-count', type=int, default=1)
    parser.add_argument('--use-curl', action='store_true')
    parser.add_argument('--use-lxml', action='store_true')
    args = parser.parse_args()

    main(args)
