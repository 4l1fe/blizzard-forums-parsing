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


def main(fetcher_count, fetcher_concurrent, worker_count):
    url_queue, data_queue = JoinableQueue(), JoinableQueue() #todo maxsize
    logger.info('Main[pid={}] process is started'.format(os.getpid()))

    logger.info('Start {} fetchers'.format(fetcher_count))

    fetchers = []
    for i in range(fetcher_count):
        p = Process(target=fetcher, name='Fetcher', args=(url_queue, data_queue, fetcher_concurrent))
        p.start()
        fetchers.append(p)

    logger.info('Start {} workers'.format(worker_count))
    workers = []
    for i in range(worker_count):
        p = Process(target=worker, name='Worker', args=(url_queue, data_queue))
        p.start()
        workers.append(p)

    logger.info('=================================Start parsing=================================')
    start_time = time.time()
    url_queue.put('http://eu.battle.net/hearthstone/ru/forum/10362657/')
    url_queue.join()
    data_queue.join()
    end_time = time.time()
    logger.info('End parsing. Duration - {}'.format(end_time-start_time))


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--fetcher-count', type=int, default=1)
    parser.add_argument('--fetcher-concurrent', type=int, default=5)
    parser.add_argument('--worker-count', type=int, default=1)
    args = parser.parse_args()

    main(args.fetcher_count, args.fetcher_concurrent, args.worker_count)
