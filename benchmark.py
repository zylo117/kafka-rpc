# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

import logging
import time
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

pool = ThreadPoolExecutor(32)

from kafka_rpc import KRPCClient
from kafka_rpc import KRPCServer

NUMS = 10000


# 213.8162382121568, no concurrent
# 419.71660475278094, no concurrent, use_redis
# 383.26126764939164, no concurrent, use_redis, has verification and encryption

# 2549.6978760136653, threadpool, poolsize 128
# 3408.9112918300966, threadpool, poolsize 64
# 4746.943117770887, threadpool, poolsize 32
# 2777.8816114784295, threadpool, poolsize 16


def start_server():
    class Sum:
        def add(self, x, y):
            return x + y

    krs = KRPCServer('localhost', 9092, Sum(), 'sum', ack=True)
    krs.server_forever()


def call():
    krc = KRPCClient('localhost', 9092, 'sum', ack=True)

    t1 = time.time()
    for i in range(NUMS):
        krc.add(1, 2)
    t2 = time.time()
    print(NUMS / (t2 - t1))

    krc.close()


def call_async():
    krc = KRPCClient('localhost', 9092, 'sum')

    t1 = time.time()
    futures = []
    for i in range(NUMS):
        futures.append(pool.submit(krc.add, 1, 2))
    for future in as_completed(futures):
        result = future.result()
        # print(result)

    t2 = time.time()
    print(NUMS / (t2 - t1))
    pool.shutdown()
    krc.close()


if __name__ == '__main__':
    log_fmt = '[%(asctime)s]\t-\t%(filename)s\t-\t%(funcName)s\t-\t%(lineno)d\t-\t[%(levelname)s]: %(message)s'
    formatter = logging.Formatter(log_fmt)

    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    log_file_handler = logging.FileHandler('log')
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(formatter)
    log.addHandler(log_file_handler)

    p = Process(target=start_server)
    p.start()

    # call()
    call_async()

    p.terminate()
