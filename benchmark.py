# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

import logging
import time
from multiprocessing import Process

from kafka_rpc import KRPCClient
from kafka_rpc import KRPCServer

NUMS = 1000


# 162.7235827382268, no concurrent
# 149.2814916361265, no concurrent, has verification and encryption


def start_server():
    class Sum:
        def add(self, x, y):
            return x + y

    krs = KRPCServer('localhost', 9092, Sum(), 'sum')
    krs.server_forever()


def call():
    krc = KRPCClient('localhost', 9092, 'sum')

    t1 = time.time()
    for i in range(NUMS):
        krc.add(1, 2)
    t2 = time.time()
    print(NUMS / (t2 - t1))

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

    call()

    p.terminate()
